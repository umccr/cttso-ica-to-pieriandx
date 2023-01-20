#!/usr/bin/env python3

"""
Update the GSuite spreadsheet for the cttso samples and
launch PierianDx outputs for any samples that have complete rows but are not in PierianDx.

This merges inputs from the following sources

* RedCap
* UMCCR Data Portal
* GLIMS
* PierianDx

This will also launch validation samples that do not have RedCap information, where
the GLIMS value ProjectName is set to "Validation" or "Control" and launch with the default outputs.

Cells are indexed by Subject and Library ID values, the following items can be updated:
* RedCap Status
* RedCap Type
* Workflow Run ID
* Workflow Run End
* Workflow Run Status
* Workflow Run Sequence Run Name
* Workflow Run Is Failed Run
* PierianDX Case ID
* PierianDx Case Accession Number
* PierianDx Case Identified
* PierianDX Job ID
* PierianDX Workflow Status
* PierianDX Report Status
* PierianDx Report Signed Out

Values with new Subject and Library ID values will be appended
"""
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_lambda.type_defs import GetFunctionResponseTypeDef, FunctionConfigurationTypeDef, InvocationResponseTypeDef

from mypy_boto3_lambda.literals import StateType as LambdaFunctionStateType
from mypy_boto3_ssm import SSMClient
from botocore.exceptions import ClientError

import pandas as pd
from typing import Dict, List, Union
import json
from time import sleep
from datetime import datetime, timedelta

from lambda_utils.arns import get_validation_lambda_arn, get_clinical_lambda_arn
from lambda_utils.aws_helpers import get_boto3_lambda_client, get_boto3_ssm_client
from lambda_utils.gspread_helpers import \
    get_cttso_samples_from_glims, get_cttso_lims, update_cttso_lims_row, \
    append_df_to_cttso_lims
from lambda_utils.logger import get_logger
from lambda_utils.pieriandx_helpers import get_pieriandx_df, get_pieriandx_status_for_missing_sample
from lambda_utils.portal_helpers import get_portal_workflow_run_data_df
from lambda_utils.redcap_helpers import get_full_redcap_data_df
from lambda_utils.globals import \
    PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH, \
    REDCAP_APIS_LAMBDA_FUNCTION_ARN_SSM_PARAMETER, \
    CLINICAL_LAMBDA_FUNCTION_SSM_PARAMETER_PATH, \
    VALIDATION_LAMBDA_FUNCTION_ARN_SSM_PARAMETER_PATH, \
    MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE, MAX_ATTEMPTS_WAKE_LAMBDAS

logger = get_logger()


def merge_redcap_portal_and_glims_data(redcap_df, portal_df, glims_df) -> pd.DataFrame:
    """
    Merge information from the redcap dataframe, portal dataframe and glims dataframe
    :param redcap_df: A pandas data frame with the following columns
      * subject_id
      * library_id
      * in_redcap
      * redcap_sample_type
      * redcap_is_complete
    :param portal_df:
      * subject_id
      * library_id
      * in_portal
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
    :param glims_df:
      * subject_id
      * library_id
      * in_glims
      * sequence_run_name
      * glims_is_validation
      * glims_is_research
    :return: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
    """
    portal_redcap_df = pd.merge(portal_df, redcap_df,
                                on=["subject_id", "library_id"],
                                how="outer")

    # Use portal_sequence_run_name to drop values from glims df that
    # aren't in the portal df
    glims_df = glims_df.rename(
        columns={
            "sequence_run_name": "portal_sequence_run_name"
        }
    )
    # Use portal_sequence_run_name to drop values from glims df that
    # aren't in the portal df
    portal_redcap_glims_df = pd.merge(portal_redcap_df, glims_df,
                                      on=["subject_id", "library_id", "portal_sequence_run_name"],
                                      how="left")

    # Fill boolean NAs
    for boolean_column in ["in_redcap", "in_portal", "in_glims"]:
        portal_redcap_glims_df[boolean_column] = portal_redcap_glims_df[boolean_column].fillna(
            value=False
        )

    # mini_dfs: List[pd.DataFrame] = []

    # Debugging process
    mini_df: pd.DataFrame
    for (subject_id, library_id), mini_df in portal_redcap_glims_df.groupby(["subject_id", "library_id"]):
        if mini_df.shape[0] > 1:
            logger.info(f"Got duplicate rows for subject id '{subject_id}', and library id '{library_id}', "
                        f"The dataframe is as shown below "
                        f"{mini_df.to_dict()}")

    return portal_redcap_glims_df


def get_libraries_for_processing(merged_df) -> pd.DataFrame:
    """
    Compare the merged dataframe with the cttso lims dataframe -
    Observe how many of these samples have a pieriandx identifier
    :param merged_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * in_pieriandx
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_panel_type
    :return: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * is_validation_sample
    """

    # Initialise
    processing_columns = [
        "subject_id",
        "library_id",
        "portal_wfr_id",
        "is_validation_sample",
        "is_research_sample"
    ]

    # Processing libraries must meet the following criteria
    # 1. Not in pieriandx
    # 2. In the portal
    # 3. Have a successful ICA tso500 workflow run
    # 4. Not be on a failed run
    # 5. Exist in either redcap or glims
    to_process_df = merged_df.query(
        "("
        "  pieriandx_case_id.isnull() and "
        "  not in_pieriandx and "
        "  not portal_wfr_id.isnull() and "
        "  portal_wfr_status == 'Succeeded' and "
        "  portal_is_failed_run == False and "
        "  ( "
        "    ( "
        "      not redcap_is_complete.isnull() and redcap_is_complete.str.lower() == 'complete' "
        "    ) or "
        "    ( "
        "      not glims_is_validation.isnull() and glims_is_validation == True "
        "    ) or "
        "    ( "
        "      not glims_is_research.isnull() and glims_is_research == True "
        "    )"
        "  ) "
        ") ",
        engine="python"  # Required for the isnull bit - https://stackoverflow.com/a/54099389/6946787
    )

    if to_process_df.shape[0] == 0:
        # No processing to occur
        return pd.DataFrame(
            columns=processing_columns
        )

    # For validation sample to be set to true
    # Must not be in redcap
    # AND be in glims
    # Redcap samples can be validation samples
    # But samples not in redcap are processed through a different
    # lambda endpoint
    to_process_df["is_validation_sample"] = to_process_df.apply(
        lambda x: True
        if x.glims_is_validation is True
        and (
               pd.isnull(x.redcap_is_complete) or
               not x.redcap_is_complete.lower() == "complete"
           )
        else False,
        axis="columns"
    )

    to_process_df["is_research_sample"] = to_process_df.apply(
        lambda x: True
        if x.glims_is_research is True else False,
        axis="columns"
    )

    return to_process_df[
        processing_columns
    ]


def submit_library_to_pieriandx(subject_id: str, library_id: str, workflow_run_id: str, lambda_arn: str):
    """
    Submit library to pieriandx
    :param subject_id:
    :param library_id:
    :param workflow_run_id
    :param lambda_arn
    :return:
    """
    lambda_client: LambdaClient = get_boto3_lambda_client()

    lambda_payload: Dict = {
            "subject_id": subject_id,
            "library_id": library_id,
            "ica_workflow_run_id": workflow_run_id
    }

    logger.info(f"Launching lambda function {lambda_arn} with the following payload {json.dumps(lambda_payload)}")

    lambda_function_response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="Event",
        Payload=json.dumps(lambda_payload)
    )

    # Check status code
    if not lambda_function_response.get("StatusCode") == 202:
        logger.error(f"Bad exit code when retrieving response from "
                     f"lambda client {lambda_arn}")
        raise ValueError

    # No payload returned since we use the 'event', to prevent multiple lambda submissions from boto3

    # # Check payload
    # if "Payload" not in list(lambda_function_response.keys()):
    #     logger.error("Could not retrieve payload, submission to batch likely failed")
    #     logger.error(f"Client response was {lambda_function_response}")
    #     raise ValueError
    #
    # # Check response
    # response_payload: Dict = json.loads(lambda_function_response.get("Payload").read())
    #
    # # Check response payload
    # if response_payload is None or not isinstance(response_payload, Dict):
    #     logger.error("Could not get response payload as a dict")
    #     logger.error(f"Client response was {lambda_function_response}")
    #     logger.error(f"Payload was {response_payload}")
    #     raise ValueError
    #
    # if "errorMessage" in response_payload.keys():
    #     logger.info(f"Could not successfully launch ica-to-pieriandx workflow for "
    #                 f"subject id '{subject_id}' / library id '{library_id}'")
    #     raise ValueError
    #
    # logger.info("Successfully launched and returning submission lambda")
    # logger.info(f"Payload returned '{response_payload}' from arn: '{lambda_arn}'")
    #
    # # Step 8 - Return case accession number and metadata information to user
    # return response_payload


def submit_libraries_to_pieriandx(processing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Submit libraries to pieriandx through their respective lambdas
    :param processing_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * is_validation_sample
    :return:
    """
    # Get number of rows to submit
    num_submissions = processing_df.shape[0]

    if num_submissions > MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE:
        logger.info(f"Dropping submission number from {num_submissions} to {MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE}")
        processing_df = processing_df.head(MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE)

    # Validation df
    # Validation if is validation sample or IS research sample with no redcap information
    processing_df["submission_arn"] = processing_df.apply(
        lambda x: get_validation_lambda_arn()
        if x.is_validation_sample or (x.is_research_sample and not x.redcap_is_complete)
        else get_clinical_lambda_arn(),
        axis="columns"
    )

    processing_df["panel_type"] = processing_df.apply(
        lambda x: "main"
        if (x.is_validation_sample or x.is_research_sample)
        else "subpanel"
    )

    processing_df["submission_succeeded"] = False

    for index, row in processing_df.iterrows():
        logger.info(f"Submitting the following subject id / library id to PierianDx")
        logger.info(f"SubjectID='{row.subject_id}', LibraryID='{row.library_id}', Workflow Run ID='{row.portal_wfr_id}'")
        logger.info(f"Submitted to arn: '{row.submission_arn}'")
        try:
            submit_library_to_pieriandx(subject_id=row.subject_id,
                                        library_id=row.library_id,
                                        workflow_run_id=row.portal_wfr_id,
                                        lambda_arn=row.submission_arn)
        except ValueError:
            pass
        else:
            processing_df.loc[index, "submission_succeeded"] = True

    return processing_df


def append_to_cttso_lims(merged_df: pd.DataFrame, cttso_lims_df: pd.DataFrame, excel_row_number_mapping_df: pd.DataFrame) -> None:
    """
    We now merge the processing dataframe with the cttso lims dataframe, setting the values of the samples
    :param merged_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * in_pieriandx
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
    :param cttso_lims_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * glims_is_validation
      * glims_is_research
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      * pieriandx_report_signed_out  - currently ignored
  :param excel_row_number_mapping_df: A pandas DataFrame with the following columns:
        * cttso_lims_index
        * excel_row_number
    :return:
    """

    # Initialise list of rows to append
    rows_to_append: List[pd.Series] = []

    # Iterate through the merged dataframe row by row and add any rows that don't exist in the sheet
    for index, row in merged_df.iterrows():
        # Collect potentially matching row in cttso_lims df
        cttso_lims_df_rows = cttso_lims_df.query(
            f"subject_id=='{row.subject_id}' and "
            f"library_id=='{row.library_id}'"
        )

        # If we have a match, we need to consider edge cases
        if cttso_lims_df_rows.shape[0] > 0:
            # Also compare portal_wfr_id if not null
            # And also compare pieriandx case id if not null
            # Handle cases explicitly here by using
            # the boolean columns: in_redcap, in_glims, in_portal, in_pieriandx
            # Edge Case 1
            # Theres been multiple portal runs for this sample
            # A sample is being reprocessed?
            # And now theres a new portal run id on the block!
            if row.in_portal:
                cttso_lims_df_rows = cttso_lims_df_rows.query(
                    f"portal_wfr_id=='{row.portal_wfr_id}'"
                )
            # Edge Case 2
            # Could be a new sample ready to be reprocessed?
            # Entered in redcap and maybe portal but definitely not in pieriandx
            # Case in GLIMS will already be in portal so need to consider
            # We drop all other rows
            if (row.in_redcap or row.in_portal) and not row.in_pieriandx:
                cttso_lims_df_rows = cttso_lims_df_rows.query(
                    f"in_pieriandx==False"
                )
            # Edge Case 2
            # Could a reprocessed sample have now found its way into PierianDx?
            # We should compare the merged df row by case ids if row.in_pieriandx is true
            if row.in_pieriandx:
                cttso_lims_df_rows = cttso_lims_df_rows.query(
                    f"pieriandx_case_id=='{row.pieriandx_case_id}'"
                )
            # Extra edge cases to come as we find them  # TODO

        # Conclusion after all edge cases considered
        if cttso_lims_df_rows.shape[0] > 1:
            logger.warning(f"Couldn't figure out whether to append or update for row "
                           f"{row.to_dict()}. "
                           f"Matches {cttso_lims_df_rows.shape[0]} rows in the current dataframe"
                           f"for subject '{row.subject_id}', "
                           f"library id '{row.library_id}'"
                           )
            continue

        # Collect new cttso lims row
        new_cttso_lims_row = merged_df.\
            loc[index, :].\
            squeeze().\
            reindex(cttso_lims_df.columns)

        # Row doesn't exist in excel sheet
        if cttso_lims_df_rows.shape[0] == 0:
            # New subject / library processed on creation
            # Simples, append
            rows_to_append.append(
                new_cttso_lims_row
            )
            continue

        # This leaves 'one row' - but we still may append over update depending on conditions

        # Let's compare the existing row with the current row
        cttso_lims_df_row: pd.Series = cttso_lims_df_rows.squeeze()
        if new_cttso_lims_row.compare(cttso_lims_df_row[new_cttso_lims_row.index]).shape[0] == 0:
            # No change
            continue

        # Let's ensure that something good has actually happened
        in_rows = [
            "in_redcap",
            "in_portal",
            "in_glims",
            "in_pieriandx"
        ]
        # either the pieriandx case id has been set to pending for this sample
        # AND the existing row is NULL OR
        # i.e the merged_df must have at least one more over in_* columns set to true
        # compared to the current column in the spreadsheet
        # Or we report this issue
        if pd.isnull(cttso_lims_df_row["pieriandx_case_id"]) and \
                not pd.isnull(new_cttso_lims_row["pieriandx_case_id"]) and \
                new_cttso_lims_row["pieriandx_case_id"] == "pending":
            logger.info("Case ID for a pieriandx has been set to pending, so updating value in cttso lims.")
        elif new_cttso_lims_row[in_rows].compare(cttso_lims_df_row[in_rows]).query("self == True and other == False").shape[0] == 0:
            # This means that nothing has changed between the 'in_' steps. Pfft, skip it.
            logger.debug(f"Skipping row change for subject '{row.subject_id}' and library '{row.library_id}'")
            continue
        else:
            logger.info(f"Change for sbj {row['subject_id']} lbj {row['library_id']}")
            logger.info(new_cttso_lims_row[in_rows].compare(cttso_lims_df_row[in_rows]).query("self == True and other == False"))

        # Get excel row number to change
        # With pandas series, the index number becomes the 'name'
        excel_row_number: int = excel_row_number_mapping_df.query(
            f"cttso_lims_index=={cttso_lims_df_row.name}"
        )["excel_row_number"].item()

        # Update the row
        logger.info(f"Updating row {excel_row_number} with {new_cttso_lims_row.to_json()}")
        update_cttso_lims_row(
            new_cttso_lims_row,
            excel_row_number
        )

    if len(rows_to_append) == 0:
        return

    # Append the list of rows to append
    append_df = pd.concat(rows_to_append, axis="columns").transpose()

    # Sort by run, portal, date in pieriandx
    append_df = append_df.sort_values(
        by=["portal_sequence_run_name", "portal_wfr_end", "pieriandx_case_creation_date"]
    )

    append_df_to_cttso_lims(append_df)


def get_pieriandx_incomplete_job_df_from_cttso_lims_df(cttso_lims_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter dataframe based on pieriandx_workflow_id, pieriandx_workflow_status, pieriandx_report_status and pieriandx_report_signed_out
    If any of these are in the non-complete categories then we re-query the case id and
    check if any of these values have been updated.

    Since we haven't signed out any cases, we currently ignore the pieriandx_report_signed_out
    :param cttso_lims_df: A pandas dataframe with the following columns:
        * subject_id
        * library_id
        * glims_is_validation
        * glims_is_research
        * in_redcap
        * in_portal
        * in_glims
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_panel_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
    :return: A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * glims_is_validation
        * glims_is_research
        * in_redcap
        * in_portal
        * in_glims
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_panel_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
    """
    static_statuses = ["complete", "failed", "canceled"]

    return cttso_lims_df.query(
        "( "
        "  ( "
        "    not pieriandx_case_id.isnull() or "
        "    pieriandx_case_id == 'pending' "
        "  ) "
        "  and "
        "  ( "
        "    pieriandx_workflow_id.isnull() or "
        "    not pieriandx_workflow_status in @static_statuses or "
        "    not pieriandx_report_status in @static_statuses"
        "  )"
        ")",
        engine="python"
    )


def update_merged_df_with_processing_df(merged_df, processing_df) -> pd.DataFrame:
    """
    Updated the merged df with the processing df
    :param merged_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
    :param processing_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * is_validation_sample
      * submission_succeeded
    :return: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
    """
    # Set the pieriandx case id to these samples as 'pending'
    for index, row in processing_df.iterrows():
        # Update the merged df row by the index of processing df
        if row.submission_succeeded:
            merged_df.loc[row.name, "pieriandx_case_id"] = "pending"
        else:
            merged_df.loc[row.name, "pieriandx_case_id"] = "failed"

    return merged_df


def update_pieriandx_job_status_missing_df(pieriandx_job_status_missing_df, merged_df):
    """
    Update pieriandx job status missing df with that of the pieriandx incomplete jobs and merged df
    :param pieriandx_job_status_missing_df: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      * pieriandx_report_signed_out - currently ignored
    :param merged_df: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
    :return: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
    """

    # We only want some columns from merged df
    merged_df = merged_df[[
        "subject_id",
        "library_id",
        "in_redcap",
        "in_portal",
        "in_glims",
        "in_pieriandx",
        "redcap_sample_type",
        "redcap_is_complete",
        "portal_wfr_id",
        "portal_wfr_end",
        "portal_wfr_status",
        "portal_sequence_run_name",
        "portal_is_failed_run",
        "glims_is_validation",
        "glims_is_research",
        "pieriandx_case_id",
        "pieriandx_case_creation_date"
    ]]

    # We merge right since we only want jobs we've picked up in the incomplete jobs df
    return pd.merge(merged_df, pieriandx_job_status_missing_df,
                    on=["subject_id", "library_id", "pieriandx_case_id"],
                    how="right")


def add_pieriandx_df_to_merged_df(merged_df: pd.DataFrame, pieriandx_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add in pieriandx df to the merged df
    :param merged_df: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
    :param pieriandx_df: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
    :return: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
    """
    merged_df_with_pieriandx_df = pd.merge(
        merged_df,
        pieriandx_df,
        on=["subject_id", "library_id"],
        how="outer"
    )

    # Set the in_pieriandx tag for subjects / libraries in pieriandx
    for boolean_column in ["in_redcap", "in_portal", "in_glims", "in_pieriandx"]:
        merged_df_with_pieriandx_df[boolean_column] = merged_df_with_pieriandx_df[boolean_column].fillna(
            value=False
        )

    # For new workflow runs we flip the in_pieriandx boolean if the case creation date
    # is older than the existing pieriandx date
    # Set all pieriandx columns to NA
    invalid_pieriandx_indices = \
        merged_df_with_pieriandx_df.query(
            "pieriandx_case_creation_date.dt.date < "
            "portal_wfr_end.dt.date"
        ).index
    merged_df_with_pieriandx_df.loc[
        invalid_pieriandx_indices
        ,
        "in_pieriandx"
    ] = False
    merged_df_with_pieriandx_df.loc[
        invalid_pieriandx_indices
        ,
        [
            "pieriandx_case_id",
            "pieriandx_case_accession_number",
        ]
    ] = pd.NA
    merged_df_with_pieriandx_df.loc[
        invalid_pieriandx_indices
        ,
        "pieriandx_case_creation_date"
    ] = pd.NaT

    # Drop cases with pieriandx where duplicates have been created in id sections
    merged_df_with_pieriandx_df = merged_df_with_pieriandx_df.drop_duplicates(
        subset=["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id"],
        keep="last"
    )

    # Drop cases in pieriandx where not found in redcap or glims and not found in portal
    merged_df_with_pieriandx_df = merged_df_with_pieriandx_df.query(
        "not ( "
        "  ( "
        "    in_glims == False and "
        "    in_redcap == False "
        "  ) and "
        "  in_portal == False and "
        "  in_pieriandx == True"
        ")"
    )

    # Drop cases in pieriandx where portal wfr run status isn't succeeded
    # Set all pieriandx columns to NA
    invalid_pieriandx_indices = \
        merged_df_with_pieriandx_df.query(
            "not portal_wfr_status.str.lower() == 'succeeded' and "
            "in_pieriandx == True"
        ).index
    merged_df_with_pieriandx_df.loc[
        invalid_pieriandx_indices
        ,
        "in_pieriandx"
    ] = False
    merged_df_with_pieriandx_df.loc[
        invalid_pieriandx_indices
        ,
        [
            "pieriandx_case_id",
            "pieriandx_case_accession_number",
        ]
    ] = pd.NA
    merged_df_with_pieriandx_df.loc[
        invalid_pieriandx_indices
        ,
        "pieriandx_case_creation_date"
    ] = pd.NaT

    # Now that we've NAs a bunch of duplicates, lets group-by subject, library, portal wfr
    # And drop duplicates that have NA values for pieriandx case ids
    mini_dfs: List[pd.DataFrame] = []
    for (subject_id, library_id, portal_wfr_id), mini_df in merged_df_with_pieriandx_df.groupby(["subject_id", "library_id", "portal_wfr_id"]):
        if mini_df.shape[0] == 1:
            mini_dfs.append(mini_df)
            continue
        mini_dfs.append(
            mini_df.dropna(axis="rows", subset="pieriandx_case_id")
        )

    merged_df_with_pieriandx_df = pd.concat(mini_dfs)

    return merged_df_with_pieriandx_df


def update_cttso_lims(update_df: pd.DataFrame, cttso_lims_df: pd.DataFrame, excel_row_mapping_df: pd.DataFrame) -> None:
    """
    Update cttso GSuite spreadsheet for rows that need to be updated
    Rows are first updated and samples with missing information will appended afterwards
    :param update_df: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * glims_is_validation
      * glims_is_research
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      * pieriandx_report_signed_out - currently ignored
    :param cttso_lims_df: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * glims_is_validation
      * glims_is_research
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      * pieriandx_report_signed_out - currently ignored
    :param excel_row_mapping_df: A pandas dataframe with the following columns:
      * cttso_lims_index
      * excel_row_number
    :return:
    """

    # Set the pieriandx case id to these samples as 'pending'
    for index, row in update_df.iterrows():
        cttso_lims_row = cttso_lims_df.query(
            f"subject_id=='{row.subject_id}' and "
            f"library_id=='{row.library_id}' and "
            f"pieriandx_case_id=='{row.pieriandx_case_id}'"
        ).squeeze()

        if isinstance(cttso_lims_row, pd.DataFrame) and cttso_lims_row.shape[0] == 0:
            # Empty dataframe, which suggests the case id was
            # Previously pending and is now an actual case
            cttso_lims_row = cttso_lims_df.query(
                f"subject_id=='{row.subject_id}' and "
                f"library_id=='{row.library_id}' and "
                f"pieriandx_case_id=='pending'"
            ).squeeze()

        # Ensure the squeezing actually worked
        if isinstance(cttso_lims_row, pd.DataFrame) and cttso_lims_row.shape[0] == 0:
            logger.info("Not sure what happened here, could not find the row of interest")
            continue
        if not isinstance(cttso_lims_row, pd.Series):
            logger.info("Got multiple rows in the dataframe for "
                        f"subject_id = '{row.subject_id}', "
                        f"library_id = '{row.library_id}', "
                        f"pieriandx_case_id = '{row.pieriandx_case_id}' "
                        f"so don't know which one to update"
                        )
            continue

        new_cttso_lims_row = row.reindex(cttso_lims_df.columns)

        # Compare rows
        pieriandx_columns = [
            "pieriandx_case_id",
            "pieriandx_case_accession_number",
            "pieriandx_case_creation_date",
            "pieriandx_case_identified",
            "pieriandx_panel_type",
            "pieriandx_workflow_id",
            "pieriandx_workflow_status",
            "pieriandx_report_status",
        ]
        row_diff_df: pd.DataFrame = \
            cttso_lims_row[pieriandx_columns].compare(new_cttso_lims_row[pieriandx_columns])

        if not row_diff_df.shape[0] == 0:
            excel_row_number = excel_row_mapping_df.query(
                f"cttso_lims_index=={cttso_lims_row.name}"
            )["excel_row_number"].item()
            # Update the row
            update_cttso_lims_row(
                new_cttso_lims_row,
                excel_row_number
            )


def get_duplicate_case_ids(lims_df: pd.DataFrame) -> List:
    """
    Clean up duplicate pieriandx cases from a dataframe
    By finding the duplicate case ids
    And duplicates are considered where the following conditions hold:
    * Multiple pieriandx case accession numbers exist for this combination
    * The latest pieriandx case has the following columns set to complete:
      * pieriandx_workflow_status
      * pieriandx_report_status
    :param lims_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_glims
      * in_portal
      * in_redcap
      * in_pieriandx
      * glims_is_validation
      * glims_is_research
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
    :return:
    """

    # Initialise list of case ids to drop
    case_ids_to_remove: List = []

    # Iterate through each grouping
    # Append rows to drop
    subject_id: str
    library_id: str
    portal_wfr_id: str
    mini_df: pd.DataFrame
    for (subject_id, library_id, portal_wfr_id), mini_df in lims_df.groupby(
            ["subject_id", "library_id", "portal_wfr_id"]):
        # Check if its just a single row
        if mini_df.shape[0] == 1:
            # Single unique row - nothing to see here
            continue

        # Check we don't have duplicate pieriandx case ids
        if not len(mini_df["pieriandx_case_id"].unique()) == mini_df.shape[0]:
            logger.info(f"Got duplicates pieriandx case ids "
                        f"for subject_id '{subject_id}', "
                        f"library_id '{library_id}' and "
                        f"portal_wfr_id '{portal_wfr_id}'")
            continue

        # We perform a check for if the last row (sorted by pieriandx_case_id) has
        # pieriandx_case_id
        # pieriandx_workflow_status
        # pieriandx_report_status
        mini_df = mini_df.sort_values(
            by="pieriandx_case_id",
            na_position="first"
        )

        mini_df_indexes = mini_df.index
        last_row: pd.Series = mini_df.loc[mini_df_indexes[-1], :]
        if not pd.isnull(last_row["pieriandx_workflow_status"]) and \
                last_row["pieriandx_workflow_status"] == "complete" and \
                not pd.isnull(last_row["pieriandx_report_status"]) and \
                last_row["pieriandx_report_status"] == "complete":
            # Append all other rows as rows to remove
            case_ids_to_remove.extend(mini_df.loc[mini_df_indexes[:-1], "pieriandx_case_id"].tolist())
        elif not pd.isnull(last_row["pieriandx_case_id"]) and not pd.isnull(last_row["pieriandx_case_creation_date"]) \
                and pd.isnull(last_row["pieriandx_workflow_status"]) and pd.isnull(last_row["pieriandx_report_status"]):
            # Case of L2100166
            # Two accession numbers created on the same day but the first one is used not the second
            # In this case, check if the last row is created over one week ago, if so then remove it
            # Only if the other case id has values though
            non_last_row_cases: pd.DataFrame = mini_df.loc[
                mini_df_indexes[:-1],
                [
                    "pieriandx_case_id",
                    "pieriandx_workflow_status",
                    "pieriandx_report_status"
                ]
            ]

            if len(non_last_row_cases.dropna(how='any')) == 0:
                continue

            date_one_week_ago = datetime.utcnow().date() - timedelta(days=7)
            if pd.Timestamp(last_row["pieriandx_case_creation_date"]).date() < date_one_week_ago:
                # This last row is over a week old and has no workflow status or report status so remove it
                case_ids_to_remove.append(mini_df.loc[mini_df_indexes[-1], "pieriandx_case_id"])

    case_ids_to_remove = [case_id
                          for case_id in case_ids_to_remove
                          if not pd.isnull(case_id)]

    return case_ids_to_remove


def cleanup_duplicate_rows(merged_df: pd.DataFrame, cttso_lims_df: pd.DataFrame, excel_row_number_mapping_df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Clean up duplicate rows - that is, rows with duplicates of the following columns
    * subject id
    * library id
    * portal wfr id
    And we remove duplicates where the following conditions hold:
    * Multiple pieriandx case accession numbers exist for this combination
    * The latest pieriandx case has the following columns set to complete:
      * pieriandx_workflow_status
      * pieriandx_report_status
    :param merged_df: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_is_validation
      * glims_is_research
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
    :param cttso_lims_df: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * in_glims
      * in_portal
      * in_redcap
      * in_pieriandx
      * glims_is_validation
      * glims_is_research
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
    :param excel_row_number_mapping_df:
      * cttso_lims_index
      * excel_row_number
    :return: (
      A pandas DataFrame with the following columns:
       * subject_id
       * library_id
       * in_redcap
       * in_portal
       * in_glims
       * redcap_sample_type
       * redcap_is_complete
       * portal_wfr_id
       * portal_wfr_end
       * portal_wfr_status
       * portal_sequence_run_name
       * portal_is_failed_run
       * glims_is_validation
       * glims_is_research
       * pieriandx_case_id
       * pieriandx_case_accession_number
       * pieriandx_case_creation_date
      ,
      A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * in_glims
        * in_portal
        * in_redcap
        * in_pieriandx
        * glims_is_validation
        * glims_is_research
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_panel_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
      ,
      A pandas DataFrame with the following columns
        * cttso_lims_index
        * excel_row_number
    )
    """

    # Merge our dataframes so we only need to do this once
    merged_lims_df: pd.DataFrame = pd.merge(
        merged_df, cttso_lims_df,
        on=["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id"],
        how="outer", suffixes=("", "_lims")
    )

    # Get list of case ids to drop
    case_ids_to_remove: List = get_duplicate_case_ids(merged_lims_df)

    # For each dataframe filter ids needed to be removed
    cttso_lims_df_dedup: pd.DataFrame = cttso_lims_df.query("pieriandx_case_id not in @case_ids_to_remove").reset_index(drop=True)
    merged_df_dedup: pd.DataFrame = merged_df.query("pieriandx_case_id not in @case_ids_to_remove").reset_index(drop=True)

    # Iterate again through the lims df and drop any duplicates now where pieriandx case id is null
    # And another pieriandx case id exists
    mini_dfs: List[pd.DataFrame] = []
    for (subject_id, library_id, portal_wfr_id), mini_df in cttso_lims_df_dedup.groupby(["subject_id", "library_id", "portal_wfr_id"]):
        if mini_df.shape[0] == 1:
            mini_dfs.append(mini_df)
            continue
        # Remove pieriandx cases where case id is null, just leaving those not null
        only_non_na_case_ids: pd.DataFrame = mini_df.query(
            "not pieriandx_case_id.isnull()",
            engine="python"
        )
        if only_non_na_case_ids.shape[0] > 0:
            mini_dfs.append(only_non_na_case_ids)
            continue
        logger.warning(f"Still got duplicate rows for subject id, library id "
                       f"'{subject_id}', '{library_id}'")
        mini_dfs.append(mini_df)
    cttso_lims_df_dedup = pd.concat(mini_dfs)

    cttso_lims_df_dedup = cttso_lims_df_dedup.sort_values(
        by=["portal_sequence_run_name", "portal_wfr_end", "pieriandx_case_creation_date"]
    )

    if cttso_lims_df_dedup.shape[0] < cttso_lims_df.shape[0]:
        # Update cttso lims sheet with replacement
        append_df_to_cttso_lims(cttso_lims_df_dedup, replace=True)
        # Wait for doc population
        sleep(3)

        # Collect new values
        cttso_lims_df: pd.DataFrame
        excel_row_number_mapping_df: pd.DataFrame
        cttso_lims_df, excel_row_number_mapping_df = get_cttso_lims()

    return merged_df_dedup, cttso_lims_df, excel_row_number_mapping_df


def get_pieriandx_case_id_from_merged_df_for_pending_case(cttso_lims_series, merged_df) -> Union[None, str]:
    """
    For a cttso lims series with a 'pending' case, use a join on subject id, library id, portal_wfr_run id
    to bind a pieriandx pending case with the actual pieriandx case id
    :param cttso_lims_series: A pandas Series with the following index
    :param merged_df: A pandas DataFrame with the following columns
    :return:
    """
    merged_rows = merged_df.query(
        f"subject_id=='{cttso_lims_series['subject_id']}' and "
        f"library_id=='{cttso_lims_series['library_id']}' and "
        f"portal_wfr_id=='{cttso_lims_series['portal_wfr_id']}'"
    )

    # Check we've gotten just one row
    if merged_rows.shape[0] == 0:
        logger.warning("Cannot be found in merged df")
        return None
    if merged_rows.shape[0] > 1:
        # Returning the 'latest' id makes sense but what if it hasn't been created yet
        # Otherwise this might be one to solve later on
        logger.warning(f"Got multiple entries for a given subject, library portal combo "
                       f"{cttso_lims_series['subject_id']}/{cttso_lims_series['library_id']}")
        logger.warning("So returning None as unsure how to collect the correct case id")
        return None

    pieriandx_case_id: str = merged_rows.squeeze()["pieriandx_case_id"]

    if pd.isnull(pieriandx_case_id):
        return None

    # Collect and return case id
    return pieriandx_case_id


def lambdas_awake() -> bool:
    """
    Go through the lambdas that are required for this service and make sure that they're all awake
    """
    required_lambdas_ssm_parameter_paths: List[str] = [
        PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH,
        REDCAP_APIS_LAMBDA_FUNCTION_ARN_SSM_PARAMETER,
        CLINICAL_LAMBDA_FUNCTION_SSM_PARAMETER_PATH,
        VALIDATION_LAMBDA_FUNCTION_ARN_SSM_PARAMETER_PATH
    ]

    # Initialise failed arns
    inactivate_required_lambda_arns: List[str] = []

    # Get ssm client
    ssm_client: SSMClient = get_boto3_ssm_client()
    lambda_client: LambdaClient = get_boto3_lambda_client()

    # Get lambda arns
    required_lambdas_arns: List[str] = []
    lambda_ssm_parameter: str
    for lambda_ssm_parameter in required_lambdas_ssm_parameter_paths:
        required_lambdas_arns.append(
            ssm_client.get_parameter(Name=lambda_ssm_parameter).get("Parameter").get("Value")
        )

    # Find inactive lambdas
    lambda_arn: str
    for lambda_arn in required_lambdas_arns:
        lambda_function_response: GetFunctionResponseTypeDef = lambda_client.get_function(
            FunctionName=lambda_arn
        )

        lambda_configuration_dict: FunctionConfigurationTypeDef = lambda_function_response.get("Configuration")

        state: LambdaFunctionStateType = lambda_configuration_dict.get("State")

        if not state.lower() == "active":
            logger.warning(f"Required lambda function '{lambda_arn}' is inactive, and is being warmed up")
            inactivate_required_lambda_arns.append(lambda_arn)

    # Check if we have any inactive lambdas
    if len(inactivate_required_lambda_arns) == 0:
        return True

    # Wake up lambdas
    lambda_arn: str
    wake_attempt_iter: int = 0
    while wake_attempt_iter < MAX_ATTEMPTS_WAKE_LAMBDAS:
        # Increment loop
        wake_attempt_iter += 1

        # Check if inactivated items is empty
        if len(inactivate_required_lambda_arns) == 0:
            return True

        # Iterate existing clients, check how many are inactive
        for lambda_arn in inactivate_required_lambda_arns:
            logger.info(f"Waking up lambda '{lambda_arn}'")
            try:
                lambda_invoke_response: InvocationResponseTypeDef = lambda_client.invoke(
                    FunctionName=lambda_arn,
                    Payload=json.dumps({})
                )
            except ClientError:
                # Next time
                pass
            else:
                # Lambda is awake, remove from list
                _ = inactivate_required_lambda_arns.pop(inactivate_required_lambda_arns.index(lambda_arn))

        # Small wait for lambdas to wake up
        sleep(5)

    logger.info("Couldn't wake up all of the downstream lambdas in time!")
    return False


def lambda_handler(event, context):
    """
    Neither event or context are used by the handler as this job is scheduled hourly
    :param event:
    :param context:
    :return:
    """
    # Get raw data values
    redcap_df: pd.DataFrame = get_full_redcap_data_df()
    redcap_df["in_redcap"] = True
    portal_df: pd.DataFrame = get_portal_workflow_run_data_df()
    portal_df["in_portal"] = True
    glims_df: pd.DataFrame = get_cttso_samples_from_glims()
    glims_df["in_glims"] = True
    pieriandx_df: pd.DataFrame = get_pieriandx_df()
    pieriandx_df["in_pieriandx"] = True

    # Merge data
    merged_df = merge_redcap_portal_and_glims_data(redcap_df, portal_df, glims_df)

    # Add pieriandx df to merged df
    merged_df = add_pieriandx_df_to_merged_df(merged_df, pieriandx_df)

    # Get existing sheet df
    cttso_lims_df: pd.DataFrame
    excel_row_number_mapping_df: pd.DataFrame
    cttso_lims_df, excel_row_number_mapping_df = get_cttso_lims()

    merged_df, cttso_lims_df, excel_row_number_mapping_df = \
        cleanup_duplicate_rows(merged_df, cttso_lims_df, excel_row_number_mapping_df)

    # Collect jobs that are yet to be completed
    pieriandx_incomplete_jobs_df: pd.DataFrame = get_pieriandx_incomplete_job_df_from_cttso_lims_df(cttso_lims_df=cttso_lims_df)

    # Update values for jobs with missing information
    if not pieriandx_incomplete_jobs_df.shape[0] == 0:
        pieriandx_jobs_missing_series: List = []
        for index, row in pieriandx_incomplete_jobs_df.iterrows():
            case_id = row["pieriandx_case_id"]
            if case_id == "failed":
                continue
            if case_id == "pending":
                case_id = get_pieriandx_case_id_from_merged_df_for_pending_case(row, merged_df)

            if case_id is not None and not pd.isnull(case_id):
                pieriandx_jobs_missing_series.append(get_pieriandx_status_for_missing_sample(case_id))

        # If any missing samples found, get latest info and update
        if not len(pieriandx_jobs_missing_series) == 0:
            pieriandx_job_status_missing_df = pd.concat(pieriandx_jobs_missing_series, axis="columns").transpose()

            # Merge pieriandx_job_status_missing_df with merged_df so all rows are present
            pieriandx_job_status_missing_df = update_pieriandx_job_status_missing_df(pieriandx_job_status_missing_df, merged_df)

            # Update cttso lims df
            update_cttso_lims(pieriandx_job_status_missing_df, cttso_lims_df, excel_row_number_mapping_df)

            # Reimport the data sheet after updating
            sleep(3)
            cttso_lims_df: pd.DataFrame
            excel_row_number_mapping_df: pd.DataFrame
            cttso_lims_df, excel_row_number_mapping_df = get_cttso_lims()

            # And perform another cleanup on new info
            merged_df, cttso_lims_df, excel_row_number_mapping_df = \
                cleanup_duplicate_rows(merged_df, cttso_lims_df, excel_row_number_mapping_df)

    # Get pieriandx df samples in merged df that are not in pieriandx_df
    processing_df = get_libraries_for_processing(merged_df)

    # Launch payloads for pieriandx_df samples that have no case id - if existent
    if not processing_df.shape[0] == 0:
        if not lambdas_awake():
            logger.error("Some of the required lambdas were asleep, waking them up now and reprocessing in an hour")

        processing_df = submit_libraries_to_pieriandx(processing_df)

        # Update merged df with pending processing df case ids
        merged_df = update_merged_df_with_processing_df(merged_df, processing_df)

    # Append new rows to cttso lims df
    append_to_cttso_lims(merged_df, cttso_lims_df, excel_row_number_mapping_df)

    # End of process
