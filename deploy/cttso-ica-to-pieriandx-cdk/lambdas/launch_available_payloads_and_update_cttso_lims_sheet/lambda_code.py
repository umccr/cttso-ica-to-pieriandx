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
import sys

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
from lambda_utils.aws_helpers import get_boto3_lambda_client, get_boto3_ssm_client, get_boto3_events_client
from lambda_utils.gspread_helpers import \
    get_cttso_lims, update_cttso_lims_row, \
    append_df_to_cttso_lims, add_deleted_cases_to_deleted_sheet, get_deleted_lims_df, set_google_secrets
from lambda_utils.logger import get_logger
from lambda_utils.pieriandx_helpers import get_pieriandx_df, get_pieriandx_status_for_missing_sample
from lambda_utils.portal_helpers import get_portal_workflow_run_data_df, get_cttso_samples_from_limsrow_df
from lambda_utils.redcap_helpers import get_full_redcap_data_df
from lambda_utils.globals import \
    PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH, \
    REDCAP_APIS_LAMBDA_FUNCTION_ARN_SSM_PARAMETER, \
    CLINICAL_LAMBDA_FUNCTION_SSM_PARAMETER_PATH, \
    VALIDATION_LAMBDA_FUNCTION_ARN_SSM_PARAMETER_PATH, \
    MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE, MAX_ATTEMPTS_WAKE_LAMBDAS, EVENT_RULE_FUNCTION_NAME_SSM_PARAMETER_PATH

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
      * glims_illumina_id
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
    """
    portal_redcap_df = pd.merge(portal_df, redcap_df,
                                on=["subject_id", "library_id"],
                                how="outer")

    # Use portal_sequence_run_name to drop values from glims df that
    # aren't in the portal df
    glims_df = glims_df.rename(
        columns={
            "glims_illumina_id": "portal_sequence_run_name"
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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_submission_time
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
      * pieriandx_case_identified
      * pieriandx_panel_type
      * pieriandx_sample_type
    :return: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * panel
      * sample_type
      * is_identified
      * needs_redcap
      * redcap_is_complete
    """

    # Initialise
    processing_columns = [
        "subject_id",
        "library_id",
        "portal_wfr_id",
        "panel",
        "sample_type",
        "is_identified",
        "needs_redcap",
        "redcap_is_complete"
    ]

    # Processing libraries must meet the following criteria
    # 1. Not in pieriandx
    # 2. In the portal
    # 3. Have a successful ICA tso500 workflow run
    # 4. Not be on a failed run
    # 5. Exist in either redcap or glims
    # Dont want to override global var
    merged_df = merged_df.copy()

    # Convert submission time into a datetime object
    merged_df["pieriandx_submission_time"] = pd.to_datetime(merged_df["pieriandx_submission_time"])
    # Check if submission time was over a week ago (and still dont have a case id)
    one_week_ago = (datetime.now() - timedelta(days=7)).date()

    to_process_df = merged_df.query(
        "("
        "  pieriandx_case_id.isnull() and "
        "  ( pieriandx_submission_time.isnull() or pieriandx_submission_time < @one_week_ago ) and "
        "  not in_pieriandx and "
        "  not portal_wfr_id.isnull() and "
        "  portal_wfr_status == 'Succeeded' and "
        "  portal_is_failed_run == False and "
        "  ( "
        "    ( "
        "      not redcap_is_complete.isnull() and redcap_is_complete.str.lower() == 'complete' "
        "    ) or "
        "    ( "
        "      glims_needs_redcap == False "
        "    ) "
        "  ) "
        ") ",
        engine="python"  # Required for the isnull bit - https://stackoverflow.com/a/54099389/6946787
    )

    if to_process_df.shape[0] == 0:
        # No processing to occur
        return pd.DataFrame(
            columns=processing_columns
        )

    # Check none of the processing df libraries are in the list of deleted lists
    deleted_lims_df, deleted_lims_excel_row_mapping_number = get_deleted_lims_df()
    process_row: pd.Series
    already_deleted_list_index = []
    for index, process_row in to_process_df.iterrows():
        if not deleted_lims_df.query(
            f"subject_id == '{process_row['subject_id']}' and "
            f"library_id == '{process_row['library_id']}' and "
            f"portal_wfr_id == '{process_row['portal_wfr_id']}'"
        ).shape[0] == 0:
            already_deleted_list_index.append(index)
            logger.warning(f"Already run and deleted this combination {process_row['subject_id']} / {process_row['library_id']} / {process_row['portal_wfr_id']}, not reprocessing")

    # Delete via index
    to_process_df = to_process_df.iloc[list(
        set(to_process_df.index.tolist()) - set(already_deleted_list_index)
    )]

    # Update columns to strip glims_ attributes
    new_column_names = [
      "panel",
      "sample_type",
      "is_identified",
      "needs_redcap"
    ]

    for column_name in new_column_names:
        to_process_df[column_name] = to_process_df[f"glims_{column_name}"]

    # Return subsetted dataframe
    return to_process_df[
        processing_columns
    ]


def submit_library_to_pieriandx(subject_id: str, library_id: str, workflow_run_id: str, lambda_arn: str, panel_type: str, sample_type: str, is_identified: bool):
    """
    Submit library to pieriandx
    :param subject_id:
    :param library_id:
    :param workflow_run_id:
    :param lambda_arn:
    :param panel_type:
    :return:
    """
    lambda_client: LambdaClient = get_boto3_lambda_client()

    lambda_payload: Dict = {
            "subject_id": subject_id,
            "library_id": library_id,
            "ica_workflow_run_id": workflow_run_id,
            "panel_type": panel_type,
            "sample_type": sample_type,
            "is_identified": is_identified
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
      * panel
      * sample_type
      * is_identified
      * needs_redcap
      * redcap_is_complete
    :return:
      A pandas dataframe with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * panel
      * sample_type
      * is_identified
      * needs_redcap
      * redcap_is_complete
      * submission_succeeded
      * submission_time
    """
    # Get number of rows to submit
    num_submissions = processing_df.shape[0]

    if num_submissions > MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE:
        logger.info(f"Dropping submission number from {num_submissions} to {MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE}")
        processing_df = processing_df.head(n=MAX_SUBMISSIONS_PER_LIMS_UPDATE_CYCLE)

    # Validation df
    # Validation if is validation sample or IS research sample with no redcap information
    processing_df["submission_arn"] = processing_df.apply(
        lambda x: get_validation_lambda_arn()
        if not x.needs_redcap and
           (
              # Sample not in RedCap
              (
                  pd.isnull(x.redcap_is_complete) or
                  not x.redcap_is_complete.lower() == "complete"
              )
           )
        else get_clinical_lambda_arn(),
        axis="columns"
    )

    processing_df["submission_succeeded"] = False

    for index, row in processing_df.iterrows():
        logger.info(f"Submitting the following subject id / library id to PierianDx")
        logger.info(f"SubjectID='{row.subject_id}', LibraryID='{row.library_id}', Workflow Run ID='{row.portal_wfr_id}'")
        logger.info(f"Submitted to arn: '{row.submission_arn}'")
        try:
            submit_library_to_pieriandx(
                subject_id=row.subject_id,
                library_id=row.library_id,
                workflow_run_id=row.portal_wfr_id,
                lambda_arn=row.submission_arn,
                panel_type=row.panel,
                sample_type=row.sample_type,
                is_identified=row.is_identified
            )
        except ValueError:
            pass
        else:
            processing_df.loc[index, "submission_succeeded"] = True
            processing_df.loc[index, "pieriandx_submission_time"] = datetime.utcnow().isoformat(sep=" ")

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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :param cttso_lims_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * in_glims
      * in_portal
      * in_redcap
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_submission_time
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_assignee
      * pieriandx_panel_type
      * pieriandx_sample_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
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
        * in_redcap
        * in_portal
        * in_glims
        * in_pieriandx
        * redcap_sample_type
        * redcap_is_complete
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_submission_time
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_panel_type
        * pieriandx_sample_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
    :return: A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * in_redcap
        * in_portal
        * in_glims
        * in_pieriandx
        * redcap_sample_type
        * redcap_is_complete
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_submission_time
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_panel_type
        * pieriandx_sample_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
    """
    static_statuses = ["complete", "failed", "canceled"]

    # Get cases where pieriandx case id is pending'
    # canceled is a static status but take exception when
    # pieriandx_report_status is 'canceled' we don't care
    # about updating the rest
    return cttso_lims_df.query(
        "( "
        "  ("
        "    pieriandx_case_id == 'pending' "
        "  ) "
        "  or "
        "  ( "
        "    ( "
        "      not pieriandx_case_id.isnull() "
        "    ) and "
        "    ( "
        "      not pieriandx_report_status == 'canceled' "
        "    ) and "
        "    ( "
        "      pieriandx_workflow_id.isnull() or "
        "      not pieriandx_workflow_status in @static_statuses or "
        "      not pieriandx_report_status in @static_statuses "
        "    ) "
        "  ) "
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
      * in_pieriandx
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :param processing_df: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * panel
      * sample_type
      * is_identified
      * needs_redcap
      * redcap_is_complete
      * submission_succeeded
      * submission_time
    :return: A pandas dataframe with the following columns
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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
      * pieriandx_submission_time
    """
    # Set the pieriandx case id to these samples as 'pending'
    for index, row in processing_df.iterrows():
        # Update the merged df row by the index of processing df
        if row.submission_succeeded:
            merged_df.loc[row.name, "pieriandx_case_id"] = "pending"
            merged_df.loc[row.name, "pieriandx_submission_time"] = row["pieriandx_submission_time"]
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
      * pieriandx_sample_type
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
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
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
    :return: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_redcap
      * in_portal
      * in_glims
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_creation_date
      * pieriandx_assignee
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
        "glims_project_owner",
        "glims_project_name",
        "glims_panel",
        "glims_sample_type",
        "glims_is_identified",
        "glims_default_snomed_term",
        "glims_needs_redcap",
        "pieriandx_submission_time",
        "pieriandx_case_id",
        "pieriandx_case_creation_date",
        "pieriandx_assignee"
    ]]

    # We merge right since we only want jobs we've picked up in the incomplete jobs df
    return pd.merge(
        merged_df, pieriandx_job_status_missing_df,
        on=["subject_id", "library_id", "pieriandx_case_id"],
        how="right"
    )


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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
    :param pieriandx_df: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :return: A pandas dataframe with the following columns:
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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
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

    # Add column for portal_wfr_end_est to set portal workflow into pieriandx timezone
    merged_df_with_pieriandx_df["portal_wfr_end_est_tz"] = merged_df_with_pieriandx_df["portal_wfr_end"].apply(
        lambda x: pd.to_datetime(x).astimezone(tz="US/Eastern") if not pd.isnull(x) else x
    )

    # For new workflow runs we flip the in_pieriandx boolean if the case creation date
    # is older than the existing pieriandx date
    # Set all pieriandx columns to NA
    invalid_pieriandx_indices = \
        merged_df_with_pieriandx_df.query(
            "pieriandx_case_creation_date.dt.date < "
            "portal_wfr_end_est_tz.dt.date"
        ).index

    # Drop portal_wfr_end_est_tz column
    merged_df_with_pieriandx_df.drop(
        columns=[
            "portal_wfr_end_est_tz"
        ],
        inplace=True
    )

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
        "  in_glims == False and "
        "  in_redcap == False and "
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
      * in_redcap
      * in_portal
      * in_glims
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_case_id
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :param cttso_lims_df: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * in_glims
      * in_portal
      * in_redcap
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_submission_time
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_assignee
      * pieriandx_panel_type
      * pieriandx_sample_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
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
            "pieriandx_assignee",
            "pieriandx_case_identified",
            "pieriandx_panel_type",
            "pieriandx_sample_type",
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
      * pieriandx_submission_time
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
      * pieriandx_panel_type
      * pieriandx_sample_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      # Duplicate columns
      * 'in_glims_lims',
      * 'in_portal_lims',
      * 'in_redcap_lims',
      * 'in_pieriandx_lims',
      * 'redcap_sample_type_lims',
      * 'redcap_is_complete_lims',
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * 'portal_wfr_end_lims',
      * 'portal_wfr_status_lims',
      * 'portal_sequence_run_name_lims',
      * 'portal_is_failed_run_lims',
      * 'pieriandx_case_accession_number_lims',
      * 'pieriandx_case_creation_date_lims'
      * 'pieriandx_assignee_lims'
    :return:
    """

    # Initialise list of case ids to drop
    case_ids_to_remove: List = []

    # Get date one week ago (used in a few situations)
    date_one_week_ago = datetime.utcnow().date() - timedelta(days=7)

    # Iterate through each grouping
    # Append rows to drop
    subject_id: str
    library_id: str
    portal_wfr_id: str
    mini_df: pd.DataFrame
    for (subject_id, library_id, portal_wfr_id), mini_df in lims_df.groupby(
            ["subject_id", "library_id", "portal_wfr_id"]):
        # Check if it's just a single row
        if mini_df.shape[0] == 1:
            # Single unique row - nothing to see here
            continue

        # Any cases with an assignee should be tracked
        # So drop any accession that have an assignment
        mini_df = mini_df.query(
            "pieriandx_assignee.isnull()",
            engine="python"
        )
        # No cases without assignment
        if mini_df.shape[0] == 0:
            continue

        # Check we don't have duplicate pieriandx case ids
        if not len(mini_df["pieriandx_case_id"].unique()) == mini_df.shape[0]:
            logger.info(f"Got duplicates pieriandx case ids "
                        f"for subject_id '{subject_id}', "
                        f"library_id '{library_id}' and "
                        f"portal_wfr_id '{portal_wfr_id}'")
            continue

        # Use existing lims filtering to drop cases where one row may have been manually selected
        # Edgecase for SBJ01666
        manually_filtered_subject_df = mini_df.dropna(axis="index", subset="pieriandx_case_accession_number_lims")
        if manually_filtered_subject_df.shape[0] < mini_df.shape[0] and not manually_filtered_subject_df.shape[0] == 0:
            accession_numbers_to_keep = manually_filtered_subject_df["pieriandx_case_accession_number_lims"].tolist()
            case_ids_to_remove.extend(
                mini_df.query(
                   "pieriandx_case_accession_number_lims not in @accession_numbers_to_keep and "
                   "pieriandx_case_creation_date < @date_one_week_ago"
                )["pieriandx_case_id"].tolist()
            )
            mini_df = manually_filtered_subject_df

        # Re check if mini df is just one row
        # Check if it's just a single row
        if mini_df.shape[0] == 1:
            # Single unique row - nothing to see here
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

            if pd.Timestamp(last_row["pieriandx_case_creation_date"]).date() < date_one_week_ago:
                # This last row is over a week old and has no workflow status or report status so remove it
                case_ids_to_remove.append(mini_df.loc[mini_df_indexes[-1], "pieriandx_case_id"])

    case_ids_to_remove = [case_id
                          for case_id in case_ids_to_remove
                          if not pd.isnull(case_id)]

    print(case_ids_to_remove)

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
      * in_pieriandx
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :param cttso_lims_df: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * in_glims
      * in_portal
      * in_redcap
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_submission_time
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_assignee
      * pieriandx_panel_type
      * pieriandx_sample_type
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
        * in_pieriandx
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_assignee
      ,
      A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * in_glims
        * in_portal
        * in_redcap
        * in_pieriandx
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_submission_time
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_assignee
        * pieriandx_panel_type
        * pieriandx_sample_type
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

    # We might have needed to reset the lims db
    if not cttso_lims_df.shape[0] == 0:
        merged_lims_df = bind_pieriandx_case_submission_time_to_merged_df(merged_lims_df, cttso_lims_df)

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
    if len(mini_dfs) == 0:
        logger.info("Glims is empty, skipping deduplication")
    else:
        cttso_lims_df_dedup = pd.concat(mini_dfs)

    cttso_lims_df_dedup = cttso_lims_df_dedup.sort_values(
        by=["portal_sequence_run_name", "portal_wfr_end", "pieriandx_case_creation_date"]
    )

    if cttso_lims_df_dedup.shape[0] < cttso_lims_df.shape[0]:
        # Update cttso lims sheet with replacement
        append_df_to_cttso_lims(cttso_lims_df_dedup, replace=True)
        # Wait for doc population
        sleep(10)

        # Collect new values
        cttso_lims_df: pd.DataFrame
        excel_row_number_mapping_df: pd.DataFrame
        cttso_lims_df, excel_row_number_mapping_df = get_cttso_lims()

    if not cttso_lims_df.shape[0] == 0:
        merged_df_dedup = bind_pieriandx_case_submission_time_to_merged_df(merged_df_dedup, cttso_lims_df)
    else:
        merged_df_dedup["pieriandx_submission_time"] = pd.NA

    return merged_df_dedup, cttso_lims_df, excel_row_number_mapping_df


def get_pieriandx_case_id_from_merged_df_for_pending_case(cttso_lims_series, merged_df) -> Union[None, str]:
    """
    For a cttso lims series with a 'pending' case, use a join on subject id, library id, portal_wfr_run id
    to bind a pieriandx pending case with the actual pieriandx case id
    :param cttso_lims_series: A pandas Series with the following index
    :param merged_df: A pandas DataFrame with the following columns
    :return:
    """

    subject_id: str = cttso_lims_series['subject_id']
    library_id: str = cttso_lims_series['library_id']
    portal_wfr_id: str = cttso_lims_series['portal_wfr_id']

    merged_rows = merged_df.query(
        f"subject_id=='{subject_id}' and "
        f"library_id=='{library_id}' and "
        f"portal_wfr_id=='{portal_wfr_id}'"
    )

    # Check we've gotten just one row
    if merged_rows.shape[0] == 0:
        logger.warning(f"Subject '{subject_id}', library '{library_id}', '{portal_wfr_id}' cannot be found in merged df")
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


def bind_pieriandx_case_submission_time_to_merged_df(merged_df: pd.DataFrame, cttso_lims_df: pd.DataFrame) -> pd.DataFrame:
    """
    plit lims submission into two
    Rows with a valid pieriandx case accession id
    Rows that do not have a valid pieriandx case accession id
    We bind first set on pieriandx_case_id - simples
    For remaining merged df (we try bind on 'pending')
    If we still have remaining, we set pieriandx submission time as day of creation
    :param merged_df: A pandas dataframe with the following columns:
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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :param cttso_lims_df: A pandas dataframe with the following columns:
      * subject_id
      * library_id
      * in_glims
      * in_portal
      * in_redcap
      * in_pieriandx
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * redcap_sample_type
      * redcap_is_complete
      * portal_wfr_id
      * portal_wfr_end
      * portal_wfr_status
      * portal_sequence_run_name
      * portal_is_failed_run
      * pieriandx_submission_time
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_case_identified
      * pieriandx_assignee
      * pieriandx_panel_type
      * pieriandx_sample_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
    :return: A pandas DataFrame with the following columns:
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
      * pieriandx_submission_time
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
      * pieriandx_panel_type
      * pieriandx_sample_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      # Duplicate columns
      * 'in_glims_lims',
      * 'in_portal_lims',
      * 'in_redcap_lims',
      * 'in_pieriandx_lims',
      * 'redcap_sample_type_lims',
      * 'redcap_is_complete_lims',
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * 'portal_wfr_end_lims',
      * 'portal_wfr_status_lims',
      * 'portal_sequence_run_name_lims',
      * 'portal_is_failed_run_lims',
      * 'pieriandx_case_accession_number_lims',
      * 'pieriandx_case_creation_date_lims'
      * 'pieriandx_assignee_lims'
    """

    cttso_lims_df_valid_merge = cttso_lims_df.query(
        "not pieriandx_case_id.isnull() and "
        "not pieriandx_submission_time.isnull() "
    )[["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id", "pieriandx_submission_time"]].drop_duplicates()

    cttso_lims_df_with_valid_case_id = cttso_lims_df_valid_merge.query(
        "pieriandx_case_id.str.isdigit()",
        engine="python"
    )

    # Not null but might be 'pending'
    cttso_lims_df_without_valid_case_id = cttso_lims_df_valid_merge.query(
        "not pieriandx_case_id.str.isdigit()",
        engine="python"
    )

    # If we have a submission time, we pull it in again from cttso lims
    if "pieriandx_submission_time" in merged_df.columns.tolist():
        merged_df = merged_df.drop(
            columns=[
                "pieriandx_submission_time"
            ]
        )

    # Merge on pieriandx case id
    merged_lims_df_valid = pd.merge(
        merged_df, cttso_lims_df_with_valid_case_id,
        how="left",
        on=["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id"]
    )

    # Join pieriandx submission time for merged_lims_df where pieriandx_submission_time is null?
    merged_lims_df_invalid = pd.merge(
        merged_df, cttso_lims_df_without_valid_case_id,
        how="left",
        on=["subject_id", "library_id", "portal_wfr_id"],
        suffixes=("_merged", "_lims")
    )

    # Work over merged_lims_df_invalid, for cases
    # Where we now have duplicate columns
    # pieriandx_case_id_merged / pieriandx_case_id_lims
    # For pieriandx case id, we have a couple of use-cases
    # Case 1: pieriandx_case_id_merged is 'int' and pieriandx_case_id_lims is 'pending'
    #   pieriandx_case_id is set to pieriandx_case_id_merged
    # Case 2: pieriandx_case_id_merged is NA and pieriandx_case_id_lims is 'pending'
    #   sample just submitted (maybe manually?) just check date is not too old?
    # Case 3: pieriandx_case_id_merged is 'int' and pieriandx_case_id_lims is NA ( for manually run samples )
    # Case 4: Both are null (for sample not submitted)
    new_merged_df_invalid_rows = []
    index: int
    row: pd.Series
    for index, row in merged_lims_df_invalid.iterrows():
        new_series = pd.Series(row)
        # Collect cells
        pieriandx_case_id_merged = row['pieriandx_case_id_merged']
        pieriandx_case_id_lims = row['pieriandx_case_id_lims']
        pieriandx_case_submission_time = row['pieriandx_submission_time']

        # Case 4 deal with NA comparison first
        # Case 4 - both are null (when sample has not been submitted or is omitted from submission (NTCs))
        if pd.isnull(pieriandx_case_id_merged) and pd.isnull(pieriandx_case_id_lims):
            new_series["pieriandx_case_id"] = pieriandx_case_id_merged
            new_merged_df_invalid_rows.append(new_series)
            continue

        # Case 1
        elif str(pieriandx_case_id_merged).isdigit() and pieriandx_case_id_lims == "pending":
            new_series["pieriandx_case_id"] = pieriandx_case_id_merged
            new_merged_df_invalid_rows.append(new_series)
            continue

        # Case 2
        elif pd.isnull(pieriandx_case_id_merged) and pieriandx_case_id_lims == 'pending':
            # Check sample submission time is not too old
            logger.info(f"Got 'pending' case id for sample subject / library / portal "
                        f"{row['subject_id']}, {row['library_id']} {row['portal_wfr_id']} "
                        f"but never got a matching pieriandx accession number")
            one_week_ago = (datetime.utcnow() - timedelta(days=7)).date()

            # All actions are the same - just logging is different
            if pd.isnull(pieriandx_case_submission_time):
                logger.info("Pieriandx Case Submission time is null, we just merge as null")
            elif pd.to_datetime(pieriandx_case_submission_time).tz_localize("UTC") < one_week_ago:
                logger.info("Case pending for over one week, this case will be resubmitted")
            else:
                logger.info("Case pending for less than one week, please resubmit manually")

            # Use the merged df case id
            new_series["pieriandx_case_id"] = pieriandx_case_id_merged
            new_merged_df_invalid_rows.append(new_series)
            continue

        # Case 3
        elif str(pieriandx_case_id_merged).isdigit() and pd.isnull(pieriandx_case_id_lims):
            new_series["pieriandx_case_id"] = pieriandx_case_id_merged
            new_merged_df_invalid_rows.append(new_series)
            continue
        else:
            new_series["pieriandx_case_id"] = pieriandx_case_id_merged
            new_merged_df_invalid_rows.append(new_series)
            continue

    # Create dataframe out of new rows
    merged_lims_df_invalid = pd.DataFrame(new_merged_df_invalid_rows).drop(
        columns=[
            "pieriandx_case_id_merged",
            "pieriandx_case_id_lims"
        ]
    )

    # Check if duplicate rows for pieriandx submission time
    merged_lims_df_valid_and_invalid_df = pd.concat(
        [
            merged_lims_df_valid,
            merged_lims_df_invalid
        ],
        ignore_index=True
    )[["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id", "pieriandx_submission_time"]]

    # Drop duplicates but fill pieriandx submission time
    new_rows = []
    for (subject_id, library_id, portal_wfr_id, pieriandx_case_id), time_df in merged_lims_df_valid_and_invalid_df.groupby(
        ["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id"]
    ):
        if time_df.shape[0] == 1:
            new_rows.append(time_df)
            continue

        # Select time_df where pieriandx_submission_time is not null
        valid_submission_time_df = time_df.query("not pieriandx_submission_time.isnull()", engine="python")
        if valid_submission_time_df.shape[0] == 1:
            new_rows.append(valid_submission_time_df)
            continue

        # Added new rows
        new_rows.append(
            time_df.drop_duplicates(
                subset=[
                    "subject_id", "library_id",
                    "portal_wfr_id", "pieriandx_case_id"
                ],
                keep="first"
            )
        )

    merged_lims_df_valid_and_invalid_df = pd.concat(
        new_rows,
        ignore_index=True
    )

    # Rebind onto merged df
    merged_lims_df = merged_df.merge(
        merged_lims_df_valid_and_invalid_df,
        how="left",
        on=["subject_id", "library_id", "portal_wfr_id", "pieriandx_case_id"]
    )

    return merged_lims_df


def drop_to_be_deleted_cases(merged_df: pd.DataFrame, cttso_lims_df: pd.DataFrame, excel_row_mapping_number_df: pd.DataFrame, existing_pieriandx_cases: List) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Cases that have been assigned to ToBeDeleted need to be dropped from row list
    and instead attached to a new sheet
    :param merged_df:
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
      * glims_project_owner
      * glims_project_name
      * glims_panel
      * glims_sample_type
      * glims_is_identified
      * glims_default_snomed_term
      * glims_needs_redcap
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date
      * pieriandx_assignee
    :param cttso_lims_df:
     A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * in_glims
        * in_portal
        * in_redcap
        * in_pieriandx
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_submission_time
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_assignee
        * pieriandx_panel_type
        * pieriandx_sample_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
    :param excel_row_mapping_number_df:
      A pandas DataFrame with the following columns:
        * cttso_lims_index
        * excel_row_number
    :param existing_pieriandx_cases:
      A list of pieriandx cases we have
    :return:
    """
    # Split cttso lims df by query
    to_be_deleted_cases_lims = cttso_lims_df.query(
        "pieriandx_assignee == 'ToBeDeleted' or "
        "( "
        "  pieriandx_case_id not in @existing_pieriandx_cases and "
        "  not pieriandx_case_id.isnull()"
        ")",
        engine="python"
    )
    to_be_deleted_cases_merged_df = merged_df.query("pieriandx_assignee == 'ToBeDeleted'")

    # Check cases that are not in pieriandx
    if len(existing_pieriandx_cases) == 0:
        logger.error("Something seriously wrong has happened! Got an empty list of cases")
        raise ValueError
    # Cases that have already been deleted are ones in the lims df that cannot be found in pieriandx
    cases_already_deleted = list(
        set(
            cttso_lims_df["pieriandx_case_id"].dropna().tolist()
        ) -
        set(
            existing_pieriandx_cases
        )
    )

    if to_be_deleted_cases_lims.shape[0] == 0 and to_be_deleted_cases_merged_df.shape[0] == 0 and len(cases_already_deleted) == 0:
        logger.info("Nothing to transfer to delete pile")
        return merged_df, cttso_lims_df, excel_row_mapping_number_df

    # Remove cases assigned to the ToBeDeleted user or cases that have already been deleted
    cttso_lims_df_cleaned = cttso_lims_df.query(
        "pieriandx_assignee != 'ToBeDeleted' and not "
        "pieriandx_case_id in @cases_already_deleted"
    )
    clean_cttso_case_ids_list = cttso_lims_df_cleaned["pieriandx_case_id"].tolist()
    deleted_case_ids_list = to_be_deleted_cases_lims["pieriandx_case_id"].tolist()

    # Clean out merged df with existing deleted cases
    # And any cases we're about to put into the deleted lims as well
    deleted_lims_df, deleted_lims_excel_row_mapping_number = get_deleted_lims_df()
    case_ids_to_remove_from_merged_df = list(
        set(
            deleted_lims_df["pieriandx_case_id"].tolist() +
            to_be_deleted_cases_lims["pieriandx_case_id"].tolist()
        )
    )

    # If the case id is in both, we need to keep it, and have it reassigned
    merged_df = merged_df.query(
        "pieriandx_case_id.isnull() or "
        "pieriandx_case_id not in @case_ids_to_remove_from_merged_df or "
        "( "
        "  pieriandx_case_id in @clean_cttso_case_ids_list and "
        "  pieriandx_case_id in @deleted_case_ids_list "
        ")",
        engine="python"
    )

    # Remove cases from merged df that have already been deleted
    merged_df = merged_df.query(
        "pieriandx_case_id not in @cases_already_deleted",
        engine="python"
    )

    # Update cttso lims sheet with replacement
    append_df_to_cttso_lims(cttso_lims_df_cleaned, replace=True)
    # Wait for doc population
    sleep(10)

    # Collect new values
    cttso_lims_df: pd.DataFrame
    excel_row_number_mapping_df: pd.DataFrame
    cttso_lims_df, excel_row_number_mapping_df = get_cttso_lims()

    # Update deleted sheet - note we only add in the cases that are in the LIMS -
    # cases in merged_df will need to be updated into LIMS first THEN pulled out of LIMS in the next iteration of this
    # lambda script
    add_deleted_cases_to_deleted_sheet(to_be_deleted_cases_lims)

    return merged_df, cttso_lims_df, excel_row_number_mapping_df


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


def disable_event_rule():
    """
    List and disable event trigger (important to prevent duplicate runs)
    :return:
    """
    # Clients
    ssm_client = get_boto3_ssm_client()
    events_client = get_boto3_events_client()

    event_function_name = ssm_client.get_parameter(Name=EVENT_RULE_FUNCTION_NAME_SSM_PARAMETER_PATH)\
        .get("Parameter")\
        .get("Value")

    # Disable rule
    events_client.disable_rule(Name=event_function_name)


def lambda_handler(event, context):
    """
    Neither event or context are used by the handler as this job is scheduled hourly
    :param event:
    :param context:
    :return:
    """
    # Set GLIMS Secrets env vars
    set_google_secrets()

    # Get raw data values
    redcap_df: pd.DataFrame = get_full_redcap_data_df()
    redcap_df["in_redcap"] = True
    portal_df: pd.DataFrame = get_portal_workflow_run_data_df()
    portal_df["in_portal"] = True
    glims_df: pd.DataFrame = get_cttso_samples_from_limsrow_df()
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

    # Clean out to-be-deleted cases
    merged_df, cttso_lims_df, excel_row_number_mapping_df = \
        drop_to_be_deleted_cases(merged_df, cttso_lims_df, excel_row_number_mapping_df, pieriandx_df["pieriandx_case_id"].tolist())

    merged_df, cttso_lims_df, excel_row_number_mapping_df = \
        cleanup_duplicate_rows(merged_df, cttso_lims_df, excel_row_number_mapping_df)

    # Collect jobs that are yet to be completed
    pieriandx_incomplete_jobs_df: pd.DataFrame = get_pieriandx_incomplete_job_df_from_cttso_lims_df(cttso_lims_df=cttso_lims_df)

    # Update values for jobs with missing information
    if not pieriandx_incomplete_jobs_df.shape[0] == 0:
        logger.info(f"Attempting to update {pieriandx_incomplete_jobs_df.shape[0]} rows of jobs that are incomplete")
        pieriandx_jobs_missing_series: List = []
        for index, row in pieriandx_incomplete_jobs_df.iterrows():
            case_id = row["pieriandx_case_id"]
            if case_id == "failed":
                continue
            if case_id == "pending":
                case_id = get_pieriandx_case_id_from_merged_df_for_pending_case(row, merged_df)
                logger.info(f"Got case '{case_id}' for pending analysis {row['subject_id']} {row['library_id']}")

            if case_id is not None and not pd.isnull(case_id):
                pieriandx_jobs_missing_series.append(get_pieriandx_status_for_missing_sample(case_id))

        # If any missing samples found, get latest info and update
        if not len(pieriandx_jobs_missing_series) == 0:
            logger.info(f"Got {len(pieriandx_jobs_missing_series)} rows to replace")
            pieriandx_job_status_missing_df = pd.concat(pieriandx_jobs_missing_series, axis="columns").transpose()

            # Merge pieriandx_job_status_missing_df with merged_df so all rows are present
            pieriandx_job_status_missing_df = update_pieriandx_job_status_missing_df(pieriandx_job_status_missing_df, merged_df)

            # Update cttso lims df
            logger.info("Updating lims")
            update_cttso_lims(pieriandx_job_status_missing_df, cttso_lims_df, excel_row_number_mapping_df)

            # Reimport the data sheet after updating (give it a minute)
            sleep(60)

            # Reimport lims
            cttso_lims_df: pd.DataFrame
            excel_row_number_mapping_df: pd.DataFrame
            cttso_lims_df, excel_row_number_mapping_df = get_cttso_lims()

            # And perform another cleanup on new info
            merged_df, cttso_lims_df, excel_row_number_mapping_df = \
                cleanup_duplicate_rows(merged_df, cttso_lims_df, excel_row_number_mapping_df)

    # Get pieriandx df samples in merged df that are not in pieriandx_df
    processing_df = get_libraries_for_processing(merged_df)

    # Check if any of the processing df are present in the incomplete jobs df
    for index, row in processing_df.iterrows():
        if pieriandx_incomplete_jobs_df.query(
                f"subject_id=='{row['subject_id']}' "
                f"and "
                f"library_id=='{row['library_id']}'"
        ).shape[0] > 0:
            logger.error(
                f"Issue in getting library for processing, {row['subject_id']}/{row['library_id']} "
                f"has already been submitted for processing. "
                "Please fix this manually, stopping all further submissions then reenable the event bridge"
            )
            disable_event_rule()

            sys.exit(1)

    # Launch payloads for pieriandx_df samples that have no case id - if existent
    if not processing_df.shape[0] == 0:
        if not lambdas_awake():
            logger.error("Some of the required lambdas were asleep, waking them up now and reprocessing in an hour")
            raise ValueError

        processing_df = submit_libraries_to_pieriandx(processing_df)

        # Update merged df with pending processing df case ids
        merged_df = update_merged_df_with_processing_df(merged_df, processing_df)

    # Append new rows to cttso lims df
    append_to_cttso_lims(merged_df, cttso_lims_df, excel_row_number_mapping_df)

    # End of process
    logger.info("End of lims script")


## LOCAL DEBUG ONLY ##
# if __name__ == "__main__":
#     lambda_handler(None, None)
