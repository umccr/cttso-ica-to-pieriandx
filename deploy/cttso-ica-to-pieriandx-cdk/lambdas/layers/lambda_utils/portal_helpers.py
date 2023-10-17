#!/usr/bin/env python3
import json

from mypy_boto3_ssm.client import SSMClient
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import pandas as pd
from typing import Dict, List
from requests import Response
import requests
from urllib.parse import urlparse

from .globals import \
    PORTAL_API_BASE_URL_SSM_PATH, \
    PORTAL_METADATA_ENDPOINT, \
    PORTAL_WORKFLOWS_ENDPOINT, \
    PORTAL_WORKFLOW_ORDERING, \
    PORTAL_MAX_ROWS_PER_PAGE, \
    PORTAL_CTTSO_WORKFLOW_TYPE_NAME, \
    PORTAL_FIELDS, \
    WFR_NAME_REGEX, PORTAL_SEQUENCE_RUNS_ENDPOINT, PORTAL_LIMSROW_ENDPOINT, PORTAL_CTTSO_SAMPLE_TYPE, \
    PORTAL_CTTSO_SAMPLE_ASSAY, PORTAL_CTTSO_SAMPLE_PHENOTYPE, LIMS_PROJECT_NAME_MAPPING_SSM_PATH

from .aws_helpers import get_aws_region, get_boto3_ssm_client
from .logger import get_logger

logger = get_logger()


def get_portal_creds(portal_base_url: str) -> BotoAWSRequestsAuth:
    """
    Get the credentials for hitting the data portal apis.
    :return:
    """
    return BotoAWSRequestsAuth(
        aws_host=urlparse(portal_base_url).hostname,
        aws_region=get_aws_region(),
        aws_service='execute-api',
    )


def get_portal_base_url() -> str:
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(
        Name=PORTAL_API_BASE_URL_SSM_PATH
    ).get("Parameter").get("Value")


def get_portal_sequence_run_data_df() -> pd.DataFrame:
    """
    Get the portal sequence run dataframe
    :return: A pandas dataframe with the following columns:
      * portal_sequence_run_id
      * portal_sequence_run_name
      * portal_sequence_run_status
    """
    portal_base_url = get_portal_base_url()
    portal_url_endpoint = PORTAL_SEQUENCE_RUNS_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    all_results = []
    page_number = 1

    while True:
        req: Response = requests.get(
            url=portal_url_endpoint,
            auth=portal_auth,
            params={
                "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE,
                "page": page_number
            }
        )

        req_dict: Dict = req.json()

        results: List
        if (results := req_dict.get("results", None)) is None:
            raise ValueError

        # Extend all results
        all_results.extend(results)

        # Get next page or break
        if req_dict.get("links", {}).get("next", None) is not None:
            page_number += 1
        else:
            break

    portal_cttso_sequence_runs_df = pd.DataFrame(all_results)

    portal_cttso_sequence_runs_df = portal_cttso_sequence_runs_df.rename(
        columns={
            "id": "portal_sequence_run_id",
            "name": "portal_sequence_run_name",
            "status": "portal_sequence_run_status",
        }
    )

    return portal_cttso_sequence_runs_df[
        [
            "portal_sequence_run_id",
            "portal_sequence_run_name",
            "portal_sequence_run_status"
        ]
    ]


def get_portal_workflow_run_data_df() -> pd.DataFrame:
    """
    Use the aws api.metadata endpoint to pull in
    portal information on the ICA workflow run ID

    :return: A pandas DataFrame with the following columns
      * subject_id
      * library_id
      * portal_wfr_id
      * portal_wfr_end                -> The end timestamp of the workflow
      * portal_wfr_status             -> The status of the workflow run
      * portal_sequence_run_name      -> The sequence run name from this cttso sample
      * portal_is_failed_run          -> Did the sequence run associated with the fastq inputs of this workflow pass or fail
    """

    portal_base_url = get_portal_base_url()
    portal_url_endpoint = PORTAL_WORKFLOWS_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    # Initialise page and appended list
    all_results = []
    page_number = 1

    while True:
        req: Response = requests.get(
            url=portal_url_endpoint,
            auth=portal_auth,
            params={
                "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE,
                "type_name": PORTAL_CTTSO_WORKFLOW_TYPE_NAME,
                "ordering": PORTAL_WORKFLOW_ORDERING,
                "page": page_number
            }
        )

        req_dict: Dict = req.json()

        results: List
        if (results := req_dict.get("results", None)) is None:
            raise ValueError

        # Extend all results
        all_results.extend(results)

        # Get next page
        if req_dict.get("links", {}).get("next", None) is not None:
            page_number += 1
        else:
            break

    # Convret to dataframe
    portal_cttso_workflow_runs_df = pd.DataFrame(all_results)

    # Rename df appropriately
    portal_cttso_workflow_runs_df = portal_cttso_workflow_runs_df.rename(
        columns={
            "wfr_id": "portal_wfr_id",
            "end": "portal_wfr_end",
            "end_status": "portal_wfr_status",
            "sequence_run": "portal_sequence_run_id",
        }
    )

    # Get sequence run dataframe
    portal_sequence_runs_data_df = get_portal_sequence_run_data_df()

    # Merge sequence run dataframe onto existing dataframe
    portal_cttso_workflow_runs_df = pd.merge(
        portal_cttso_workflow_runs_df, portal_sequence_runs_data_df,
        on=["portal_sequence_run_id"]
    )

    # Get subject id and library id from wfr name
    portal_cttso_workflow_runs_df["subject_id"] = portal_cttso_workflow_runs_df.apply(
        lambda x: x.wfr_name.split("__")[3],
        axis="columns"
    )
    portal_cttso_workflow_runs_df["library_id"] = portal_cttso_workflow_runs_df.apply(
        lambda x: x.wfr_name.split("__")[4],
        axis="columns"
    )

    # Get if failed run
    portal_cttso_workflow_runs_df["portal_is_failed_run"] = portal_cttso_workflow_runs_df.apply(
        lambda x: True if x.portal_sequence_run_status.lower() == "failed" else False,
        axis="columns"
    )

    # Only get workflows that have finished (running ones might confuse things a little)
    finished_statuses = [  # This array is used in the next command
        "aborted",
        "failed",
        "succeeded"
    ]
    portal_cttso_workflow_runs_df = portal_cttso_workflow_runs_df.query(
        "portal_wfr_status.str.lower() in @finished_statuses"
    )

    mini_dfs: List[pd.DataFrame] = []
    for (subject_id, library_id), mini_df in portal_cttso_workflow_runs_df.groupby(["subject_id", "library_id"]):
        mini_df = mini_df.drop_duplicates()
        if mini_df.shape[0] == 1:
            mini_dfs.append(mini_df)
            continue
        # Remove portal failed runs if one has passed
        if len(mini_df["portal_is_failed_run"].unique().tolist()) > 1:
            only_succeeded_sequence_runs_df: pd.DataFrame = mini_df.query("portal_is_failed_run == False")
            if only_succeeded_sequence_runs_df.shape[0] > 0:
                # Remove failed portal workflow runs
                mini_df = only_succeeded_sequence_runs_df
        # Remove failed portal workflow runs that means we still have at least one valid row
        if len(mini_df["portal_wfr_status"].unique().tolist()) > 1:
            only_succeeded_workflow_runs_df: pd.DataFrame = mini_df.query("portal_wfr_status.str.lower() == 'succeeded'")
            if only_succeeded_workflow_runs_df.shape[0] > 0:
                # Remove failed portal workflow runs
                mini_df = only_succeeded_workflow_runs_df
        # If multiple sequence runs (with same sequence run and workflow run status), filter to latest
        if len(mini_df["portal_sequence_run_name"].unique().tolist()) > 1:
            latest_sequence_run: str = mini_df.sort_values(
                by="portal_sequence_run_name",
                # Sort in ascending manner
                ascending=True,
                # But put na values first
                na_position="first"
            )["portal_sequence_run_name"].tolist()[-1]
            mini_df = mini_df.query(f"portal_sequence_run_name=='{latest_sequence_run}'")
        # If multiple workflow runs (with same sequence run, and workflow run status)
        # Get the latest run by portal workflow run status
        latest_workflow_run_end_time: str = mini_df.sort_values(
            by="portal_wfr_end",
            # Sort in ascending manner
            ascending=True,
            # But put na values first
            na_position="first"
        )["portal_wfr_end"].tolist()[-1]
        mini_df = mini_df.query(f"portal_wfr_end=='{latest_workflow_run_end_time}'")

        # Append now since were very likely to be down to one row
        mini_dfs.append(mini_df)

    # Merge everything back together
    portal_cttso_workflow_runs_df = pd.concat(mini_dfs)

    # Convert portal_wfr_end to date before returning
    portal_cttso_workflow_runs_df['portal_wfr_end'] = pd.to_datetime(portal_cttso_workflow_runs_df['portal_wfr_end'])

    return portal_cttso_workflow_runs_df[
        [
            "subject_id",
            "library_id",
            "portal_wfr_id",
            "portal_wfr_end",
            "portal_wfr_status",
            "portal_sequence_run_name",
            "portal_is_failed_run"""
        ]
    ]


def get_clinical_metadata_information_from_portal_for_subject(subject_id: str, library_id: str) -> pd.DataFrame:
    """
    Get the required information from the data portal
    * External Sample ID -> External Specimen ID
    * External Subject ID -> Patient URN
    :param subject_id:
    :param library_id:
    :return: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * project_name
      * external_sample_id
      * external_subject_id
    """

    portal_base_url = get_portal_base_url()
    portal_url_endpoint = PORTAL_METADATA_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    req: Response = requests.get(
        url=portal_url_endpoint,
        auth=portal_auth,
        params={
            "subject_id": subject_id,
            "library_id": library_id
        }
    )

    req_dict: Dict = req.json()

    results: List
    if (results := req_dict.get("results", None)) is None:
        logger.error(f"Did not get any results on {portal_url_endpoint}, "
                     f"subject id: {subject_id}, library id: {library_id}")
        raise AttributeError

    # Check length of results
    if not len(results) == 1:
        logger.error(f"Expected only one entry for subject id: {subject_id}, library id: {library_id}")
        raise ValueError

    result: Dict = results[0]

    # Ensure the expected keys are present
    field: str
    for field in PORTAL_FIELDS:
        if field not in result.keys():
            logger.error(f"Expected {field} in portal metadata query but only got {list(result.keys())}")

    logger.info("Completed async function and returning metadata information from portal")

    return pd.DataFrame([result])[PORTAL_FIELDS]


def get_ica_workflow_run_id_from_portal(subject_id: str, library_id: str) -> str:
    """
    Get the ICA workflow run ID from the portal name
    wfr_name will look something like: umccr__automated__tso_ctdna_tumor_only__SBJ02091__L2200593__202205245ae2e876
    Get latest successful run
    :param subject_id:
    :param library_id:
    :return:
    """
    portal_base_url = get_portal_base_url()
    portal_url_endpoint = PORTAL_WORKFLOWS_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    all_results = []
    page_number = 1

    while True:
        req: Response = requests.get(
            url=portal_url_endpoint,
            auth=portal_auth,
            params={
                "type_name": PORTAL_CTTSO_WORKFLOW_TYPE_NAME,
                "end_status": "Succeeded",
                "ordering": PORTAL_WORKFLOW_ORDERING,
                "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE
            }
        )

        req_dict: Dict = req.json()

        results: List
        if (results := req_dict.get("results", None)) is None:
            logger.error("Could not get requests from portal workflow endpoint")
            raise ValueError

        # Extend all results
        all_results.extend(results)

        # Get next page
        if req_dict.get("links", {}).get("next", None) is not None:
            page_number += 1
        else:
            break

    # Collect data frames
    cttso_workflows_df: pd.DataFrame = pd.DataFrame(all_results)

    cttso_workflows_df["subject_id"] = cttso_workflows_df.apply(
        lambda x: WFR_NAME_REGEX.fullmatch(x.wfr_name).groups(1),
        axis="columns"
    )

    cttso_workflows_df["library_id"] = cttso_workflows_df.apply(
        lambda x: WFR_NAME_REGEX.fullmatch(x.wfr_name).groups(2),
        axis="columns"
    )

    # Filter workflows
    cttso_workflows_df = cttso_workflows_df.query(f"subject_id=='{subject_id}' && library_id=='{library_id}'")

    if cttso_workflows_df.shape[0] == 0:
        logger.error(f"Could not find cttso workflow for subject {subject_id} and library id {library_id}")
        raise ValueError

    logger.info("Completing async function 'collecting ICA workflow run ID from portal'")

    # Collect the workflow run id from the most recent run
    return cttso_workflows_df["wfr_id"].tolist()[0]


def get_ssm_project_mapping_json() -> List:
    """
    Returns the json object that maps project owner to sample type -
    Looks like this
    [
      {
        "project_owner": "VCCC",
        "project_name": "PO",
        "panel": "subpanel",
        "sample_type": "patient_care_sample",
        "is_identified": "identified",
        "default_snomed_term":null
      },
      ...
      {
        "project_owner": "UMCCR",
        "project_name": "Control",
        "panel": "main",
        "sample_type": "validation",
        "is_identified": "deidentified",
        "default_snomed_term": "Disseminated malignancy of unknown primary"
      },
      {
        "project_owner": "*",
        "project_name": "*",
        "panel": "main",
        "sample_type": "patient_care_sample",
        "is_identified": "deidentified",
        "default_snomed_term": "Disseminated malignancy of unknown primary"
      }
    ]
    :return:
    """
    ssm_client = get_boto3_ssm_client()

    return json.loads(
        ssm_client.get_parameter(
            Name=LIMS_PROJECT_NAME_MAPPING_SSM_PATH
        ).get("Parameter").get("Value")
    )


def apply_mapping_json_to_row(row: pd.Series, mapping_json: List):
    """
    Apply mapping json to row
      {
        ""
      }
    :param row:
    :param mapping_json:
    :return:
    """

    # Get mapping dict by project_name / project_owner
    try:
        mapping_dict = next(
            filter(
                lambda mapping_json_iter:
                (
                    mapping_json_iter.get("project_owner") == row['glims_project_owner'] and
                    mapping_json_iter.get("project_name") == row['glims_project_name']
                ) or
                (
                    mapping_json_iter.get("project_owner") == "*" and
                    mapping_json_iter.get("project_name") == row['glims_project_name']
                ) or
                (
                        mapping_json_iter.get("project_owner") == row['glims_project_owner'] and
                        mapping_json_iter.get("project_name") == "*"
                ),
                mapping_json
            )
        )
    except StopIteration:
        mapping_dict = next(
            filter(
                lambda mapping_json_iter:
                mapping_json_iter.get("project_owner") == "*" and
                mapping_json_iter.get("project_name") == "*",
                mapping_json
            )
        )

    # Determing column 'needs_redcap' by determining if default_snomed_term is set?
    if mapping_dict.get("default_snomed_term", None) is None:
        mapping_dict["needs_redcap"] = True
    else:
        mapping_dict["needs_redcap"] = False

    return mapping_dict


def get_cttso_samples_from_limsrow_df() -> pd.DataFrame:
    """
    Get cttso samples from GLIMS

    :return: A pandas DataFrame with the following columns
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
    """

    portal_base_url = get_portal_base_url()
    portal_url_endpoint = PORTAL_LIMSROW_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    # Initialise page and appended list
    all_results = []
    page_number = 1

    while True:
        req: Response = requests.get(
            url=portal_url_endpoint,
            auth=portal_auth,
            params={
                "type": PORTAL_CTTSO_SAMPLE_TYPE,
                "assay": PORTAL_CTTSO_SAMPLE_ASSAY,
                "phenotype": PORTAL_CTTSO_SAMPLE_PHENOTYPE,
                "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE,
                "ordering": PORTAL_WORKFLOW_ORDERING,
                "page": page_number
            }
        )

        req_dict: Dict = req.json()

        results: List
        if (results := req_dict.get("results", None)) is None:
            raise ValueError

        # Extend all results
        all_results.extend(results)

        # Get next page
        if req_dict.get("links", {}).get("next", None) is not None:
            page_number += 1
        else:
            break

    # Convret to dataframe
    portal_cttso_limsrow_df = pd.DataFrame(all_results)

    # Set column in_glims to true for all rows in this df
    portal_cttso_limsrow_df["in_glims"] = True

    # Rename project owner and name columns
    portal_cttso_limsrow_df = portal_cttso_limsrow_df.rename(
        columns={
            "project_owner": "glims_project_owner",
            "project_name": "glims_project_name",
        }
    )

    mapping_json: List = get_ssm_project_mapping_json()

    portal_cttso_limsrow_df["mapping_json"] = portal_cttso_limsrow_df.apply(
        lambda row: apply_mapping_json_to_row(row, mapping_json),
        axis="columns"
    )

    # Get glims rows based on project owner and project name
    columns_to_update = [
        "panel",
        "sample_type",
        "is_identified",
        "default_snomed_term",
        "needs_redcap"
    ]

    for columns_to_update in columns_to_update:
        portal_cttso_limsrow_df[f"glims_{columns_to_update}"] = portal_cttso_limsrow_df["mapping_json"].apply(
            lambda json_map: json_map.get(columns_to_update)
        )

    # Rename illumina_id to glims_illumina_id
    portal_cttso_limsrow_df.rename(
        columns={
            "illumina_id": "glims_illumina_id"
        },
        inplace=True
    )

    columns_to_return = [
        "subject_id",
        "library_id",
        "in_glims",
        "glims_illumina_id",
        "glims_project_owner",
        "glims_project_name",
        "glims_panel",
        "glims_sample_type",
        "glims_is_identified",
        "glims_default_snomed_term",
        "glims_needs_redcap"
    ]

    return portal_cttso_limsrow_df[columns_to_return]

