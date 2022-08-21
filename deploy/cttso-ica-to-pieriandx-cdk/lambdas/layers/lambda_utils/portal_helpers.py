#!/usr/bin/env python3

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
    PORTAL_CTTSO_TYPE_NAME, \
    PORTAL_FIELDS, \
    WFR_NAME_REGEX, PORTAL_SEQUENCE_RUNS_ENDPOINT

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

    req: Response = requests.get(
        url=portal_url_endpoint,
        auth=portal_auth,
        params={
            "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE,
        }
    )

    req_dict: Dict = req.json()

    results: List
    if (results := req_dict.get("results", None)) is None:
        raise ValueError

    portal_cttso_sequence_runs_df = pd.DataFrame(results)

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
      * portal_is_failed_run         -> Did the sequence run assosciated with the fastq inputs of this workflow pass or fail
    """

    portal_base_url = get_portal_base_url()
    portal_url_endpoint = PORTAL_WORKFLOWS_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    req: Response = requests.get(
        url=portal_url_endpoint,
        auth=portal_auth,
        params={
            "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE,
            "type_name": PORTAL_CTTSO_TYPE_NAME,
            "ordering": PORTAL_WORKFLOW_ORDERING,
        }
    )

    req_dict: Dict = req.json()

    results: List
    if (results := req_dict.get("results", None)) is None:
        raise ValueError

    # Convret to dataframe
    portal_cttso_workflow_runs_df = pd.DataFrame(results)

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

    mini_dfs: List[pd.DataFrame] = []
    for (subject_id, library_id), mini_df in portal_cttso_workflow_runs_df.groupby(["subject_id", "library_id"]):
        mini_df = mini_df.drop_duplicates()
        if mini_df.shape[0] == 1:
            mini_dfs.append(mini_df)
            continue
        pd.set_option('display.max_columns', 500)
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

    req: Response = requests.get(
        url=portal_url_endpoint,
        auth=portal_auth,
        params={
            "type_name": PORTAL_CTTSO_TYPE_NAME,
            "end_status": "Succeeded",
            "ordering": PORTAL_WORKFLOW_ORDERING,
            "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE
        }
    )

    # Collect the json
    json_dict: Dict = req.json()

    # Ensure requests
    results: List
    if (results := json_dict.get("results", None)) is None:
        logger.error("Could not get requests from portal workflow endpoint")
        raise ValueError

    # Collect data frames
    cttso_workflows_df: pd.DataFrame = pd.DataFrame(results)

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

