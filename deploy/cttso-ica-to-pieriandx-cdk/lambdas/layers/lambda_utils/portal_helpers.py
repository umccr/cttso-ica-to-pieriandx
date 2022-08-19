#!/usr/bin/env python3

import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from botocore.client import BaseClient
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from pyriandx.client import Client
import logging
import pandas as pd
from typing import Dict, Union, List, Tuple
import boto3
import json
import sys
from datetime import datetime
import pytz
from requests import Response
import requests
from urllib.parse import urlparse
from gspread_pandas.spread import Spread


from .globals import \
    PORTAL_API_BASE_URL_SSM_PATH, \
    PORTAL_METADATA_ENDPOINT, \
    PORTAL_WORKFLOWS_ENDPOINT, \
    PORTAL_WORKFLOW_ORDERING, \
    PORTAL_MAX_ROWS_PER_PAGE, \
    PORTAL_CTTSO_TYPE_NAME, \
    PORTAL_FIELDS, \
    WFR_NAME_REGEX

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
    portal_url_endpoint = PORTAL_METADATA_ENDPOINT.format(
        PORTAL_API_BASE_URL=portal_base_url
    )
    portal_auth = get_portal_creds(portal_url_endpoint)

    req: Response = requests.get(
        url=portal_url_endpoint,
        auth=portal_auth,
        params={
            "rowsPerPage": PORTAL_MAX_ROWS_PER_PAGE
        }
    )

    req_dict: Dict = req.json()

    results: List
    if (results := req_dict.get("results", None)) is None:
        raise ValueError

    return pd.DataFrame(results)


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

