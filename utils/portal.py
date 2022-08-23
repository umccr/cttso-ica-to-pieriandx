#!/usr/bin/env python3

"""
Not a whole lot of functionality in it at the minute,
just used to collect the workflow run from the workflow id
"""

from utils.globals import RUN_INFO_XML_REGEX
from utils.logging import get_logger
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from urllib.parse import urlparse
from typing import Dict, Optional, List
import json
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_secretsmanager.client import SecretsManagerClient
import requests
from requests import Response
import boto3

PORTAL_WORKFLOWS_ENDPOINT = "https://{PORTAL_API_BASE_URL}/iam/workflows"
PORTAL_API_BASE_URL_SSM_PATH = "/data_portal/backend/api_domain_name"

logger = get_logger()


def get_boto3_session() -> boto3.Session:
    """
    Get a regular boto3 session
    :return:
    """
    return boto3.session.Session()


def get_boto3_ssm_client() -> SSMClient:
    return boto3.client("ssm")


def get_boto3_lambda_client() -> LambdaClient:
    return boto3.client("lambda")


def get_aws_region() -> str:
    """
    Get AWS region using boto3
    :return:
    """
    boto3_session = get_boto3_session()
    return boto3_session.region_name


def get_portal_base_url() -> str:
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(
        Name=PORTAL_API_BASE_URL_SSM_PATH
    ).get("Parameter").get("Value")


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


def get_workflow_obj_from_portal(portal_run_id: str) -> Dict:
    """
    Get the workflow object from the portal
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
            "portal_run_id": portal_run_id
        }
    )

    # Collect the json
    req_dict: Dict = req.json()

    results: List
    if (results := req_dict.get("results", None)) is None:
        logger.error(f"Did not get any results on {portal_url_endpoint}, "
                     f"portal_run_id {portal_run_id}")
        raise AttributeError

    if len(results) == 0:
        logger.error(f"Could not find the portal run id: {portal_run_id}")
        raise ValueError

    # Check length of results
    if not len(results) == 1:
        logger.error(f"Expected only one entry for portal run id: {portal_run_id}")
        raise ValueError

    result: Dict = results[0]

    # Return result
    return result


def get_workflow_inputs_from_portal_workflow_obj(workflow_object: Dict) -> Optional[Dict]:
    """
    Get workflow inputs from the workflow object
    :return:
    """
    return json.loads(workflow_object.get("input", None))


def collect_run_xml_from_workflow_inputs(workflow_inputs: Dict) -> Optional[Dict]:
    """
    Get the run_info_xml field from the inputs
    :return:
    """
    return workflow_inputs.get("run_info_xml", None)


def get_run_name_from_run_xml_location(run_info_path: str):
    """

    :param run_info_path:
    :return:
    """

    if ( runinfo_regex_match := RUN_INFO_XML_REGEX.fullmatch(run_info_path)) is None:
        logger.error(f"Could not get the run name from the run xml location '{run_info_path}'")

    # Return the only regex group
    return runinfo_regex_match.group(1)


def get_run_name_from_portal_run_id(portal_run_id: str) -> str:
    """
    Put it all together
    :param portal_run_id:
    :return:
    """

    # Query portal with portal run id
    try:
        workflow_object: Dict = get_workflow_obj_from_portal(portal_run_id)
    except AttributeError:
        logger.info("If we're running in dev, this is expected")
        return portal_run_id

    # Get inputs from object
    workflow_inputs: Dict = get_workflow_inputs_from_portal_workflow_obj(workflow_object)

    # Collect run xml values from workflow inputs
    run_xml_obj: Dict = collect_run_xml_from_workflow_inputs(workflow_inputs)

    # Get run name from location attribute of run xml object
    run_name: str = get_run_name_from_run_xml_location(run_xml_obj.get("location"))

    return run_name



