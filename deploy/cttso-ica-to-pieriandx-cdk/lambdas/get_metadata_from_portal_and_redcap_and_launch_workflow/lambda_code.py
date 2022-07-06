#!/usr/bin/env python3

"""
Given a subject id and library id (and case accession number)
Complete the following steps
* Grab all metadata from redcap required for cttso pipeline - launches a lambda
* Grab all metadata from portal required for cttso pipeline - launches a portal
* Collect ICA workflow run ID from the cttso pipeline via portal if not provided
* Ensure that the case doesn't already exist in pieriandx
* Launch the lambda that triggers batch
"""
import datetime
import requests
import sys
from base64 import b64encode
import boto3
from botocore.client import BaseClient
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from pyriandx.client import Client
import os
from requests import Response
import time
import json
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import logging
import re
from typing import List, Optional, Union, Tuple
import pandas as pd
from typing import Dict
from pathlib import Path
import asyncio
from urllib.parse import urlparse
import pytz

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)

PORTAL_API_BASE_URL_SSM_PATH = "/data_portal/backend/api_domain_name"
PORTAL_METADATA_ENDPOINT = "https://{PORTAL_API_BASE_URL}/iam/metadata/"
PORTAL_WORKFLOWS_ENDPOINT = "https://{PORTAL_API_BASE_URL}/workflows"
PORTAL_CTTSO_TYPE_NAME = "tso_ctdna_tumor_only"
PORTAL_MAX_ROWS_PER_PAGE = 1000  # FIXME - this breaks after 1000 workflow runs
PORTAL_WORKFLOW_ORDERING = "-start"  # We generally want the latest

REDCAP_RAW_FIELDS: List = [
    "record_id",
    "clinician_firstname",
    "clinician_lastname",
    "patient_urn",
    "disease",
    "date_collection",
    "time_collected",
    "date_receipt",
    "id_sbj",
    "libraryid"
]

REDCAP_LABEL_FIELDS: List = [
    "record_id",
    "report_type",
    "disease",
    "patient_gender",
    "id_sbj",
    "libraryid"
    "pierian_metadata_complete",
]

PORTAL_FIELDS: List = [
    "subject_id",
    "library_id",
    "external_sample_id",
    "external_subject_id"
]

REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER: str = "redcap-apis-lambda-function"
REDCAP_PROJECT_NAME_SSM_PARAMETER: str = "/cdk/cttso-ica-to-pieriandx/redcap_project_name"

PIERIANDX_CDK_SSM_PATH: Path = Path("/cdk") / "cttso-ica-to-pieriandx" / "env_vars"
PIERIANDX_CDK_SSM_LIST: List = [
    "PIERIANDX_USER_EMAIL",
    "PIERIANDX_INSTITUTION",
    "PIERIANDX_BASE_URL"
]

PIERIANDX_PASSWORD_SECRETS_PATH: Path = Path("PierianDx") / "UserPassword"
PIERIANDX_PASSWORD_SECRETS_KEY: str = "PierianDxUserPassword"

PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH = "cttso-ica-to-pieriandx-lambda-function"

EXPECTED_ATTRIBUTES = [
    "sample_type",
    "disease_id",
    "indication",
    "accession_number",
    "external_specimen_id",
    "date_accessioned",
    "date_collected",
    "date_received",
    "hospital_number",
    "gender",
    "mrn",
    "requesting_physicians_first_name",
    "requesting_physicians_last_name"
]

# "umccr__automated__tso_ctdna_tumor_only__SBJ00998__L2101500__202112115d8bdae7"
WFR_NAME_REGEX = re.compile(
    rf"umccr__automated__{PORTAL_CTTSO_TYPE_NAME}__(SBJ\d{{5}})__(L\d{{7}})__\S+"
)

MAX_ATTEMPTS_GET_CASES = 50
LIST_CASES_RETRY_TIME = 1

CURRENT_TIME = datetime.datetime.utcnow()
AUS_TIMEZONE = pytz.timezone("Australia/Melbourne")

HOSPITAL_NUMBER = 99


def change_case(column_name: str) -> str:
    """
    Change from Sample Type or SampleType to sample_type
    :param column_name:
    :return:
    """
    return ''.join(['_' + i.lower() if i.isupper()
                    else i for i in column_name]).lstrip('_'). \
        replace("(", "").replace(")", ""). \
        replace("/", "_per_")


def get_boto3_session() -> boto3.Session:
    """
    Get a regular boto3 session
    :return:
    """
    return boto3.session.Session()


def get_aws_region() -> str:
    """
    Get AWS region using boto3
    :return:
    """
    boto3_session = get_boto3_session()
    return boto3_session.region_name


def get_boto3_lambda_client() -> Union[LambdaClient, BaseClient]:
    return boto3.client("lambda")


def get_boto3_ssm_client() -> Union[SSMClient, BaseClient]:
    return boto3.client("ssm")


def get_boto3_secretsmanager_client() -> Union[SecretsManagerClient, BaseClient]:
    return boto3.client("secretsmanager")


def get_redcap_lambda_function_arn() -> str:
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(
        Name=REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER
    ).get("Parameter").get("Value")


def get_redcap_project_name() -> str:
    ssm_client: SSMClient = get_boto3_ssm_client()

    redcap_project_ssm_parameter_obj: Dict = ssm_client.get_parameter(Name=REDCAP_PROJECT_NAME_SSM_PARAMETER)
    redcap_project_name: str = redcap_project_ssm_parameter_obj.get("Parameter").get("Value")

    return redcap_project_name


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


def get_info_from_redcap(subject_id: str, library_id: str,
                         fields: Optional[List] = None,
                         raw_or_label=None) -> pd.DataFrame:
    """
    Get fields from redcap using the redcap lambda given the subject id and library id as identifiers
    :return:
    """

    lambda_client: LambdaClient = get_boto3_lambda_client()

    filter_logic = f"[id_sbj] = \"{subject_id}\" && [libraryid] = \"{library_id}\""

    lambda_dict: Dict = lambda_client.invoke(
        FunctionName=get_redcap_lambda_function_arn(),
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {
                "redcapProjectName": get_redcap_project_name(),
                "queryStringParameters": {
                    "filter_logic": filter_logic,
                    "fields": fields,
                    "raw_or_label": raw_or_label
                }
            }
        )
    )

    response: Dict = json.loads(lambda_dict.get("Payload").read())

    if not response.get("statusCode") == 200:
        logger.error(f"Bad exit code when retrieving redcap information {response}")
        sys.exit(1)

    response_body: List[Dict] = json.loads(response.get("body"))

    if len(response_body) == 0:
        logger.error(f"Could not pull the required information from redcap {response}")
        sys.exit(1)

    return pd.DataFrame(response_body)


def get_pieriandx_env_vars() -> Tuple:
    """
    Set cttso ica to pieriandx credentials
    PIERIANDX_USER_EMAIL -> From ssm parameter store
    PIERIANDX_INSTITUTION -> From ssm parameter store
    PIERIANDX_BASE_URL -> From ssm parameter store
    PIERIANDX_USER_PASSWORD -> From secrets manager
    """

    ssm_client: SSMClient = get_boto3_ssm_client()
    output_dict: Dict = {}

    # Set env values based on ssm values
    for env_var in PIERIANDX_CDK_SSM_LIST:
        # Check if its in the env first
        if env_var in os.environ:
            continue

        # Get the value from SSM
        ssm_parameter_obj: Dict = ssm_client.get_parameter(Name=str(PIERIANDX_CDK_SSM_PATH / env_var.lower()))

        # Check we got the parameter
        if ssm_parameter_obj is None or ssm_parameter_obj.get("Parameter") is None:
            print(f"Could not get parameter {str(PIERIANDX_CDK_SSM_PATH / env_var)}")
            exit()

        # Get the parameter dict
        parameter_dict: Dict
        if (parameter_dict := ssm_parameter_obj.get("Parameter")) is None:
            print(f"Could not get parameter {str(PIERIANDX_CDK_SSM_PATH / env_var)}")
            exit()

        # Make sure value is valid
        parameter_value: str
        if (parameter_value := parameter_dict.get("Value", None)) is None or len(parameter_value) == 0:
            print(f"Could not get parameter {str(PIERIANDX_CDK_SSM_PATH / env_var)}")
            exit()

        output_dict[env_var] = parameter_value

    # Set PIERIANDX_USER_PASSWORD based on secret
    if "PIERIANDX_USER_PASSWORD" in os.environ:
        # Already here!
        output_dict["PIERIANDX_USER_PASSWORD"] = os.environ["PIERIANDX_USER_PASSWORD"]
    else:
        # Get the secrets manager client
        secrets_manager_client: SecretsManagerClient = get_boto3_secretsmanager_client()
        response = secrets_manager_client.get_secret_value(
            SecretId=str(PIERIANDX_PASSWORD_SECRETS_PATH)
        )
        secrets_json = json.loads(response.get("SecretString"))
        if PIERIANDX_PASSWORD_SECRETS_KEY not in secrets_json.keys():
            logger.error(f"Could not find secrets key in {PIERIANDX_PASSWORD_SECRETS_PATH}")
            sys.exit(1)

        output_dict["PIERIANDX_USER_PASSWORD"] = secrets_json[PIERIANDX_PASSWORD_SECRETS_KEY]

    return (
        output_dict.get("PIERIANDX_USER_EMAIL"),
        output_dict.get("PIERIANDX_USER_PASSWORD"),
        output_dict.get("PIERIANDX_INSTITUTION"),
        output_dict.get("PIERIANDX_BASE_URL")
    )


def get_pieriandx_client(email: str = os.environ.get("PIERIANDX_USER_EMAIL", None),
                         password: str = os.environ.get("PIERIANDX_USER_PASSWORD", None),
                         institution: str = os.environ.get("PIERIANDX_INSTITUTION", None),
                         base_url: str = os.environ.get("PIERIANDX_BASE_URL", None)) -> Client:
    """
    Get the pieriandx client, validate environment variables
    PIERIANDX_BASE_URL
    PIERIANDX_INSTITUTION
    PIERIANDX_USER_EMAIL
    PIERIANDX_USER_PASSWORD
    :return:
    """

    missing_env_vars = False

    # Check inputs
    if email is None:
        logger.error(f"Please set the environment variable 'PIERIANDX_USER_EMAIL'")
        missing_env_vars = True
    if password is None:
        logger.error(f"Please set the environment variable 'PIERIANDX_USER_PASSWORD'")
        missing_env_vars = True
    if institution is None:
        logger.error(f"Please set the environment variable 'PIERIANDX_INSTITUTION'")
        missing_env_vars = True
    if base_url is None:
        logger.error(f"Please set the environment variable 'PIERIANDX_BASE_URL'")
        missing_env_vars = True

    if missing_env_vars:
        logger.error("Missing PIERIANDX environment variable")
        raise EnvironmentError

    # Return client object
    return Client(email=email,
                  key=password,
                  institution=institution,
                  base_url=base_url)


def get_cttso_ica_to_pieriandx_lambda_function_arn() -> str:
    """
    Get the parameter from the parameter name
    :return:
    """
    # Get lambda function arn
    ssm_client = get_boto3_ssm_client()
    cttso_ica_to_pieriandx_lambda_function_dict: Dict = ssm_client.get_parameter(
        Name=PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH
    )

    # Get the function dict
    cttso_ica_to_pieriandx_lambda_function_dict_parameter: Dict
    if (cttso_ica_to_pieriandx_lambda_function_dict_parameter := cttso_ica_to_pieriandx_lambda_function_dict.get(
            "Parameter", None)) is None:
        logger.error("Could not get Parameter key from ssm value ")
        sys.exit(1)
    cttso_ica_to_pieriandx_lambda_function_dict_parameter_value: str

    # Get the parameter value
    if (
            cttso_ica_to_pieriandx_lambda_function_dict_parameter_value :=
            cttso_ica_to_pieriandx_lambda_function_dict_parameter.get("Value", None)
       ) is None:
        logger.error("Could not get value key from ssm parameter")
        sys.exit(1)

    return cttso_ica_to_pieriandx_lambda_function_dict_parameter_value


async def get_existing_pieriandx_case_accession_numbers() -> List:
    """
    Get the list of pieriandx case accession numbers -
    since we don't want to try launch with an existing accession umber
    :return:
    """
    email, password, institution, base_url = get_pieriandx_env_vars()
    pyriandx_client = get_pieriandx_client(
        email=email,
        password=password,
        institution=institution,
        base_url=base_url
    )
    iter_count = 0

    while True:
        # Add iter_count
        iter_count += 1

        if iter_count >= MAX_ATTEMPTS_GET_CASES:
            logger.error(f"Tried to get all cases {str(MAX_ATTEMPTS_GET_CASES)} times and failed")
            raise EnvironmentError

        # Attempt to get cases
        response: Response = pyriandx_client._get_api(endpoint=f"/case")

        logger.debug("Printing response")
        if response is None:
            logger.warning(f"Trying again to get cases - attempt {iter_count}")
            time.sleep(LIST_CASES_RETRY_TIME)
        else:
            break

    cases_df = pd.DataFrame(response)

    sanitised_columns = [change_case(column_name)
                         for column_name in cases_df.columns.tolist()]

    cases_df.columns = sanitised_columns

    return cases_df["accession_number"].tolist()


async def get_metadata_information_from_redcap(subject_id: str, library_id: str) -> pd.DataFrame:
    """
    Get the following information from redcap
    * Clinician Name
    * Subject ID (to confirm with portal data)
    * Library ID (to confirm with portal data)
    * Patient URN (to confirm with portal data)
    * Disease (Both ID and code)
    * Date Collection (Collection date of specimen)
    * Time Collection (Collection time of specimen)
    * Date Received (Date Specimen was received)
    * Record Type (Is this a validation workflow or a Patient Sample?)
    * Gender (The gender of the patient)
    :param subject_id:
    :param library_id:
    :return:
    """

    # Get raw data from redcap
    redcap_raw_df: pd.DataFrame = get_info_from_redcap(subject_id=subject_id, library_id=library_id,
                                                       fields=REDCAP_RAW_FIELDS,
                                                       raw_or_label="raw")

    # Rename fields in redcap raw df (to prevent conflict with label df and to match accession json)
    redcap_raw_df = redcap_raw_df.rename(
        columns={
            "clinician_firstname": "requesting_physicians_first_name",
            "clinician_lastname": "requesting_physicians_last_name",
            "id_sbj": "subject_id",
            "libraryid": "library_id",
            "mrn": "patient_urn",
            "disease": "disease_id"
        }
    )

    # Update date fields
    redcap_raw_df["date_collected"] = redcap_raw_df.apply(
        lambda x: x.date_collection + "T" + x.time_collected + ":00+1000",
        axis="columns"
    )

    # Add time to 'date_receipt' string
    redcap_raw_df["date_received"] = redcap_raw_df.apply(
        lambda x: x.date_receipt + "T00:00:00+1000",
        axis="columns"
    )

    # Subset columns for redcap raw df
    redcap_raw_df = redcap_raw_df[
        [
            "disease_id",
            "requesting_physicians_first_name",
            "requesting_physicians_last_name",
            "subject_id",
            "library_id",
            "date_collected",
            "date_received",
            "patient_urn"
        ]
    ]

    # Get label data from redcap
    redcap_label_df: pd.DataFrame = get_info_from_redcap(subject_id=subject_id, library_id=library_id,
                                                         fields=REDCAP_LABEL_FIELDS,
                                                         raw_or_label="label")

    redcap_label_df = redcap_label_df.rename(
        columns={
            "report_type": "sample_type",
            "patient_gender": "gender",
            "disease": "disease_name",
            "id_sbj": "subject_id",
            "libraryid": "library_id",
        }
    )

    # Filter redcap label df
    redcap_label_df = redcap_label_df[
        [
            "sample_type",
            "disease_name",
            "gender",
            "subject_id",
            "library_id",
            "pierian_metadata_complete"
        ]
    ]

    # Merge redcap data
    redcap_df: pd.DataFrame = pd.merge(
        redcap_raw_df, redcap_label_df,
        on=["subject_id", "library_id"]
    )

    # Merge redcap information and then return
    num_entries: int
    if not (num_entries := redcap_df.shape[0]) == 1:
        logger.info(f"Expected dataframe to be of length 1, not {num_entries}")

    return redcap_df


async def get_metadata_information_from_portal(subject_id: str, library_id: str) -> pd.DataFrame:
    """
    Get the required information from the data portal
    * External Sample ID -> External Specimen ID
    * External Subject ID -> Patient URN
    :param subject_id:
    :param library_id:
    :return:
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

    return pd.DataFrame([result])[PORTAL_FIELDS]


async def get_ica_workflow_run_id_from_portal(subject_id: str, library_id: str) -> str:
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
        sys.exit(1)

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
        sys.exit(1)

    # Collect the workflow run id from the most recent run
    return cttso_workflows_df["wfr_id"].tolist()[0]


def merge_redcap_and_portal_data(redcap_df: pd.DataFrame, portal_df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine the values of the redcap dataframe and the portal dataframe.
    redcap_df contains the following columns:
      * disease_id
      * requesting_physicians_first_name
      * requesting_physicians_last_name
      * subject_id
      * patient_urn
      * date_collected
      * date_received
      * mrn
      * sample_type
      * disease_name
      * gender
    Whilst the portal dataframe contains the following columns:
      * subject_id
      * library_id
      * external_sample_id
      * external_subject_id
    :param redcap_df:
    :param portal_df:
    :return:
    """

    # Merge over subject and library id
    merged_df: pd.DataFrame = pd.merge(
        redcap_df, portal_df,
        on=["subject_id", "library_id"]
    )

    # Assert only one row remains
    if not merged_df.shape[0] == 1:
        logger.error(f"Expected one row but got {merged_df.shape[0]}")
        sys.exit(1)

    return merged_df


def lambda_handler(event, context):
    """
    Expect a subject and library id in the event at minimum,
    ica_workflow_run_id would also be good but not necessary
    case_accession_number also good but not necessary - will create one otherwise
    If case accession number is defined and the accession number already exists, handler will fail.

    Event:
    {
        "subject_id": "SBJ00006",
        "library_id": "L1234567",
        "case_accession_number": "SBJID_LIBID_123",
        "ica_workflow_run_id": "wfr.123abc"
    }
    """

    # Step 0 - ensure subject id and library id can be found in the event, fail otherwise
    subject_id: str
    if (subject_id := event.get("subject_id", None)) is None:
        logger.error(f"Could not find subject id in event keys {list(event.keys())}")
        raise ValueError

    library_id: str
    if (library_id := event.get("library_id", None)) is None:
        logger.error(f"Could not find library id in event keys {list(event.keys())}")
        raise ValueError

    # Steps 1, 2, 3, and 4 all done asynchronously

    # Step 1 - Get all pieriandx case accession numbers
    loop = asyncio.new_event_loop()

    pieriandx_task = loop.create_task(
        get_existing_pieriandx_case_accession_numbers(),

    )

    # Step 2 - Get all required metadata information from redcap
    redcap_task = loop.create_task(
        get_metadata_information_from_redcap(
            subject_id=subject_id,
            library_id=library_id)
    )

    # Step 3 - Get all required metadata information from portal
    portal_task = loop.create_task(
        get_metadata_information_from_portal(
            subject_id=subject_id,
            library_id=library_id
        )
    )

    # Step 4 - check if ica workflow run id is defined
    ica_workflow_run_id: str
    if (ica_workflow_run_id := event.get("ica_workflow_run_id", None)) is None:
        # Step 3.false - grab workflow from portal
        get_ica_workflow_run_id_task = loop.create_task(
            get_ica_workflow_run_id_from_portal(
                subject_id=subject_id,
                library_id=library_id
            )
        )
    else:
        get_ica_workflow_run_id_task = None

    # Wait for results to complete
    loop.run_until_complete(pieriandx_task)
    loop.run_until_complete(redcap_task)
    loop.run_until_complete(portal_task)

    if get_ica_workflow_run_id_task is not None:
        loop.run_until_complete(get_ica_workflow_run_id_task)

    # Retrieve values
    pieriandx_case_accession_numbers: List = pieriandx_task.result()
    redcap_df: pd.DataFrame = redcap_task.result()
    portal_df: pd.DataFrame = portal_task.result()
    if get_ica_workflow_run_id_task is not None:
        ica_workflow_run_id: str = get_ica_workflow_run_id_task.result()

    # Step 5 - Merge redcap information with portal information
    merged_df: pd.DataFrame = merge_redcap_and_portal_data(
        redcap_df=redcap_df,
        portal_df=portal_df
    )

    # Step 5a - check if pierian_metadata_complete value is set to 'complete' for this redcap dataframe
    merged_df = merged_df.query("pierian_metadata_complete=='Complete'")

    # Check length
    if merged_df.shape[0] == 0:
        logger.error("PierianDx metadata was not 'Complete', exiting")
        sys.exit(1)

    # Step 6 - check if case accession number is defined
    case_accession_number: str
    if (case_accession_number := event.get("case_accession_number", None)) is None:
        # Step 6.true.a - ensure it is of the syntax SBJID / LIB ID
        re_str: str = f"{subject_id}_{library_id}_" + r"\d{3}"
        if re.fullmatch(re_str, case_accession_number) is None:
            logger.error(f"Case accession number '{case_accession_number}' did not match regex '{re_str}'")
            raise ValueError
        # Step 6.true.b - ensure it does not match any other case accession numbers
        if case_accession_number in pieriandx_case_accession_numbers:
            logger.error("Case already exists!")
            raise ValueError
    else:
        # Step 6.false - create case accession number that does not match any previous accession numbers
        # Get a case accession number that does not exist yet in the form SBJ_LIB_000
        iter_int = 0
        while True:
            case_accession_number = f"{subject_id}_{library_id}_{str(iter_int).zfill(3)}"
            if case_accession_number not in pieriandx_case_accession_numbers:
                break
            iter_int += 1

    # Set defaults
    merged_df["indication"] = "NA"  # Set indication to NA
    merged_df["hospital_number"] = HOSPITAL_NUMBER
    merged_df["accession_number"] = case_accession_number
    merged_df["date_accessioned"] = str(CURRENT_TIME.astimezone(AUS_TIMEZONE).date())

    # Rename columns
    merged_df = merged_df.rename(
        columns={
            "external_sample_id": "external_specimen_id",
            "external_subject_id": "mrn"
        }
    )

    # Step 7 - assert expected values exist
    for expected_column in EXPECTED_ATTRIBUTES:
        if expected_column not in merged_df.columns.tolist():
            logger.error(
                f"Expected column {expected_column} but "
                f"did not find it in columns {', '.join(merged_df.columns.tolist())}"
            )
            raise ValueError

    # Step 7a - make up the 'identified' values (date_of_birth / first_name / last_name)
    merged_df["date_of_birth"] = str(CURRENT_TIME.astimezone(AUS_TIMEZONE).date())
    merged_df["first_name"] = merged_df.apply(
        lambda x: "John"
        if x.gender.lower() == "male"
        else
        "Jane",
        axis="columns"
    )
    merged_df["last_name"] = "Doe"

    # Step 7 - Launch batch lambda function
    accession_json: Dict = merged_df.to_dict(orient="records")[0]

    # Initialise payload parameters
    payload_parameters: Dict = {
        "accession_json_base64_str": b64encode(json.dumps(accession_json).encode("ascii")).decode("utf-8"),
        "ica_workflow_run_id": ica_workflow_run_id,
    }

    # Add verbose or dryrun parameters if set
    if event.get("dryrun", False):
        payload_parameters["dryrun"]: bool = True

    if event.get("verbose", False):
        payload_parameters["verbose"]: bool = True

    payload: bytes = json.dumps(
        {
            "parameters": payload_parameters,
        }
    ).encode("ascii")

    cttso_ica_to_pieriandx_lambda_arn: str = get_cttso_ica_to_pieriandx_lambda_function_arn()

    lambda_client: LambdaClient = get_boto3_lambda_client()

    # Get lambda
    client_response = lambda_client.invoke(
        FunctionName=cttso_ica_to_pieriandx_lambda_arn,
        InvocationType="RequestResponse",
        Payload=payload
    )

    response_payload: Dict = json.loads(client_response.get("Payload").read())

    if not response_payload.get("statusCode") == 200:
        logger.error(f"Bad exit code when retrieving response from "
                     f"cttso-ica-to-pieriandx lambda client {response_payload}")
        sys.exit(1)

    response_body: List[Dict] = json.loads(response_payload.get("body"))

    if len(response_body) == 0:
        logger.error(f"Could not pull the required information from redcap {response_payload}")
        sys.exit(1)

    # Step 8 - Return case accession number and metadata information to user
    return response_body
