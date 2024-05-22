#!/usr/bin/env python3

"""
All things PierianDx that are useful
"""

import os
import re
from datetime import datetime
from typing import Tuple, Dict, List, Union

from mypy_boto3_lambda import LambdaClient
from pyriandx.client import Client
import json
import pandas as pd
import time
import jwt
from jwt import DecodeError


from pyriandx.utils import retry_session

from .globals import \
    PIERIANDX_CDK_SSM_LIST, \
    PIERIANDX_CDK_SSM_PATH, \
    MAX_ATTEMPTS_GET_CASES, LIST_CASES_RETRY_TIME, \
    PanelType, SampleType, PIERIANDX_USER_AUTH_TOKEN_LAMBDA_PATH, JWT_EXPIRY_BUFFER

from .miscell import \
    change_case

from .aws_helpers import \
    SSMClient, get_boto3_ssm_client, \
    get_boto3_lambda_client

from .logger import get_logger


logger = get_logger()


def get_pieriandx_env_vars() -> Tuple:
    """
    Set cttso ica to pieriandx credentials
    PIERIANDX_USER_EMAIL -> From ssm parameter store
    PIERIANDX_INSTITUTION -> From ssm parameter store
    PIERIANDX_BASE_URL -> From ssm parameter store
    PIERIANDX_USER_AUTH_TOKEN -> From secrets manager
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

    # Set PIERIANDX_USER_AUTH_TOKEN based on secret
    if "PIERIANDX_USER_AUTH_TOKEN" in os.environ and jwt_is_valid(os.environ["PIERIANDX_USER_AUTH_TOKEN"]):
        # Already here!
        output_dict["PIERIANDX_USER_AUTH_TOKEN"] = os.environ["PIERIANDX_USER_AUTH_TOKEN"]
    else:
        # Get the secrets manager client
        lambda_client: LambdaClient = get_boto3_lambda_client()

        # Collect the auth token
        auth_token_resp = None
        while auth_token_resp is None or auth_token_resp == 'null' or json.loads(auth_token_resp).get("auth_token") is None:
            response = lambda_client.invoke(
                FunctionName=PIERIANDX_USER_AUTH_TOKEN_LAMBDA_PATH,
                InvocationType="RequestResponse"
            )
            auth_token_resp = response['Payload'].read().decode('utf-8')
            if auth_token_resp is None or auth_token_resp == 'null' or json.loads(auth_token_resp).get("auth_token") is None:
                logger.info("Could not get valid auth token from lambda, trying again in five seconds")
                time.sleep(5)

        output_dict["PIERIANDX_USER_AUTH_TOKEN"] = json.loads(auth_token_resp).get("auth_token")
        os.environ["PIERIANDX_USER_AUTH_TOKEN"] = output_dict["PIERIANDX_USER_AUTH_TOKEN"]

    return (
        output_dict.get("PIERIANDX_USER_EMAIL"),
        output_dict.get("PIERIANDX_USER_AUTH_TOKEN"),
        output_dict.get("PIERIANDX_INSTITUTION"),
        output_dict.get("PIERIANDX_BASE_URL")
    )


def get_pieriandx_client(email: str = os.environ.get("PIERIANDX_USER_EMAIL", None),
                         auth_token: str = os.environ.get("PIERIANDX_USER_AUTH_TOKEN", None),
                         institution: str = os.environ.get("PIERIANDX_INSTITUTION", None),
                         base_url: str = os.environ.get("PIERIANDX_BASE_URL", None)) -> Client:
    """
    Get the pieriandx client, validate environment variables
    PIERIANDX_BASE_URL
    PIERIANDX_INSTITUTION
    PIERIANDX_USER_EMAIL
    PIERIANDX_USER_AUTH_TOKEN
    :return:
    """

    missing_env_vars = False

    # Check inputs
    if email is None:
        logger.error(f"Please set the environment variable 'PIERIANDX_USER_EMAIL'")
        missing_env_vars = True
    if auth_token is None:
        logger.error(f"Please set the environment variable 'PIERIANDX_USER_AUTH_TOKEN'")
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
                  key=auth_token,
                  institution=institution,
                  base_url=base_url,
                  key_is_auth_token=True)


def get_pieriandx_df() -> pd.DataFrame:
    """
    Use pyriandx to collect the pyriandx dataframe

    :return: A pandas DataFrame with the following columns:
      * subject_id (first bit of case accession number)
      * library_id (second bit of case accession number)
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_creation_date  (as dt object)
      * pieriandx_assignee
    """
    email, auth_token, institution, base_url = get_pieriandx_env_vars()

    pyriandx_client = get_pieriandx_client(
        email=email,
        auth_token=auth_token,
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
        response: List = pyriandx_client._get_api(endpoint=f"/case")

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

    # Update column names
    columns_to_update = {
        "id": "pieriandx_case_id",
        "accession_number": "pieriandx_case_accession_number",
        "date_created": "pieriandx_case_creation_date"
    }

    if "assignee" in cases_df.columns.tolist():
        columns_to_update.update(
            {
                "assignee": "pieriandx_assignee"
            }
        )
    else:
        # Assign nulls to column
        cases_df["pieriandx_assignee"] = pd.NA

    cases_df = cases_df.rename(
        columns=columns_to_update
    )

    # Convert pieriandx assignee from list to last assignee
    # pieriandx assignee might not exist
    cases_df["pieriandx_assignee"] = cases_df["pieriandx_assignee"].apply(
        lambda x: x[-1] if isinstance(x, List) else pd.NA
    )

    # Convert case creation date to datetime object
    cases_df["pieriandx_case_creation_date"] = pd.to_datetime(cases_df["pieriandx_case_creation_date"])

    # Get subject id and library id
    cases_df["subject_id"] = cases_df.apply(
        lambda x: get_subject_id_from_accession_number(x.pieriandx_case_accession_number),
        axis="columns"
    )
    cases_df["library_id"] = cases_df.apply(
        lambda x: get_library_id_from_accession_number(x.pieriandx_case_accession_number),
        axis="columns"
    )

    cases_df = cases_df.query(
        "not ("
        "  subject_id.isnull() or "
        "  library_id.isnull() "
        ")",
        engine="python"
    )

    logger.info("Collected cases information and returning all accession numbers")
    columns_to_return = [
        "subject_id",
        "library_id",
        "pieriandx_case_id",
        "pieriandx_case_accession_number",
        "pieriandx_case_creation_date",
        "pieriandx_assignee"
    ]

    return cases_df[columns_to_return]


def get_subject_id_from_accession_number(accession_number: str) -> Union[str, None]:
    """
    Dont fail just return null
    :param accession_number:
    :return:
    """
    try:
        subject_id, library_id = split_subject_id_and_library_id_from_case_accession_number(accession_number)
        return subject_id
    except ValueError:
        return None


def get_library_id_from_accession_number(accession_number: str) -> Union[str, None]:
    """
    Dont fail just return null
    :param accession_number:
    :return:
    """
    try:
        subject_id, library_id = split_subject_id_and_library_id_from_case_accession_number(accession_number)
        return library_id
    except ValueError:
        return None


def get_existing_pieriandx_case_accession_numbers() -> List:
    """
    Get the list of pieriandx case accession numbers -
    since we don't want to try launch with an existing accession umber
    :return: [
      "SBJ12345_L12345_001",
      ...
    ]
    """

    cases_df = get_pieriandx_df()

    return cases_df["pieriandx_case_accession_number"].tolist()


def get_new_case_accession_number(subject_id: str, library_id: str) -> str:
    """
    Get a new case accession number
    :param subject_id:
    :param library_id:
    :return:
    """

    existing_case_accession_numbers: List = get_existing_pieriandx_case_accession_numbers()

    iter_int = 1
    while True:
        case_accession_number = f"{subject_id}_{library_id}_{str(iter_int).zfill(3)}"
        if case_accession_number not in existing_case_accession_numbers:
            break
        iter_int += 1

    return case_accession_number


def split_subject_id_and_library_id_from_case_accession_number(case_accession_number: str) -> Tuple[str, str]:
    """
    Get the subject id and library id from the case accession number
    :param case_accession_number:
    :return:
    """
    case_accession_number_regex_obj = re.fullmatch(r"^(SBJ\d+)_(L\d+)(?:_\d+)?$", case_accession_number)
    if case_accession_number_regex_obj is None:
        logger.debug(f"Could not split the subject id and library id from the case accession number '{case_accession_number}'")
        raise ValueError
    subject_id = case_accession_number_regex_obj.group(1)
    if subject_id is None:
        logger.debug(f"Could not collect the subject id from the case accession number '{case_accession_number}'")
        raise ValueError
    library_id = case_accession_number_regex_obj.group(2)
    if library_id is None:
        logger.debug(f"Could not collect the library id from the case accession number '{case_accession_number}'")
        raise ValueError
    return subject_id, library_id


def validate_case_accession_number(subject_id: str, library_id: str, case_accession_number: str) -> None:
    """
    Ensure the existing case accession number is valid
    :param library_id:
    :param subject_id:
    :param case_accession_number:
    :return:
    """
    # Get existing case numbers
    existing_case_accession_numbers: List = get_existing_pieriandx_case_accession_numbers()

    # Step 6.true.a - ensure it is of the syntax SBJID / LIB ID
    re_str: str = f"{subject_id}_{library_id}_" + r"\d{3}"
    if re.fullmatch(re_str, case_accession_number) is None:
        logger.error(f"Case accession number '{case_accession_number}' did not match regex '{re_str}'")
        raise ValueError

    # Step 6.true.b - ensure it does not match any other case accession numbers
    if case_accession_number in existing_case_accession_numbers:
        logger.error("Case already exists!")
        raise ValueError


def check_case_exists(case_id: str) -> bool:
    """
    Check a case actually exists and has not been deleted
    :param case_id:
    :return:
    """
    email, auth_token, institution, base_url = get_pieriandx_env_vars()

    pyriandx_client = get_pieriandx_client(
        email=email,
        auth_token=auth_token,
        institution=institution,
        base_url=base_url
    )

    # Go around _get_api to collect error code if it exists
    url = pyriandx_client.baseURL + f"/case/{case_id}"
    response = retry_session(pyriandx_client.headers).get(url, params=None)

    if response.status_code == 200:
        return True

    if response.status_code == 400:
        logger.info(f"Case {case_id} is not found, it may have been deleted")
        return False

    if response.status_code == 401:
        logger.error("Got unauthorized status code 401. Cannot continue with script")
        raise ChildProcessError

    logger.warning(f"Got status_code {response.status_code}. Assuming case does not exist")
    return False


def get_pieriandx_status_for_missing_sample(case_id: str) -> pd.Series:
    """
    Get pieriandx results for a sample with incomplete results
    :return: A pandas Series with the following columns:
      * subject_id (first bit of case accession number)
      * library_id (second bit of case accession number)
      * pieriandx_case_id
      * pieriandx_case_accession_number
      * pieriandx_case_identified
      * pieriandx_disease_code
      * pieriandx_disease_label
      * pieriandx_panel_type
      * pieriandx_workflow_id
      * pieriandx_workflow_status
      * pieriandx_report_status
      * pieriandx_report_signed_out - currently ignored
    """
    email, auth_token, institution, base_url = get_pieriandx_env_vars()

    pyriandx_client = get_pieriandx_client(
        email=email,
        auth_token=auth_token,
        institution=institution,
        base_url=base_url
    )
    iter_count = 0

    while True:
        # Add iter_count
        iter_count += 1
        if iter_count >= MAX_ATTEMPTS_GET_CASES:
            logger.error(f"Tried to get all cases {str(MAX_ATTEMPTS_GET_CASES)} times and failed")
            raise ChildProcessError

        # Attempt to get cases
        response: Dict = pyriandx_client._get_api(endpoint=f"/case/{case_id}")

        logger.debug("Printing response")
        if response is None:
            logger.warning(f"Trying again to get cases - attempt {iter_count}")
            time.sleep(LIST_CASES_RETRY_TIME)
        else:
            break

    # Initialise the case dict
    case_dict: Dict = {
            "subject_id": pd.NA,
            "library_id": pd.NA,
            "pieriandx_case_id": case_id,
            "pieriandx_case_accession_number": response.get("specimens")[0].get("accessionNumber"),
            "pieriandx_case_identified": response.get("identified", False),
            "pieriandx_disease_code": response.get("disease").get("code"),
            "pieriandx_disease_label": response.get("disease").get("label"),
            "pieriandx_panel_type": PanelType(response.get("panelName")).name,
            "pieriandx_sample_type": SampleType(response.get("sampleType")).name,
            "pieriandx_workflow_id": pd.NA,
            "pieriandx_workflow_status": pd.NA,
            "pieriandx_report_status": pd.NA
    }

    # Get subject id and library id
    subject_id, library_id = split_subject_id_and_library_id_from_case_accession_number(case_dict["pieriandx_case_accession_number"])
    case_dict.update({
        "subject_id": subject_id,
        "library_id": library_id
    })

    informatics_jobs_list: List = response.get("informaticsJobs", [])
    if len(informatics_jobs_list) == 0:
        logger.info(f"No informatics jobs available for case {case_id}")
    else:
        informatics_job: Dict = sorted(
            informatics_jobs_list,
            key=lambda x: int(x.get("id"))
        )[-1]
        case_dict["pieriandx_workflow_id"] = informatics_job["id"]
        case_dict["pieriandx_workflow_status"] = informatics_job["status"]

    reports_list: List = response.get("reports", [])
    if len(reports_list) == 0:
        logger.info(f"No reports available for case {case_id}")
    else:
        report: Dict = sorted(
            reports_list,
            key=lambda x: int(x.get("id"))
        )[-1]
        case_dict["pieriandx_report_status"] = report["status"]

    return pd.Series(case_dict)


def decode_jwt(jwt_string: str) -> Dict:
    return jwt.decode(
        jwt_string,
        algorithms=["HS256"],
        options={"verify_signature": False}
    )


def jwt_is_valid(jwt_string: str) -> bool:
    try:
        decode_jwt(jwt_string)
        timestamp_exp = decode_jwt(jwt_string).get("exp")

        # If timestamp will expire in less than one minute's time, return False
        if int(timestamp_exp) < (int(datetime.now().timestamp()) + JWT_EXPIRY_BUFFER):
            return False
        else:
            return True
    except DecodeError as e:
        return False
