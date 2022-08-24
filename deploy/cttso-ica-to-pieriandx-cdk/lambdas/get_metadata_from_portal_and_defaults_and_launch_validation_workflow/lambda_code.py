#!/usr/bin/env python

"""
Given a subject id and library id (and case accession number)
Complete the following steps
* Grab all metadata from validation globals required for cttso pipeline
* Grab all metadata from portal required for cttso pipeline - launches a portal
* Collect ICA workflow run ID from the cttso pipeline via portal if not provided
* Ensure that the case doesn't already exist in pieriandx
* Launch the lambda that triggers batch
"""

from dateutil.parser import parse as date_parser
from base64 import b64encode
from mypy_boto3_lambda.client import LambdaClient
import json
from typing import List
import pandas as pd
from typing import Dict
import pytz

from lambda_utils.arns import get_cttso_ica_to_pieriandx_lambda_function_arn
from lambda_utils.aws_helpers import get_boto3_lambda_client
from lambda_utils.globals import CLINICAL_DEFAULTS, VALIDATION_DEFAULTS, CURRENT_TIME, EXPECTED_ATTRIBUTES
from lambda_utils.miscell import handle_date, datetime_obj_to_utc_isoformat
from lambda_utils.pieriandx_helpers import \
    validate_case_accession_number, get_new_case_accession_number, get_existing_pieriandx_case_accession_numbers
from lambda_utils.logger import get_logger
from lambda_utils.portal_helpers import get_clinical_metadata_information_from_portal_for_subject

logger = get_logger()


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
        "ica_workflow_run_id": "wfr.123abc",
    }
    """

    # Step 0 - ensure subject id and library id can be found in the event, fail otherwise
    logger.info("Step 0: Ensure that subject id and library id can be found in the event")

    subject_id: str
    if (subject_id := event.get("subject_id", None)) is None:
        logger.error(f"Could not find subject id in event keys {list(event.keys())}")
        raise ValueError

    library_id: str
    if (library_id := event.get("library_id", None)) is None:
        logger.error(f"Could not find library id in event keys {list(event.keys())}")
        raise ValueError

    # Get ICA
    ica_workflow_run_id: str
    if (ica_workflow_run_id := event.get("ica_workflow_run_id", None)) is None:
        logger.error("Please provide the parameter in the payload 'ica_workflow_run_id'")
        raise ValueError

    # Collect portal dataframe
    sample_df: pd.DataFrame = get_clinical_metadata_information_from_portal_for_subject(subject_id, library_id)

    # Check length
    if sample_df.shape[0] == 0:
        logger.error(f"Could not get the subject / library information from the portal "
                     f"for subject id '{subject_id}' / library id '{library_id}'")
        raise ValueError

    # Update sample_df with valudation defaults
    sample_df["sample_type"] = VALIDATION_DEFAULTS["sample_type"]
    sample_df["indication"] = VALIDATION_DEFAULTS["indication"]
    sample_df["disease_id"] = VALIDATION_DEFAULTS["disease_id"]
    sample_df["disease_name"] = VALIDATION_DEFAULTS["disease_name"]
    sample_df["is_identified"] = VALIDATION_DEFAULTS["is_identified"]
    sample_df["requesting_physicians_first_name"] = VALIDATION_DEFAULTS["requesting_physicians_first_name"]
    sample_df["requesting_physicians_last_name"] = VALIDATION_DEFAULTS["requesting_physicians_last_name"]
    sample_df["first_name"] = VALIDATION_DEFAULTS["first_name"]
    sample_df["last_name"] = VALIDATION_DEFAULTS["last_name"]
    sample_df["date_of_birth"] = VALIDATION_DEFAULTS["date_of_birth"]
    sample_df["specimen_type"] = VALIDATION_DEFAULTS["specimen_type"]
    sample_df["date_accessioned"] = VALIDATION_DEFAULTS["date_accessioned"]
    sample_df["date_collected"] = VALIDATION_DEFAULTS["date_collected"]
    sample_df["date_received"] = VALIDATION_DEFAULTS["date_received"]
    sample_df["gender"] = VALIDATION_DEFAULTS["gender"]
    sample_df["ethnicity"] = VALIDATION_DEFAULTS["ethnicity"]
    sample_df["race"] = VALIDATION_DEFAULTS["race"]
    sample_df["hospital_number"] = VALIDATION_DEFAULTS["hospital_number"]

    # Get pieriandx case accession numbers
    pieriandx_case_accession_numbers: List = get_existing_pieriandx_case_accession_numbers()

    # Get Case accession number
    if (case_accession_number := event.get("case_accession_number", None)) is not None:
        validate_case_accession_number(subject_id=subject_id,
                                       library_id=library_id,
                                       case_accession_number=case_accession_number)

    # Assign case accession number
    case_accession_number: str
    if (case_accession_number := event.get("case_accession_number", None)) is not None:
        # Eensure it is of the syntax SBJID / LIB ID
        validate_case_accession_number(subject_id=subject_id,
                                       library_id=library_id,
                                       case_accession_number=case_accession_number)
        # Step 6.true.b - ensure it does not match any other case accession numbers
        if case_accession_number in pieriandx_case_accession_numbers:
            logger.error("Case already exists!")
            raise ValueError
    else:
        # Step 6.false - create case accession number that does not match any previous accession numbers
        # Get a case accession number that does not exist yet in the form SBJ_LIB_001
        case_accession_number = get_new_case_accession_number(subject_id, library_id)

    sample_df["accession_number"] = case_accession_number
    sample_df["date_accessioned"] = datetime_obj_to_utc_isoformat(CURRENT_TIME)

    # Convert times to utc time and strings
    for date_column in ["date_received", "date_collected", "date_of_birth"]:
        sample_df[date_column] = sample_df[date_column].apply(
            lambda x: datetime_obj_to_utc_isoformat(handle_date(x))
        )

    # Rename columns
    logger.info("Rename external subject and external sample columns")
    sample_df = sample_df.rename(
        columns={
            "external_sample_id": "external_specimen_id",
            "external_subject_id": "mrn"
        }
    )

    # Assert expected values exist
    logger.info("Check we have all of the expected information")
    for expected_column in EXPECTED_ATTRIBUTES:
        if expected_column not in sample_df.columns.tolist():
            logger.error(
                f"Expected column {expected_column} but "
                f"did not find it in columns {', '.join(sample_df.columns.tolist())}"
            )
            raise ValueError

    # Launch batch lambda function
    accession_json: Dict = sample_df.to_dict(orient="records")[0]

    # Initialise payload parameters
    logger.info("Converting accession json to a lambda payload")
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
    logger.info("Launch lambda client to invoke pieriandx")
    client_response = lambda_client.invoke(
        FunctionName=cttso_ica_to_pieriandx_lambda_arn,
        InvocationType="RequestResponse",
        Payload=payload
    )

    if not client_response.get("StatusCode") == 200:
        logger.error(f"Bad exit code when retrieving response from "
                     f"cttso-ica-to-pieriandx lambda client {client_response}")
        raise ValueError

    if "Payload" not in list(client_response.keys()):
        logger.error("Could not retrieve payload, submission to batch likely failed")
        logger.error(f"Client response was {client_response}")
        raise ValueError

    response_payload: Dict = json.loads(client_response.get("Payload").read())

    if response_payload is None or not isinstance(response_payload, Dict):
        logger.error("Could not get response payload as a dict")
        logger.error(f"Client response was {client_response}")
        logger.error(f"Payload was {response_payload}")
        raise ValueError

    logger.info("Successfully launched and returning pieriandx submission lambda")

    # Step 8 - Return case accession number and metadata information to user
    return response_payload