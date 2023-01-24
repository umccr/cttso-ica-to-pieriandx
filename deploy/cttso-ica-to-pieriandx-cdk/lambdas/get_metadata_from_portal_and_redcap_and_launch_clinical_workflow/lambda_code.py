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
from dateutil.parser import parse as date_parser
from base64 import b64encode
from mypy_boto3_lambda.client import LambdaClient
import json
from typing import List, Union
import pandas as pd
from typing import Dict
import asyncio
import pytz
from concurrent.futures import ThreadPoolExecutor

from lambda_utils.arns import get_cttso_ica_to_pieriandx_lambda_function_arn
from lambda_utils.async_lambda_functions import async_get_metadata_information_from_redcap, \
    async_get_metadata_information_from_portal, async_get_ica_workflow_run_id_from_portal, \
    async_get_existing_pieriandx_case_accession_numbers
from lambda_utils.aws_helpers import get_boto3_lambda_client
from lambda_utils.globals import \
    CLINICAL_DEFAULTS, EXPECTED_ATTRIBUTES, \
    CURRENT_TIME
from lambda_utils.miscell import handle_date, datetime_obj_to_utc_isoformat

from lambda_utils.pieriandx_helpers import \
    validate_case_accession_number, get_new_case_accession_number
from lambda_utils.logger import get_logger

MAX_SIM_TASKS = 4  # Number of simultaneous tasks

# Set basic logger
logger = get_logger()


def merge_clinical_redcap_and_portal_data(redcap_df: pd.DataFrame, portal_df: pd.DataFrame) -> pd.DataFrame:
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
    :return: A pandas dataframe with the following columns
      * subject_id
      * library_id
      * disease_id
      * requesting_physicians_first_name
      * requesting_physicians_last_name
      * patient_urn
      * date_collected
      * date_received
      * mrn
      * sample_type
      * disease_name
      * gender
      * external_sample_id
      * external_subject_id
    """

    logger.info("Merging portal and redcap dataframes")

    # Merge over subject and library id
    merged_df: pd.DataFrame = pd.merge(
        redcap_df, portal_df,
        on=["subject_id", "library_id"]
    )

    # Assert only one row remains
    if not merged_df.shape[0] == 1:
        logger.error(f"Expected one row but got {merged_df.shape[0]}")
        raise ValueError

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
        "ica_workflow_run_id": "wfr.123abc",
        "allow_missing_redcap_entry": false,
        "panel_type": "main"
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

    # Steps 1, 2, 3, and 4 all done asynchronously
    logger.info("Completing metadata requests asynchronously")

    # Start event loop
    loop = asyncio.new_event_loop()

    # Step number of simultaneous executions
    loop.set_default_executor(ThreadPoolExecutor(max_workers=MAX_SIM_TASKS))

    # Start tasks

    # Step 1 - Get all pieriandx case accession numbers
    pieriandx_task = loop.create_task(
        async_get_existing_pieriandx_case_accession_numbers(),
    )

    # Step 2 - Get all required metadata information from redcap
    redcap_task = loop.create_task(
        async_get_metadata_information_from_redcap(
            subject_id=subject_id,
            library_id=library_id,
            allow_missing_data=event.get("allow_missing_redcap_entry", False)
        )
    )

    # Step 3 - Get all required metadata information from portal
    portal_task = loop.create_task(
        async_get_metadata_information_from_portal(
            subject_id=subject_id,
            library_id=library_id
        )
    )

    # Step 4 - check if ica workflow run id is defined
    ica_workflow_run_id: str
    if (ica_workflow_run_id := event.get("ica_workflow_run_id", None)) is None:
        # Step 3.false - grab workflow from portal
        get_ica_workflow_run_id_task = loop.create_task(
            async_get_ica_workflow_run_id_from_portal(
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
    redcap_df: Union[pd.DataFrame, None] = redcap_task.result()
    portal_df: pd.DataFrame = portal_task.result()
    if get_ica_workflow_run_id_task is not None:
        ica_workflow_run_id: str = get_ica_workflow_run_id_task.result()

    logger.info("Completed all asynchronous steps")

    # Step 5 - Merge redcap information with portal information
    logger.info("Merge redcap and portal metadata information")
    merged_df: pd.DataFrame = merge_clinical_redcap_and_portal_data(
        redcap_df=redcap_df,
        portal_df=portal_df
    )

    # Step 5a - check if pierian_metadata_complete value is set to 'complete' for this redcap dataframe
    if not event.get("allow_missing_redcap_entry", False):
        logger.info("Make sure pieriandx metadata is complete")
        merged_df = merged_df.query("pierian_metadata_complete=='Complete'")

    if (panel_type := event.get("panel_type", None)) is None:
        panel_type = CLINICAL_DEFAULTS["panel_type"].name.lower()

    # Check length
    if merged_df.shape[0] == 0:
        logger.error("PierianDx metadata was not 'Complete', exiting")
        raise ValueError

    # Step 6 - check if case accession number is defined
    logger.info("Ensure that the case accession value does not already exist in PierianDx")

    # Step 7 - Get Case accesison number
    if (case_accession_number := event.get("case_accession_number", None)) is not None:
        validate_case_accession_number(subject_id=subject_id,
                                       library_id=library_id,
                                       case_accession_number=case_accession_number)

    # Step 7 - Assign case accession number
    case_accession_number: str
    if (case_accession_number := event.get("case_accession_number", None)) is not None:
        # Step 6.true.a - ensure it is of the syntax SBJID / LIB ID
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

    # Set defaults
    merged_df["specimen_type"] = CLINICAL_DEFAULTS["specimen_type"]
    merged_df["is_identified"] = CLINICAL_DEFAULTS["is_identified"]
    merged_df["indication"] = CLINICAL_DEFAULTS["indication"]
    merged_df["hospital_number"] = CLINICAL_DEFAULTS["hospital_number"]
    merged_df["accession_number"] = case_accession_number
    merged_df["date_accessioned"] = CURRENT_TIME.astimezone(pytz.utc).replace(microsecond=0).isoformat()

    # Convert times to utc time
    for date_column in ["date_received", "date_collected"]:
        merged_df[date_column] = merged_df[date_column].apply(
            lambda x: datetime_obj_to_utc_isoformat(handle_date(x))
        )

    # Rename columns
    logger.info("Rename external subject and external sample columns")
    merged_df = merged_df.rename(
        columns={
            "external_sample_id": "external_specimen_id",
            "external_subject_id": "mrn"
        }
    )

    # Step 7 - assert expected values exist
    logger.info("Check we have all of the expected information")
    for expected_column in EXPECTED_ATTRIBUTES:
        if expected_column not in merged_df.columns.tolist():
            logger.error(
                f"Expected column {expected_column} but "
                f"did not find it in columns {', '.join(merged_df.columns.tolist())}"
            )
            raise ValueError

    # Step 7a - make up the 'identified' values (date_of_birth / first_name / last_name)
    merged_df["date_of_birth"] = datetime_obj_to_utc_isoformat(CLINICAL_DEFAULTS["date_of_birth"])
    merged_df["first_name"] = merged_df.apply(
        lambda x: CLINICAL_DEFAULTS["patient_name"][x.gender.lower()].split(" ")[0],
        axis="columns"
    )
    merged_df["last_name"] = merged_df.apply(
        lambda x: CLINICAL_DEFAULTS["patient_name"][x.gender.lower()].split(" ")[-1],
        axis="columns"
    )

    # Set panel type
    merged_df["panel_type"] = panel_type

    # Step 7 - Launch batch lambda function
    accession_json: Dict = merged_df.to_dict(orient="records")[0]

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
