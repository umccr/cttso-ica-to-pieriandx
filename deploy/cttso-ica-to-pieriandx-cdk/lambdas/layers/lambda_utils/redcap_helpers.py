#!/usr/bin/env python3

"""
Helpers for RedCap
"""

from typing import Dict
import pandas as pd
import json
from typing import List, Optional, Union
import sys
from requests import RequestException

import requests

from .globals import \
    REDCAP_PROJECT_NAME_PARAMETER_PATH, \
    REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER, \
    AUS_TIMEZONE, AUS_TIMEZONE_SUFFIX, \
    REDCAP_RAW_FIELDS_CLINICAL, REDCAP_LABEL_FIELDS_CLINICAL, \
    CLINICAL_DEFAULTS

from .aws_helpers import \
    SSMClient, get_boto3_ssm_client, \
    LambdaClient, get_boto3_lambda_client
from .logger import get_logger

logger = get_logger()


def get_redcap_project_name():
    """
    Get the name of the redcap project
    :return:
    """
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(
        Name=REDCAP_PROJECT_NAME_PARAMETER_PATH
    ).get("Parameter").get("Value")


def get_redcap_lambda_function_arn() -> str:
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(
        Name=REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER
    ).get("Parameter").get("Value")


def query_info_from_redcap(subject_id: str, library_id: str,
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
        raise RequestException

    response_body: List[Dict] = json.loads(response.get("body"))

    if len(response_body) == 0:
        logger.error(f"Could not pull the required information from redcap {response}")
        raise ValueError

    return pd.DataFrame(response_body)


def get_clinical_metadata_from_redcap_for_subject(subject_id: str, library_id: str, allow_missing_data: bool = False) -> pd.DataFrame:
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
    * Pierian Metadata Complete (Is the row complete?)
    :param subject_id:
    :param library_id:
    :param allow_missing_data:
    :return: A pandas DataFrame with the following columns:
      * requesting_physicians_first_name
      * requesting_physicians_last_name
      * subject_id
      * library_id
      * patient_urn
      * disease_id
      * disease_name
      * date_collected
      * time_collected
      * date_received
      * sample_type
      * gender
      * pierian_metadata_complete
    """

    # Get raw data from redcap
    logger.info("Starting async function 'Collecting information from redcap'")

    logger.info("Collecting raw dataframe from redcap")
    redcap_raw_df: pd.DataFrame = pd.DataFrame(columns=REDCAP_RAW_FIELDS_CLINICAL)

    try:
      redcap_raw_output_df: pd.DataFrame = query_info_from_redcap(subject_id=subject_id, library_id=library_id,
                                                                  fields=REDCAP_RAW_FIELDS_CLINICAL,
                                                                  raw_or_label="raw")
      redcap_raw_df = pd.concat([redcap_raw_df, redcap_raw_output_df])
    except ValueError:
        if not allow_missing_data:
            logger.error("Did not return any results from redcap query")
            raise ValueError

    logger.info(
        f"Returned {redcap_raw_df.shape[0]} rows and {redcap_raw_df.shape[1]} columns from raw dataframe redcap")
    logger.info(f"Collected the following columns from raw redcap: {', '.join(redcap_raw_df.columns.tolist())}")

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

    # Update time_collected field since it might not exist
    redcap_raw_df["time_collected"] = redcap_raw_df["time_collected"].apply(
        lambda x: x if not pd.isnull(x) else "00:00"
    )

    # Update date fields
    redcap_raw_df["date_collected"] = redcap_raw_df.apply(
        lambda x: x.date_collection + "T" + x.time_collected + f":00{AUS_TIMEZONE_SUFFIX}",
        axis="columns"
    )

    # Add time to 'date_receipt' string
    redcap_raw_df["date_received"] = redcap_raw_df.apply(
        lambda x: x.date_receipt + f"T00:00:00{AUS_TIMEZONE_SUFFIX}",
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
    # If data doesn't exist and missing data allowed, we fill in with defaults later on
    logger.info("Collecting label information from redcap")
    redcap_label_df: pd.DataFrame = pd.DataFrame(columns=REDCAP_LABEL_FIELDS_CLINICAL)
    try:
      redcap_label_output_df: pd.DataFrame = query_info_from_redcap(subject_id=subject_id, library_id=library_id,
                                                                    fields=REDCAP_LABEL_FIELDS_CLINICAL,
                                                                    raw_or_label="label")
      redcap_label_df = pd.concat([redcap_label_df, redcap_label_output_df])
    except ValueError:
        if not allow_missing_data:
            logger.error("Did not return any results from redcap query")
            raise ValueError
        logger.info("Could not retrieve the information for this sample, but missing data allowed")

    logger.info(
        f"Returned {redcap_label_df.shape[0]} rows and {redcap_label_df.shape[1]} columns from label dataframe in redcap")
    logger.info(f"Collected the following columns from label redcap: {', '.join(redcap_label_df.columns.tolist())}")

    redcap_label_df = redcap_label_df.rename(
        columns={
            "report_type": "sample_type",
            "patient_gender": "gender",
            "disease": "disease_name",
            "id_sbj": "subject_id",
            "libraryid": "library_id"
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

    if redcap_df.shape[0] == 0:
        if not allow_missing_data:
            logger.error("Did not return any results from redcap query")
            raise ValueError
        logger.info(f"No results found in redcap for subject_id='{subject_id}' / library_id='{library_id}'")
        logger.info(f"allow_missing_data parameter set to true however so we can populate with clinical defaults")
        redcap_df = pd.concat([
            redcap_df,
            pd.DataFrame([
                {
                    "subject_id": subject_id,
                    "library_id": library_id,
                    "requesting_physicians_first_name": CLINICAL_DEFAULTS["requesting_physicians_first_name"],
                    "requesting_physicians_last_name": CLINICAL_DEFAULTS["requesting_physicians_last_name"],
                    "patient_urn": CLINICAL_DEFAULTS["patient_urn"],
                    "disease_id": CLINICAL_DEFAULTS["disease_id"],
                    "disease_name": CLINICAL_DEFAULTS["disease_name"],
                    "date_collected": CLINICAL_DEFAULTS["date_collected"],
                    "time_collected": CLINICAL_DEFAULTS["time_collected"],
                    "date_received": CLINICAL_DEFAULTS["date_received"],
                    "sample_type": CLINICAL_DEFAULTS["sample_type"],
                    "gender": CLINICAL_DEFAULTS["gender"],
                    "pierian_metadata_complete": CLINICAL_DEFAULTS["pierian_metadata_complete"],
                }
            ])
        ])

    # Merge redcap information and then return
    num_entries: int
    if not (num_entries := redcap_df.shape[0]) == 1:
        logger.info(f"Expected dataframe to be of length 1, not {num_entries}")

    return redcap_df


def get_full_redcap_data_df() -> pd.DataFrame:
    """
    Returns the following columns from the RedCap Project
    [
      "record_id",
      "report_type",
      "id_sbj",
      "libraryid",
      "pierian_metadata_complete"
    ]
    :return: A pandas Dataframe with the following columns -
      * subject_id
      * library_id
      * redcap_sample_type
      * redcap_is_complete
    """

    lambda_client = get_boto3_lambda_client()

    redcap_dict: Dict = {
        "redcapProjectName": get_redcap_project_name(),
        "queryStringParameters": {
            "fields": [
                "record_id",
                "report_type",
                "id_sbj",
                "libraryid",
                "pierian_metadata_complete"
            ],
            "raw_or_label": "label"
        }
    }

    redcap_response = lambda_client.invoke(
        FunctionName=get_redcap_lambda_function_arn(),
        InvocationType="RequestResponse",
        Payload=json.dumps(redcap_dict)
    )

    if not redcap_response.get("StatusCode") == 200:
        logger.error(f"Error! StatusCode is {redcap_response.get('StatusCode')} not 200")
        raise ValueError

    payload: Dict = json.loads(redcap_response.get("Payload").read())

    # Check payload status code
    if not payload.get("statusCode") == 200:
        logger.error(f"Error! status code is {payload.get('statusCode')} not 200")
        raise ValueError

    response_body: List[Dict] = json.loads(payload.get("body"))

    if len(response_body) == 0:
        logger.error(f"Could not pull the required information from redcap {response_body}")
        raise ValueError

    # Import as dataframe
    redcap_df: pd.DataFrame = pd.DataFrame(response_body)

    # Rename
    redcap_df = redcap_df.rename(
        columns={
            "id_sbj": "subject_id",
            "libraryid": "library_id",
            "report_type": "redcap_sample_type",
            "pierian_metadata_complete": "redcap_is_complete"
        }
    )

    # Return
    return redcap_df


