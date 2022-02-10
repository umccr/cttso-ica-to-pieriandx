#!/usr/bin/env python3

"""
Given two csvs generate and launch aws payloads
"""

# Imports
import pandas as pd
import numpy as np
from pathlib import Path
from base64 import b64encode
import tzlocal  # pip install tzlocal
import pytz
import json
import boto3
import time
from datetime import datetime
from argparse import ArgumentParser
import logging

logging.basicConfig(level=logging.INFO)

# Globals
METADATA_PAYLOAD_COLUMNS = [
                             "sample_type",
                             "indication",
                             "disease_id",
                             "is_identified",
                             "requesting_physicians_first_name",
                             "requesting_physicians_last_name",
                             "accession_number",
                             "study_id",
                             "participant_id",
                             "specimen_type",
                             "external_specimen_id",
                             "date_accessioned",
                             "date_collected",
                             "date_received",
                             "gender",
                             "ethnicity",
                             "race"
                           ]
LAMBDA_FUNCTION_NAME = "ctTSOICAToPierianDx_batch_lambda"


def get_args() -> ArgumentParser():

    # Argument parser
    argument_parser = ArgumentParser(description="Launch bulk pieriandx uploader")

    # Add args
    argument_parser.add_argument("--ica-workflow-run-by-accession-number-csv",
                                 required=True,
                                 help="ICA Workflow run ID by workflow run accession number")

    argument_parser.add_argument("--pieriandx-metadata-csv",
                                 required=True,
                                 help="ICA Workflow run ID by workflow run accession number")

    argument_parser.add_argument("--output-csv",
                                 required=True,
                                 help="Place to put updated csv")

    return argument_parser.parse_args()


def check_args(args) -> None:
    """
    Check both csv files exist
    :param args:
    :return:
    """

    if not Path(args.ica_workflow_run_by_accession_number_csv).is_file():
        logging.error(f"Could not find file {args.ica_workflow_run_by_accession_number_csv}")
        raise FileNotFoundError

    if not Path(args.pieriandx_metadata_csv).is_file():
        logging.error(f"Could not find file {args.pieriandx_metadata_csv}")
        raise FileNotFoundError


def get_dataframe(args: ArgumentParser()) -> (pd.DataFrame, pd.DataFrame):
    """
    Get dataframes
    :param args:
    :return:
    """

    # Read csvs
    ica_mappings_df = pd.read_csv(args.ica_workflow_run_by_accession_number_csv, header=0)
    metadata_df = pd.read_csv(args.pieriandx_metadata_csv, header=0)
    metadata_df_og = metadata_df.copy()

    # Set timezone straight
    # Get Australia time (for date accessioned value)
    local_timezone = tzlocal.get_localzone()
    aware_dt = datetime.now(local_timezone)
    aus_mel_time = aware_dt.astimezone(pytz.timezone("Australia/Melbourne"))
    utc_time = aware_dt.astimezone(pytz.timezone("GMT"))
    aus_utc_offset = int(100 * aus_mel_time.utcoffset().seconds / 3600)
    utc_offset = int(100 * utc_time.utcoffset().seconds / 3600)

    # Drop accession numbers that are blank
    metadata_df = metadata_df.dropna(axis="rows", subset=["accession_number"])

    metadata_df["date_accessioned"] = metadata_df["date_accessioned"].replace('-', pd.NA)

    # Take only rows where date accessioned is null
    metadata_df = metadata_df[metadata_df["date_accessioned"].isnull()]

    # Set date accessioned
    metadata_df['date_accessioned_aus_time'] = aus_mel_time.strftime(f"%Y-%m-%dT%H:%M:%S+{aus_utc_offset}")
    metadata_df["date_accessioned"] = utc_time.strftime(f"%Y-%m-%dT%H:%M:%S+{utc_offset}")

    # Set 'tba' as NA and drop all rows containing tba
    metadata_df = metadata_df.replace('tba', pd.NA)

    metadata_df = metadata_df.dropna(axis="rows", subset=METADATA_PAYLOAD_COLUMNS)

    # Ensure disease_id and specimen_type are set to integers
    metadata_df["disease_id"] = metadata_df["disease_id"].astype(int)
    metadata_df["specimen_type"] = metadata_df["specimen_type"].astype(int)

    # Merge dataframes
    metadata_df = pd.merge(metadata_df, ica_mappings_df.dropna(), on="accession_number")

    return metadata_df_og, metadata_df


def get_base64_column(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get accession json base64 str
    :return:
    """
    # Get accession json object as base64 string
    metadata_df['accession_json_base64_str'] = metadata_df[METADATA_PAYLOAD_COLUMNS].\
        apply(lambda x: b64encode(bytes(x.to_json(), encoding='utf-8')).decode("ascii"),
              axis="columns")

    return metadata_df


def create_payload_column(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """

    :param metadata_df:
    :return:
    """
    # Create payload
    metadata_df['payload'] = metadata_df.apply(lambda x: json.dumps({
                                                   "parameters": {
                                                     "ica_workflow_run_id": x.ica_workflow_run_id,
                                                     "accession_json_base64_str": x.accession_json_base64_str
                                                   }
                                                 }).encode(),
                                               axis='columns')

    return metadata_df


def launch_lambda(payload, function_name):
    """
    Launch lambda through boto3
    :param payload:
    :param function_name:
    :return:
    """
    lambda_client = boto3.client('lambda')

    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=payload
    )

    logging.info(f"Response: {response}")

    if isinstance(response, dict):
        if 'ResponseMetadata' in response.keys() and 'RequestId' in response['ResponseMetadata'].keys():
            return response['ResponseMetadata']["RequestId"]


def main():
    """
    Given inputs of a workflow accession csv and a metadata csv launch the payloads
    :return:
    """

    # Set and check arguments
    args: ArgumentParser() = get_args()
    check_args(args)

    # Get dfs
    metadata_df_og, metadata_df = get_dataframe(args)

    # Create base64 and payload column
    metadata_df: pd.DataFrame = get_base64_column(metadata_df)
    metadata_df: pd.DataFrame = create_payload_column(metadata_df)

    # Create payloads
    response_ids = []
    payload: bytes
    for index, payload in enumerate(metadata_df['payload'].tolist()):
        logging.info("Launching payload: '{payload}")
        logging.info("Sleeping 10 seconds")
        time.sleep(10)
        response_ids.append(launch_lambda(payload, LAMBDA_FUNCTION_NAME))

    with open(args.output_csv, "w") as payloads_h:
        merged_df = pd.merge(metadata_df_og, metadata_df[["accession_number", "date_accessioned_aus_time", "payload"]],
                             how='left', on="accession_number")
        merged_df['response_id'] = response_ids
        merged_df.to_csv(payloads_h, index=False, header=True)


if __name__ == "__main__":
    main()
