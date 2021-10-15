#!/usr/bin/env python

"""
Get args through argparse
"""

import argparse
from argparse import ArgumentParser
from utils.errors import ArgumentError
from utils.ica_wes import get_ica_workflow_run_id_objs

from utils.accession import read_input_json, read_input_csv
from typing import List
import pandas as pd
from pathlib import Path

from utils.logging import get_logger

from utils.classes import Case, PierianDXSequenceRun
from utils.ica_wes import get_ica_workflow_run_objs_from_library_names

from libica.openapi.libwes import WorkflowRun

logger = get_logger()


def get_args():
    """
    Use the simple argument parse to return an argument object
    :return:
    """

    parser = ArgumentParser(description="""
Given a ica workflow run id, and an input.json file, 
pull the cttso files and upload to s3 for pieriandx, 
generate a case, run and informatics job. 
The following environment variables are expected: 
  * ICA_BASE_URL
  * ICA_ACCESS_TOKEN
  * PIERIANDX_BASE_URL
  * PIERIANDX_INSTITUTION
  * PIERIANDX_AWS_REGION
  * PIERIANDX_AWS_S3_PREFIX
  * PIERIANDX_AWS_ACCESS_KEY_ID
  * PIERIANDX_AWS_SECRET_ACCESS_KEY
  * PIERIANDX_USER_EMAIL
  * PIERIANDX_USER_PASSWORD
""",
                            formatter_class=argparse.RawTextHelpFormatter)

    # Get the ica workflow run id
    parser.add_argument("--ica-workflow-run-ids",
                        required=False,
                        help="List of ICA workflow run IDs (comma separated), if not specified, "
                             "script will look through the workflow run list for matching patterns")

    # Get the redcap inputs
    parser.add_argument("--accession-json",
                        required=False,
                        help="Path to accession json containing redcap information for sample list")

    parser.add_argument("--accession-csv",
                        required=False,
                        help="Path to accession csv containing redcap information for sample list")
    parser.add_argument("--verbose",
                        action='store_true',
                        default=False,
                        help="Set log level from info to debug")

    return parser.parse_args()


def check_args(args):
    """
    Read through inputs and assign objects based on input types
    :return:
    """

    # Confirm input.json or input.csv is defined
    if getattr(args,"accession_json", None) is not None and getattr(args,"accession_csv", None) is not None:
        logger.error("Please specify either --accession-json OR --accession-csv")
        raise ArgumentError
    elif getattr(args,"accession_json", None) is None and getattr(args,"accession_csv", None) is None:
        logger.error("Please specify either --accession-json OR --accession-csv")
        raise ArgumentError

    # Get
    if getattr(args, "accession_json", None) is not None:
        input_df: pd.DataFrame = read_input_json(Path(getattr(args, "accession_json")))
    else:
        input_df: pd.DataFrame = read_input_csv(Path(getattr(args, "accession_csv")))
    setattr(args, "input_df", input_df)

    # Get sample libraries
    setattr(args, "sample_libraries", get_sample_libraries_from_input_df(input_df))

    # Check ica workflow run id is defined
    if getattr(args,"ica-workflow-run-ids", None) is not None:
        ica_workflow_run_ids: List[str] = getattr(args,"ica-workflow-run-ids").split(",")
        ica_workflow_run_id_objs: List[WorkflowRun] = get_ica_workflow_run_id_objs(ica_workflow_run_ids)
    else:
        ica_workflow_run_id_objs: List[WorkflowRun] = get_ica_workflow_run_objs_from_library_names(args.sample_libraries)

    setattr(args, "ica_workflow_run_objs", ica_workflow_run_id_objs)

    # Read in case object
    setattr(args, "cases", get_cases_from_input_df(input_df))
    setattr(args, "runs", get_runs_from_input_df(input_df, args.cases))

    return args


def get_sample_libraries_from_input_df(input_df: pd.DataFrame) -> List[str]:
    """
    Obtain the case accession numbers from the input df
    :return:
    """
    return input_df["libraries"].tolist()


def get_cases_from_input_df(input_df: pd.DataFrame) -> List[Case]:
    """
    Get the case attributes
    :return:
    """

    cases: List[Case] = []

    for index, row in input_df.iterrows():
        # Create a case object from the row values in the input df
        cases.append(Case.from_dict(row.to_dict()))

    return cases


def get_runs_from_input_df(input_df: pd.DataFrame, cases: List[Case]) -> List[PierianDXSequenceRun]:
    """
    Create a list of PierianDx sequencing runs
    :return:
    """
    runs: List[PierianDXSequenceRun] = []

    for (index, row), case in zip(input_df.iterrows(), cases):
        # Create a run object from the row values in the input df
        runs.append(PierianDXSequenceRun(run_name=row.get("accession_number"),
                                         cases=[case]))

    return runs
