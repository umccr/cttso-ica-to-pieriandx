#!/usr/bin/env python

"""
Get args through argparse
"""

import argparse
from argparse import ArgumentParser
from utils.errors import ArgumentError
from utils.ica_wes import get_ica_workflow_run_id_objs

from utils.accession import read_input_json, read_input_csv, sanitise_data_frame
from typing import List
import pandas as pd
from pathlib import Path

from utils.logging import get_logger
from utils.classes import Case, DeIdentifiedCase, IdentifiedCase, PierianDXSequenceRun
from utils.ica_wes import get_ica_workflow_run_objs_from_library_names

from libica.openapi.libwes import WorkflowRun

logger = get_logger()


def get_ica_to_pieriandx_args():
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

    parser.add_argument("--dryrun",
                        action="store_true",
                        default=False,
                        help="Don't actually submit / create or upload anything to PierianDx - "
                             "will still download data from ICA to tmpdir")

    return parser.parse_args()


def get_case_status_args():
    """
    Get the case status
    :param args:
    :return:
    """
    parser = ArgumentParser(description="""
Given a comma-separated list of case accession numbers or case accession ids, 
return a list of informatics jobs, the informatics job ids and the status of each.  
If both case ids and case accession numbers are provided, an outer-join is performed.
    """,
                            formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--case-ids",
                        required=False,
                        help="List of case ids")

    parser.add_argument("--case-accession-numbers",
                        required=False,
                        help="List of case accession numbers")

    parser.add_argument("--verbose",
                        action="store_true",
                        default=False,
                        help="Set logging level to DEBUG")

    return parser.parse_args()


def get_case_args(args):
    """
    Get the case status
    :param args:
    :return:
    """
    parser = ArgumentParser(description="""
Given a comma-separated list of case accession numbers or case accession ids, 
return a list of informatics jobs, the informatics job ids and the status of each.  
If both case ids and case accession numbers are provided, an outer-join is performed.
    """,
                            formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--case-ids",
                        required=False,
                        help="List of case ids")

    parser.add_argument("--case-accession-numbers",
                        required=False,
                        help="List of case accession numbers")

    return parser.parse_args()


def get_download_reports_args():
    """
    Get download reports
    :return:
    """
    parser = ArgumentParser(description="""
    Given a comma-separated list of case accession numbers or case accession ids, 
    download a list of reports to the zip file specified in --output-file 
    If both case ids and case accession numbers are provided, an outer-join is performed.
    Must specify one (and only one) of pdf and json. Parent directory of output file must exist.
        """,
                            formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--case-ids",
                        required=False,
                        help="List of case ids")

    parser.add_argument("--case-accession-numbers",
                        required=False,
                        help="List of case accession numbers")

    parser.add_argument("--output-file",
                        required=True,
                        help="Path to output zip file")

    parser.add_argument("--pdf",
                        default=False,
                        action='store_true',
                        help="Download reports as pdfs")

    parser.add_argument("--json",
                        default=False,
                        action='store_true',
                        help="Download reports as jsons")

    parser.add_argument("--verbose",
                        default=False,
                        action="store_true",
                        help="Set logging level to debug")

    return parser.parse_args()


def check_ica_to_pieriandx_args(args):
    """
    Read through inputs and assign objects based on input types
    :return:
    """

    # Confirm input.json or input.csv is defined
    if getattr(args, "accession_json", None) is not None and getattr(args, "accession_csv", None) is not None:
        logger.error("Please specify either --accession-json OR --accession-csv")
        raise ArgumentError
    elif getattr(args, "accession_json", None) is None and getattr(args, "accession_csv", None) is None:
        logger.error("Please specify either --accession-json OR --accession-csv")
        raise ArgumentError

    # Get
    if getattr(args, "accession_json", None) is not None:
        input_df: pd.DataFrame = read_input_json(Path(getattr(args, "accession_json"))).to_frame().transpose()
    else:
        input_df: pd.DataFrame = read_input_csv(Path(getattr(args, "accession_csv")))

    input_df = sanitise_data_frame(input_df)
    setattr(args, "input_df", input_df)

    # Get sample libraries
    setattr(args, "sample_libraries", get_sample_libraries_from_input_df(input_df))

    # Check ica workflow run id is defined
    if getattr(args, "ica_workflow_run_ids", None) is not None:
        ica_workflow_run_ids: List[str] = getattr(args, "ica_workflow_run_ids").split(",")
        ica_workflow_run_id_objs: List[WorkflowRun] = get_ica_workflow_run_id_objs(ica_workflow_run_ids)
    else:
        ica_workflow_run_id_objs: List[WorkflowRun] = get_ica_workflow_run_objs_from_library_names(args.sample_libraries)

    setattr(args, "ica_workflow_run_objs", ica_workflow_run_id_objs)

    # Read in case object
    setattr(args, "cases", get_cases_from_input_df(input_df))
    setattr(args, "runs", get_runs_from_input_df(input_df, args.cases))

    return args


def check_case_status_args(args):
    """
    Check the case status args
    :param args:
    :return:
    """
    case_id_args_str = getattr(args, "case_ids", None)
    case_accession_numbers_str = getattr(args, "case_accession_numbers", None)

    if case_id_args_str is None and case_accession_numbers_str is None:
        logger.error("Must specify one of --case-ids and --case-accesion-numbers")
        raise ArgumentError

    if case_id_args_str is not None:
        case_id_args_list = case_id_args_str.split(",")
    else:
        case_id_args_list = None
    if case_accession_numbers_str is not None:
        case_accession_numbers_list = case_accession_numbers_str.split(",")
    else:
        case_accession_numbers_list = None

    setattr(args, "case_ids_list", case_id_args_list)
    setattr(args, "case_accession_numbers_list", case_accession_numbers_list)

    return args


def check_download_reports_args(args):
    """
    Use the check-case args first, then make usre
    :return:
    """

    # Check status args
    args = check_case_status_args(args)

    # Check parent of output file is specified
    output_file_str = getattr(args, "output_file", None)
    output_file_path = Path(output_file_str).absolute().resolve()
    setattr(args, "output_file_path", output_file_path)

    if not output_file_path.name.endswith(".zip"):
        logger.error("--output-file must be a zip file")

    if not output_file_path.parent.is_dir():
        logger.error(f"Please create the parent directory to {output_file_str} before continuing")

    # Check output file type
    is_pdf = getattr(args, "pdf", None)
    is_json = getattr(args, "json", None)

    if not is_pdf and not is_json:
        logger.error("Please specify one of --pdf or --json")
        raise ArgumentError

    if is_pdf:
        setattr(args, "output_file_type", "pdf")
    else:
        setattr(args, "output_file_type", "json")

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
        if row.get("is_identified", False):
            cases.append(IdentifiedCase.from_dict(row.to_dict()))
        else:
            cases.append(DeIdentifiedCase.from_dict(row.to_dict()))

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
