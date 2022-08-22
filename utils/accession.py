#!/usr/bin/env python

"""
Read in the accession csv or json
"""

from typing import List, Dict, Optional
from pathlib import Path
import pandas as pd
import gzip
import requests
import json
import time
import re
from requests import Response

from utils.logging import get_logger
from dateutil.parser import parse as date_parser
from datetime import datetime, timezone
from utils.globals import MANDATORY_INPUT_COLUMNS, OPTIONAL_DEFAULTS, ACCESSION_FORMAT_REGEX, OUTPUT_STATS_FILE, \
    MAX_ATTEMPTS_GET_CASES, LIST_CASES_RETRY_TIME, \
    MANDATORY_INPUT_COLUMNS_FOR_DEIDENTIFIED_SAMPLES, MANDATORY_INPUT_COLUMNS_FOR_IDENTIFIED_SAMPLES

from utils.micro_classes import Disease, SpecimenType
from utils.classes import Case
from utils.enums import SampleType, Ethnicity, Gender, Race
from utils.pieriandx_helper import get_pieriandx_client
from utils.errors import CaseNotFoundError, ListCasesError

import pytz

logger = get_logger()


def log_informatics_job_by_case(cases: List[Case]):
    """
    For each job generated, crase
    :return:
    """
    cases_dict = [
        {
            "case_accession_number": case.case_accession_number,
            "case_id": case.case_id,
            "case_informatics_job": case.informatics_job_id,
            "case_run_id": case.run_objs[0].run_id
        }
        for case in cases
    ]

    pd.DataFrame(cases_dict).to_csv(OUTPUT_STATS_FILE, index=False, header=True, line_terminator="\n")


def read_input_json(input_json: Path) -> pd.Series:
    with open(input_json, "r") as json_h:
        return pd.Series(json.load(json_h))


def get_cases_df() -> pd.DataFrame:
    """
    Return all cases in a dataframe with the following attributes:
    * id
    * accession_number
    * date_created
    * assignee
    :return:
    """

    pyriandx_client = get_pieriandx_client()

    logger.debug(f"Listing all cases")

    iter_count = 0

    while True:
        # Add iter_count
        iter_count += 1

        if iter_count >= MAX_ATTEMPTS_GET_CASES:
            logger.error(f"Tried to get all cases {str(MAX_ATTEMPTS_GET_CASES)} times and failed")
            raise ListCasesError

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

    return cases_df


def filter_cases_df(cases_df: pd.DataFrame, case_ids: List = None, case_accessions: List = None, merge_type: str = "inner") -> pd.DataFrame:
    """
    Given a list of cases merge case ids and case accessions
    :param cases_df: 
    :param case_ids: 
    :param case_accessions:
    :param merge_type: 
    :return: 
    """

    # Check one has been specified
    if case_ids is None and case_accessions is None:
        logger.error("Please specify one of case_ids or case_accessions")
        raise ValueError

    # Check the merge type
    if merge_type not in ["inner", "outer"]:
        logger.error("Only 'inner' and 'outer' joins are supported for filtering the cases df by id and accession number")
        raise ValueError

    # Check the single use cases
    if case_ids is not None and case_accessions is None:
        return cases_df.query("id in @case_ids")
    elif case_ids is None and case_accessions is not None:
        return cases_df.query("accession_number in @case_accessions")

    # Check the duplicate use cases
    if merge_type == "inner":
        # Need an ampersand between the two queries
        return cases_df.query("id in @case_ids & accession_number in @case_accessions")
    elif merge_type == "outer":
        return cases_df.query("id in @case_ids | accession_number in @case_accessions")


def get_cases_df_from_params(case_ids: List = None, case_accessions: List = None, merge_type: str = "inner") -> pd.DataFrame:
    """
    Get all cases, then filter by parameters
    :param case_ids:
    :param case_accessions:
    :param merge_type:
    :return:
    """
    return filter_cases_df(get_cases_df(), case_ids=case_ids, case_accessions=case_accessions, merge_type=merge_type)


def get_case(case_id: str) -> Dict:
    pyriandx_client = get_pieriandx_client()

    logger.debug(f"Getting case object case {case_id}")
    iter_count = 0

    while True:
        # Add iter_count
        iter_count += 1

        if iter_count >= MAX_ATTEMPTS_GET_CASES:
            logger.error(f"Tried to get all cases {str(MAX_ATTEMPTS_GET_CASES)} times and failed")
            raise ListCasesError

        # Attempt to get cases
        response: Optional[Dict] = pyriandx_client._get_api(endpoint=f"/case/{case_id}")

        logger.debug("Printing response")
        if response is None:
            logger.warning(f"Trying again to get cases - attempt {iter_count}")
            time.sleep(LIST_CASES_RETRY_TIME)
        else:
            break

    return response


def get_report_ids_by_case_id(case_id) -> Optional[List[Dict]]:
    """
    :param case_id:
    :return:
    """
    logger.debug(f"Getting the reports for the case {case_id}")
    case_obj: Dict = get_case(case_id)

    if "reports" not in case_obj.keys():
        return None

    report_list = []
    for report_item in case_obj.get("reports"):
        report_list.append({
            "case_id": case_id,
            "report_id": report_item.get("id"),
            "report_status": report_item.get("status"),
        })

    return report_list


def get_informatics_status_by_case_id(case_id: str) -> Optional[List[Dict]]:
    """
    Get the informatics status by the case id
    :return:
    """
    iter_count = 0
    while iter_count < 10:
        case_obj = get_case(case_id)
        if case_obj is None:
            logger.warning(f"Could not get case object for case {case_id}, retrying")
            iter_count += 1
            time.sleep(5)
            continue

        break
    else:
        logger.error("Tried 10 times to download the case files and failed!")
        raise CaseNotFoundError

    # Make sure informatics job is in the response keys
    if 'informaticsJobs' not in case_obj.keys():
        return None

    job_list = []
    for job_item in case_obj.get("informaticsJobs"):
        job_list.append({
            "case_id": case_id,
            "job_id": job_item.get("id"),
            "job_status": job_item.get("status"),
            "job_date_started": date_parser(job_item.get("dateStarted")),
            "job_date_ended": date_parser(job_item.get("dateEnded"))
        })

    return job_list


def get_reports_by_case_id(case_id: str) -> Optional[List[Dict]]:
    """
    Get list of reports from a given case id
    :param case_id:
    :return:
    """
    iter_count = 0
    while iter_count < 10:
        case_obj = get_case(case_id)
        if case_obj is None:
            logger.warning(f"Could not get case object for case {case_id}, retrying")
            iter_count += 1
            time.sleep(5)
            continue

        break
    else:
        logger.error("Tried 10 times to download the case files and failed!")
        raise CaseNotFoundError

    # Make sure reports is in the response keys
    if 'reports' not in case_obj.keys():
        return None

    job_list = []
    for report_item in case_obj.get("reports"):
        job_list.append({
            "id": report_item.get("id"),
            "signed_out": report_item.get("signedOut"),
            "status": date_parser(report_item.get("status"))
        })

    return job_list


def download_report(case_id: str, report_id: str, output_file_type: str, output_file_path: Path):
    """
    Download the report to an output file path
    :param case_id:
    :param report_id:
    :param output_file_path:
    :return:
    """
    pyriandx_client = get_pieriandx_client()

    logger.debug(f"Getting case object case {case_id}")

    response = requests.get(url=pyriandx_client.baseURL + f"/case/{case_id}/reports/{report_id}",
                            stream=True,
                            headers=pyriandx_client.headers,
                            params=[("format", output_file_type)])

    logger.debug(f"Writing report to {output_file_path}")
    with open(output_file_path, "wb") as report_output_h:
        report_output_h.write(gzip.decompress(response.content))

    # In case this script is run in a for loop
    time.sleep(3)


def change_case(column_name: str) -> str:
    """
    Change from Sample Type or SampleType to sample_type
    :param column_name:
    :return:
    """
    return ''.join(['_' + i.lower() if i.isupper()
                    else i for i in column_name]).lstrip('_').\
        replace("(", "").replace(")", "").\
        replace("/", "_per_")


def read_input_csv(input_csv: Path) -> pd.DataFrame:
    """
    Read in the input csv,
    Sanitise inputs,
    Added disease and specimen dicts
    Add non-mandatory fields with blank values if necessary
    :param input_csv:
    :return:
    """

    # Check file exists
    if not input_csv.is_file():
        logger.error(f"Could not find file {input_csv}")
        raise FileNotFoundError

    # Check file ends with csv.
    if not input_csv.name.endswith(".csv"):
        logger.warning(f"Reading {input_csv} despite not having csv format")

    # Read csv
    logger.debug(f"Reading {input_csv}")
    return pd.read_csv(input_csv, header=0, comment="#")


def datetime_obj_to_utc(datetime_obj: datetime) -> datetime:
    if datetime_obj.tzinfo is None:
        # Assume utc time and just append
        datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)
    else:
        datetime_obj.astimezone(pytz.utc)

    return datetime_obj.replace(microsecond=0)


def sanitise_data_frame(input_df: pd.DataFrame) -> pd.DataFrame:
    # Copy dataframe and convert blanks to nas
    input_df = input_df.copy().replace("", pd.NA)

    # Drop all columns with blanks
    input_df.dropna(axis="columns", how="all")

    # Sanitise inputs
    logger.debug("Sanitising input csv names")
    orig_columns = input_df.columns.tolist()
    sanitised_columns = [change_case(column_name.replace(" ", "")).replace("_i_d", "_id").replace("t_m_b", "tmb")
                         for column_name in orig_columns]
    input_df.columns = sanitised_columns

    # Rename date collected column
    if 'datecollected' in list(input_df.columns):
        input_df.rename(columns={
            "datecollected": "date_collected"
        }, inplace=True)

    logger.info("Confirming all samples have accession number in correct format. "
                "Splitting case accession number into subject and library ids")
    subjects = []
    libraries = []
    for index, row in input_df.iterrows():
        case_accession_number = row.get("accession_number", None)
        if case_accession_number is None or pd.isna(case_accession_number):
            logger.error(f"Could not retrieve the accession number for row {index}")
            raise AttributeError

        if ACCESSION_FORMAT_REGEX.match(case_accession_number) is None:
            logger.error(f"Could not match the accession number to the regex SBJ\\d+_L\\d+")
            raise AttributeError

        # Add subject and cases to library
        subjects.append(ACCESSION_FORMAT_REGEX.match(case_accession_number).group(1))
        libraries.append(ACCESSION_FORMAT_REGEX.match(case_accession_number).group(2))

    # Add in the subject ID and library ID
    input_df["subject_id"] = subjects
    input_df["libraries"] = libraries

    # Validating disease and specimen type
    logger.info("Confirming all samples have correct disease and specimen attribute")
    disease_objs: List[Disease] = []
    specimen_type_objs: List[SpecimenType] = []

    for index, row in input_df.iterrows():
        # Get sample name
        sample_name = row.get('accession_number', None)

        # Check disease id
        disease_id = None
        disease_name = None
        # Get disease id
        if row.get("disease", None) is not None:
            disease_id = row.get("disease")
        elif row.get("disease_id", None) is not None:
            disease_id = row.get("disease_id")
        # Get disease name
        if row.get("disease_name", None) is not None:
            disease_name = row.get("disease_name")

        disease_objs.append(Disease(code=disease_id,
                                    label=disease_name))

        # Now check the specimens
        specimen_type_id = None
        specimen_type_name = None
        # Get specimen_type id
        if row.get("specimen_type", None) is not None:
            specimen_type_id = row.get("specimen_type")
        # Get specimen_type name
        if row.get("specimen_type_name", None) is not None:
            specimen_type_name = row.get("specimen_type_name")

        # Sanity check at least one of the columns is defined
        if specimen_type_id is None and specimen_type_name is None:
            logger.error(f"Could not get specimen_type or specimen_type name for sample {sample_name}")
            raise AttributeError

        # Append the specimen_type series
        specimen_type_objs.append(SpecimenType(code=specimen_type_id, label=specimen_type_name))

    # Update dicts for df
    input_df["disease_obj"] = disease_objs
    input_df["specimen_type_obj"] = specimen_type_objs

    # Confirm mandatory inputs
    missing_columns = list(set(MANDATORY_INPUT_COLUMNS) - set(list(input_df.dropna(axis="columns", how="all").columns)))
    if not len(missing_columns) == 0:
        logger.error(f"Missing inputs in the following mandatory columns: {', '.join(missing_columns)}")
        raise ValueError

    # Add defaults to non-mandatory fields that have them
    for key, value in OPTIONAL_DEFAULTS.items():
        if key not in list(input_df.columns):
            input_df[key] = value
        else:
            input_df[key].fillna(value, inplace=True)

    # Coerce to 'lower' for our enum types
    # Coerce patient care sample to patientcare
    input_df["sample_type"] = input_df["sample_type"].apply(lambda x:
                                                            SampleType(
                                                                re.sub(r"sample$", "", x.lower().replace(" ", ""))
                                                            ))
    input_df["ethnicity"] = input_df["ethnicity"].apply(lambda x: Ethnicity(x.lower()))
    input_df["race"] = input_df["race"].apply(lambda x: Race(x.lower()))
    input_df["gender"] = input_df["gender"].apply(lambda x: Gender(x.lower()))

    # Check if identified column set, if not set, set to false
    input_df["is_identified"] = input_df.apply(lambda x: x.is_identified
                                                         if hasattr(x, "is_identified")
                                                         else False,
                                               axis="columns")

    # Check if any deidentified samples and make sure all the columns are there for them
    if input_df.query("is_identified==False").shape[0] > 0:
        missing_columns = list(set(MANDATORY_INPUT_COLUMNS_FOR_DEIDENTIFIED_SAMPLES) -
                               set(list(input_df.query("is_identified==False").dropna(axis="columns", how="all").columns)))
        if not len(missing_columns) == 0:
            logger.error(f"Missing inputs in the following mandatory columns: {', '.join(missing_columns)}")
            raise ValueError

    # Check if any identified samples and make sure all the columns are there for them
    if input_df.query("is_identified==True").shape[0] > 0:
        missing_columns = list(set(MANDATORY_INPUT_COLUMNS_FOR_IDENTIFIED_SAMPLES) -
                               set(list(input_df.query("is_identified==True").dropna(axis="columns", how="all").columns)))
        if not len(missing_columns) == 0:
            logger.error(f"Missing inputs in the following mandatory columns: {', '.join(missing_columns)}")
            raise ValueError

    # De-Identified columns only
    # Map 'study_id' to 'study_identifier' but check identified status first
    input_df["study_identifier"] = input_df.apply(
        lambda x: x.study_id
        if hasattr(x, "study_id")
        else pd.NA,
        axis="columns")

    # Map 'participant_id' to 'study_subject_identifier'
    input_df["study_subject_identifier"] = input_df.apply(
        lambda x: x.participant_id
        if hasattr(x, "participant_id")
        else pd.NA,
        axis="columns")

    # Set defaults for study identifier and study subject identifier
    input_df["study_identifier"] = input_df.apply(
        lambda x: x.study_identifier
        if hasattr(x, "study_identifier")
           and not pd.isna(x.study_identifier)
        else x.sample_type.value,
        axis="columns")

    input_df["study_subject_identifier"] = input_df.apply(
        lambda x: x.study_subject_identifier
        if hasattr(x, "study_subject_identifier")
           and not pd.isna(x.study_subject_identifier)
        else x.accession_number,
        axis="columns")

    # Identified columns only
    input_df["date_of_birth"] = input_df.apply(
        lambda x: x.date_of_birth
        if hasattr(x, "date_of_birth")
        else pd.NAT,
        axis="columns"
    )
    input_df["first_name"] = input_df.apply(
        lambda x: x.first_name
        if hasattr(x, "first_name")
        else pd.NA,
        axis="columns"
    )
    input_df["last_name"] = input_df.apply(
        lambda x: x.last_name
        if hasattr(x, "last_name")
        else pd.NA,
        axis="columns"
    )
    input_df["mrn"] = input_df.apply(
        lambda x: x.mrn
        if hasattr(x, "mrn")
        else pd.NA,
        axis="columns"
    )
    input_df["hospital_number"] = input_df.apply(
        lambda x: x.hospital_number
        if hasattr(x, "hospital_number")
        else pd.NA,
        axis="columns"
    )
    input_df["facility"] = input_df.apply(
        lambda x: x.facility
        if hasattr(x, "facility")
        else pd.NA,
        axis="columns"
    )

    # Set medical record number as a list of dicts
    input_df["medical_record_numbers"] = input_df.apply(
        lambda x: [
            {
                "mrn": x.get("mrn", None),
                "facility": x.get("facility", None),
                "hospital_number": x.get("hospital_number", None)
            }
        ],
        axis="columns"
    )

    # Set physician as a list of dicts
    input_df["requesting_physicians"] = input_df.apply(
        lambda x: [
            {
                "first_name": x.get("requesting_physicians_first_name", None),
                "last_name": x.get("requesting_physicians_last_name", None)
            }
        ],
        axis="columns"
    )

    # Coerce dates to date objects
    for date_column in ["date_accessioned", "date_received", "date_collected", "date_of_birth"]:
        # Don't care if not here
        if date_column not in input_df.columns:
            continue
        # Get the input df date column as a utc date object
        input_df[date_column] = input_df[date_column].apply(
            lambda x: datetime_obj_to_utc(x)
                      if not pd.isnull(x)
                      else x
        )

    # Confirm dates are not later than now
    current_datetime = datetime_obj_to_utc(datetime.utcnow())
    for date_column in ["date_accessioned", "date_received", "date_collected", "date_of_birth"]:
        if date_column not in input_df.columns:
            continue
        for date_time_obj in input_df[date_column]:
            if date_time_obj > current_datetime:
                logger.error(f"Got date {date_time_obj} which is in the future")
                raise ValueError

    return input_df
