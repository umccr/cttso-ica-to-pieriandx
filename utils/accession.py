#!/usr/bin/env python

"""
Read in the accession csv or json
"""

from typing import List
from pathlib import Path
import pandas as pd

from utils.logging import get_logger
from dateutil.parser import parse as date_parser
from datetime import datetime, timezone
from utils.globals import MANDATORY_INPUT_COLUMNS, OPTIONAL_DEFAULTS, ACCESSION_FORMAT_REGEX, OUTPUT_STATS_FILE

from utils.micro_classes import Disease, SpecimenType
from utils.classes import Case
from utils.enums import SampleType, Ethnicity, Gender, Race

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

def read_input_json():
    raise NotImplementedError


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
    input_df: pd.DataFrame = pd.read_csv(input_csv, header=0, comment="#")

    # Convert blanks to nas
    input_df.replace("", pd.NA, inplace=True)

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
    input_df["sample_type"] = input_df["sample_type"].apply(lambda x: SampleType(x.lower()))
    input_df["ethnicity"] = input_df["ethnicity"].apply(lambda x: Ethnicity(x.lower()))
    input_df["race"] = input_df["race"].apply(lambda x: Race(x.lower()))
    input_df["gender"] = input_df["gender"].apply(lambda x: Gender(x.lower()))

    # Set defaults for study identifier and study subject identifier
    input_df["study_identifier"] = input_df.apply(lambda x: x.study_identifier
                                                            if hasattr(x, "study_identifier")
                                                            and not pd.isna(x.study_identifier)
                                                            else x.accession_number,
                                                  axis="columns")

    input_df["study_subject_identifier"] = input_df.apply(lambda x: x.study_subject_identifier
                                                                    if hasattr(x, "study_subject_identifier")
                                                                    and not pd.isna(x.study_subject_identifier)
                                                                    else x.sample_type.value,
                                                          axis="columns")

    # Coerce dates to date objects
    for date_column in ["date_accessioned", "date_received", "date_collected"]:
        # Get the input df date column as a utc date object
        input_df[date_column] = input_df[date_column].apply(lambda x: date_parser(x).
                                                            replace(tzinfo=timezone.utc).
                                                            astimezone(pytz.utc).replace(microsecond=0))

    # Confirm dates are not later than now
    current_datetime = datetime.utcnow().astimezone(pytz.utc)
    for date_column in ["date_accessioned", "date_received", "date_collected"]:
        for date_time_obj in input_df[date_column]:
            if date_time_obj > current_datetime:
                logger.error(f"Got date {date_time_obj} which is in the future")
                raise ValueError

    return input_df



