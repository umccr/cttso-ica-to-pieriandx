#!/usr/bin/env python3
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import os
from typing import List

import pandas as pd
import numpy as np
from gspread_pandas import Spread

# Locals
from .globals import \
    GOOGLE_LIMS_SHEET_ID_SSM_PARAMETER_PATH, \
    GOOGLE_LIMS_AUTH_JSON_SSM_PARAMETER_PATH, \
    CTTSO_LIMS_SHEET_ID_SSM_PARAMETER_PATH
from .aws_helpers import get_boto3_ssm_client, SSMClient
from .miscell import get_alphabet


def get_glims_sheet_id() -> str:
    """
    Get the sheet id for glims
    """
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(Name=GOOGLE_LIMS_SHEET_ID_SSM_PARAMETER_PATH,
                                    WithDecryption=True).get("Parameter").get("Value")


def get_cttso_lims_sheet_id() -> str:
    """
    Get the sheet id for cttso lims
    :return:
    """
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(Name=CTTSO_LIMS_SHEET_ID_SSM_PARAMETER_PATH,
                                    WithDecryption=True).get("Parameter").get("Value")


def get_google_secret_contents() -> str:
    ssm_client: SSMClient = get_boto3_ssm_client()

    return ssm_client.get_parameter(Name=GOOGLE_LIMS_AUTH_JSON_SSM_PARAMETER_PATH,
                                    WithDecryption=True).get("Parameter").get("Value")


def download_google_secret_json_to_gspread_pandas_dir(gspread_pandas_dir: Path):
    secret_contents: str = get_google_secret_contents()

    if not gspread_pandas_dir.is_dir():
        gspread_pandas_dir.mkdir(parents=True, exist_ok=True)

    with open(gspread_pandas_dir / "google_secret.json", "w") as g_secret_h:
        g_secret_h.write(secret_contents)


def create_gspread_pandas_dir() -> Path:
    """
    Get the gspread pandas creds directory
    :return:
    """

    # Create the directory
    gspread_pandas_creds_dir = TemporaryDirectory()

    return Path(gspread_pandas_creds_dir.name)


def set_google_secrets():
    if os.environ.get("GSPREAD_PANDAS_CONFIG_DIR", None) is not None:
        return

    # Add in the secret and set the env var
    gspread_pandas_dir = create_gspread_pandas_dir()

    download_google_secret_json_to_gspread_pandas_dir(gspread_pandas_dir)

    os.environ["GSPREAD_PANDAS_CONFIG_DIR"] = str(gspread_pandas_dir)


def get_column_range(series_length: int) -> List:
    """
    A to Z plus AA, AB, AC etc
    :param series_length:
    :return:
    """
    column_range = get_alphabet()
    counter = 0

    while True:
        if len(column_range) >= series_length:
            break
        column_range = column_range + list(
            map(
                lambda letter: get_alphabet()[counter] + letter,
                get_alphabet()
            )
        )

        counter += 1

    return column_range[:series_length]


def update_cttso_lims_row(new_row: pd.Series, row_number: int):
    """
    Update cttso lims row
    :param new_row:
    :param row_number:
    :return:
    """

    new_row = new_row.replace({pd.NaT: None}).replace({'NaT': None}).replace({np.NaN: ""})

    series_length = new_row.shape[0]
    column_range = get_column_range(series_length)
    sheet_obj = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1")
    sheet_obj.update_cells(
        start=f"{column_range[0]}{row_number}",
        end=f"{column_range[-1]}{row_number}",
        vals=new_row.map(str).tolist()
    )


def append_row_to_cttso_lims(new_row: pd.Series):
    """
    Add a cttso lims row
    :param new_row:
    :return:
    """
    # Collect series length
    series_length = new_row.shape[0]
    column_range = get_alphabet()[:series_length]

    # Open up the sheet object
    sheet_obj = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1")

    # Get the total number of rows in the sheet
    num_rows = sheet_obj.sheet_to_df().shape[0]

    # Add another row
    sheet_obj.update_cells(
        start=f"{column_range[0]}{num_rows+2}",
        end=f"{column_range[-1]}{num_rows+2}",
        vals=new_row.map(str).tolist()
    )


def append_df_to_cttso_lims(new_df: pd.DataFrame, replace=False):
    """
    Add a new dataframe
    :param new_df:
    :param replace:
    :return:
    """
    # Open up the sheet object
    sheet_obj = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1")

    # Perform a proper NA replacement
    # https://github.com/pandas-dev/pandas/issues/29024#issuecomment-1098052276
    new_df = new_df.replace({pd.NaT: None}).replace({'NaT': None}).replace({np.NaN: ""})

    if replace:
        # Add an extra 1000 rows to the bottom of the page
        sheet_obj.df_to_sheet(
            pd.concat(
                [
                    new_df,
                    pd.DataFrame(columns=new_df.columns, index=range(1000))
                ]
            ),
            index=False, replace=True, fill_value=""
        )
    else:
        # Get existing sheet
        existing_sheet = sheet_obj.sheet_to_df(index=0)
        # Update the sheet object with the list
        sheet_obj.df_to_sheet(
            pd.concat(
                [
                    existing_sheet,
                    new_df,
                    pd.DataFrame(columns=existing_sheet.columns, index=range(1000))
                ]
            ),
            index=False, replace=True, fill_value=""
        )


def get_cttso_lims() -> (pd.DataFrame, pd.DataFrame):
    """
    Collect the values from the existing GSuite spreadsheet
    Maps the values from the existing GSuite spreadsheet to their excel row number
    Also returns the row value for each of the items
    :return: (
      A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * in_glims
        * in_portal
        * in_redcap
        * in_pieriandx
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * redcap_sample_type
        * redcap_is_complete
        * portal_run_id
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_submission_time
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_assignee
        * pieriandx_disease_code
        * pieriandx_disease_label
        * pieriandx_panel_type
        * pieriandx_sample_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
      ,
      A pandas DataFrame with the following columns:
        * cttso_lims_index
        * excel_row_number
    )
    """

    cttso_lims_df: pd.DataFrame = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1").sheet_to_df(index=0)

    cttso_lims_df = cttso_lims_df.replace("", pd.NA)

    # Replace booleans
    cttso_lims_df = cttso_lims_df.replace({
        "TRUE": True,
        "FALSE": False
    })

    excel_row_number_df: pd.DataFrame = pd.DataFrame({"cttso_lims_index": cttso_lims_df.index})

    # Conversion to 1-based index plus single header row
    excel_row_number_df["excel_row_number"] = excel_row_number_df.index + 2

    # Update legacy samples where pieriandx_submission_time is not set
    cttso_lims_df["pieriandx_submission_time"] = cttso_lims_df.apply(
        lambda x: x.pieriandx_case_creation_date
        if (
            pd.isnull(x.pieriandx_submission_time) or pd.isna(x.pieriandx_submission_time)
        )
        and not (
            pd.isnull(x.pieriandx_case_creation_date) or pd.isna(x.pieriandx_case_creation_date)
        )
        else x.pieriandx_submission_time,
        axis="columns"
    )

    return cttso_lims_df, excel_row_number_df


def get_deleted_lims_df() -> (pd.DataFrame, pd.DataFrame):
    """
    Collect the values from the existing GSuite spreadsheet
    Maps the values from the existing GSuite spreadsheet to their excel row number
    Also returns the row value for each of the items
    :return: (
      A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * in_glims
        * in_portal
        * in_redcap
        * in_pieriandx
        * glims_project_owner
        * glims_project_name
        * glims_panel
        * glims_sample_type
        * glims_is_identified
        * glims_default_snomed_term
        * glims_needs_redcap
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_run_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_submission_time
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_assignee
        * pieriandx_disease_code
        * pieriandx_disease_label
        * pieriandx_panel_type
        * pieriandx_sample_type
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
        * date_added_to_deletion_table
      ,
      A pandas DataFrame with the following columns:
        * cttso_lims_index
        * excel_row_number
    )
    :return:
    """
    deleted_lims_df: pd.DataFrame = Spread(spread=get_cttso_lims_sheet_id(), sheet="Deleted Cases").sheet_to_df(index=0)

    deleted_lims_df = deleted_lims_df.replace("", pd.NA)

    # Replace booleans
    deleted_lims_df = deleted_lims_df.replace({
        "TRUE": True,
        "FALSE": False
    })

    excel_row_number_df: pd.DataFrame = pd.DataFrame({"cttso_lims_index": deleted_lims_df.index})

    # Conversion to 1-based index plus single header row
    excel_row_number_df["excel_row_number"] = excel_row_number_df.index + 2

    return deleted_lims_df, excel_row_number_df


def append_rows_to_deleted_lims(to_be_deleted: pd.DataFrame):
    """
    List of rows to be added to the deleted lims database
    # FIXME add df
    :param to_be_deleted:
    :return:
    """
    # Open up the sheet object
    sheet_obj = Spread(spread=get_cttso_lims_sheet_id(), sheet="Deleted Cases")

    # Perform a proper NA replacement
    # https://github.com/pandas-dev/pandas/issues/29024#issuecomment-1098052276
    new_df = to_be_deleted.replace({pd.NaT: None}).replace({'NaT': None}).replace({np.NaN: ""})

    # Get existing sheet
    existing_sheet = sheet_obj.sheet_to_df(index=0)
    # Update the sheet object with the list
    sheet_obj.df_to_sheet(
        pd.concat(
            [
                existing_sheet,
                new_df,
                pd.DataFrame(columns=existing_sheet.columns, index=range(1000))
            ]
        ),
        index=False, replace=True, fill_value=""
    )


def add_deleted_cases_to_deleted_sheet(new_cases_to_delete_df: pd.DataFrame):
    """
    # FIXME add df here
    :param new_cases_to_delete_df:
    :return:
    """
    deleted_lims_df, excel_row_mapping_number = get_deleted_lims_df()

    # Create list for query
    existing_deleted_case_ids = deleted_lims_df["pieriandx_case_id"].tolist()

    # Get list of deleted cases
    new_cases_to_delete_df = new_cases_to_delete_df.query(
        "pieriandx_case_id not in @existing_deleted_case_ids",
        engine="python"
    )

    if new_cases_to_delete_df.shape[0] == 0:
        return

    new_cases_to_delete_df["date_added_to_deletion_table"] = datetime.utcnow().isoformat(sep="T", timespec="seconds") + "Z"

    append_rows_to_deleted_lims(new_cases_to_delete_df)
