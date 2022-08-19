#!/usr/bin/env python3

from pathlib import Path
from tempfile import TemporaryDirectory
import os
import pandas as pd
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
    # Add in the secret and set the env var
    gspread_pandas_dir = create_gspread_pandas_dir()

    download_google_secret_json_to_gspread_pandas_dir(gspread_pandas_dir)

    os.environ["GSPREAD_PANDAS_CONFIG_DIR"] = str(gspread_pandas_dir)


def get_cttso_samples_from_glims() -> pd.DataFrame:
    """
    Get cttso samples from GLIMS
    :return: A pandas DataFrame with the following columns
      * subject_id
      * library_id
      * glims_is_validation -> Is this a validation sample? Determined by ProjectName is equal to "Validation"
    """

    df: pd.DataFrame = Spread(spread=get_glims_sheet_id(), sheet="Sheet1").sheet_to_df()
    df = df.query("Type=='ctDNA' & Assay=='ctTSO'")
    df["glims_is_validation"] = df.apply(lambda x: True if x.ProjectName.lower() == "validation" else False)

    df = df.rename(
        columns={
            "SubjectID": "subject_id",
            "LibraryID": "library_id"
        }
    )

    return df[["subject_id", "library_id", "glims_is_validation"]]


def update_cttso_lims_row(new_row: pd.Series, row_number: int):
    """
    Update cttso lims row
    :param new_row:
    :param row_number:
    :return:
    """
    series_length = new_row.shape[0]
    column_range = get_alphabet()[:series_length]
    sheet_obj = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1")
    sheet_obj.update_cells(
        start=(row_number, column_range[0]),
        end=(row_number, column_range[-1]),
        vals=new_row.tolist()
    )


def append_row_to_cttso_lims(new_row: pd.Series):
    """
    Update cttso lims row
    :param new_row:
    :return:
    """
    # Collect series length
    series_length = new_row.shape[0]
    column_range = get_alphabet()[:series_length]

    # Open up the sheet object
    sheet_obj = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1")

    # Get the total number of rows in the sheet
    num_rows, _ = sheet_obj.get_sheet_dims()

    # Add another row
    sheet_obj.update_cells(
        start=(num_rows+1, column_range[0]),
        end=(num_rows+1, column_range[-1]),
        vals=new_row.tolist()
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
        * glims_is_validation
        * redcap_sample_type
        * redcap_is_complete
        * portal_wfr_id
        * portal_wfr_end
        * portal_wfr_status
        * portal_sequence_run_name
        * portal_is_failed_run
        * pieriandx_case_id
        * pieriandx_case_accession_number
        * pieriandx_case_creation_date
        * pieriandx_case_identified
        * pieriandx_workflow_id
        * pieriandx_workflow_status
        * pieriandx_report_status
        * pieriandx_report_signed_out  - currently ignored
      ,
      A pandas DataFrame with the following columns:
        * subject_id
        * library_id
        * excel_row_number
    )
    """

    cttso_lims_df: pd.DataFrame = Spread(spread=get_cttso_lims_sheet_id(), sheet="Sheet1").sheet_to_df()

    excel_row_number_df = cttso_lims_df[["subject_id", "library_id"]]
    excel_row_number_df["excel_row_number"] = excel_row_number_df.index + 2  # Conversion to 1-based index plus single header row

    return pd.DataFrame(), pd.DataFrame()
