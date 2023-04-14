#!/usr/bin/env python3

from pathlib import Path
from tempfile import TemporaryDirectory
import os
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
      * sequence_run_name
      * glims_is_validation -> Is this a validation sample? Determined by ProjectName is equal to "Validation" or "Control"
      * glims_is_research -> Is this a research sample? Determined by ProjectName is equal to "Research"
    """

    if os.environ.get("GSPREAD_PANDAS_CONFIG_DIR") is None:
        set_google_secrets()

    glims_df: pd.DataFrame = Spread(spread=get_glims_sheet_id(), sheet="Sheet1").sheet_to_df(index=0)
    glims_df = glims_df.query("Type=='ctDNA' & Assay=='ctTSO'")
    glims_df["glims_is_validation"] = glims_df.apply(
        lambda x: True if x.ProjectName.lower() in ["validation", "control"] else False,
        axis="columns"
    )
    glims_df["glims_is_research"] = glims_df.apply(
        lambda x: True if x.Workflow.lower() in ["research"] else False,
        axis="columns"
    )

    glims_df = glims_df.rename(
        columns={
            "SubjectID": "subject_id",
            "IlluminaID": "sequence_run_name",
            "LibraryID": "library_id"
        }
    )

    # Drop duplicate rows and return
    return glims_df[["subject_id", "library_id", "sequence_run_name", "glims_is_validation", "glims_is_research"]].drop_duplicates()


def update_cttso_lims_row(new_row: pd.Series, row_number: int):
    """
    Update cttso lims row
    :param new_row:
    :param row_number:
    :return:
    """

    new_row = new_row.replace({pd.NaT: None}).replace({'NaT': None}).replace({np.NaN: ""})

    series_length = new_row.shape[0]
    column_range = get_alphabet()[:series_length]
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
        * glims_is_validation
        * glims_is_research
        * redcap_sample_type
        * redcap_is_complete
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
        * pieriandx_panel_type
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
        if pd.isna(x.pieriandx_submission_time) and not pd.isna(x.pieriandx_case_creation_date)
        else x.pieriandx_submission_time,
        axis="columns"
    )

    return cttso_lims_df, excel_row_number_df
