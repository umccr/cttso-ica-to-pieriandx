from lambda_utils.gspread_helpers import set_google_secrets
from gspread_pandas import Spread
import pandas as pd

# Set google secrets
set_google_secrets()

# Create the new spreadsheet
new_spread = Spread(spread="ctTSO LIMS",
                    sheet="Sheet1",
                    create_spread=True)

new_headers = [
    "subject_id",
    "library_id",
    "in_glims",
    "in_portal",
    "in_redcap",
    "in_pieriandx",
    "glims_project_owner",
    "glims_project_name",
    "glims_panel",
    "glims_sample_type",
    "glims_is_identified",
    "glims_default_snomed_term",
    "glims_needs_redcap",
    "redcap_sample_type",
    "redcap_is_complete",
    "portal_wfr_id",
    "portal_wfr_end",
    "portal_wfr_status",
    "portal_sequence_run_name",
    "portal_is_failed_run",
    "pieriandx_submission_time",
    "pieriandx_case_id",
    "pieriandx_case_accession_number",
    "pieriandx_case_creation_date",
    "pieriandx_case_identified",
    "pieriandx_assignee",
    "pieriandx_disease_code",
    "pieriandx_disease_label",
    "pieriandx_panel_type",
    "pieriandx_sample_type",
    "pieriandx_workflow_id",
    "pieriandx_workflow_status",
    "pieriandx_report_status"
]

headers_df = pd.DataFrame(columns=new_headers)

new_spread.df_to_sheet(headers_df, headers=True, index=False, replace=True)

# Auth update
# Allow users to read
new_spread.add_permission(
    "all@umccr.org|reader"
)

# Allow yourself to edit
# You may need to manually add extra rows as some point
new_spread.add_permission(
    "alexis.lucattini@umccr.org|writer"
)

# Show url - to set ssm parameter
print(new_spread.url)
