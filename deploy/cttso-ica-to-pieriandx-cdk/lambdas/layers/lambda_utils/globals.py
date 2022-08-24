#!/usr/bin/env python

"""
All globals
"""

# Even globals needs some imports
from pathlib import Path
from typing import List
from datetime import datetime
import pytz
import re

# GLOBALS
PORTAL_API_BASE_URL_SSM_PATH = "/data_portal/backend/api_domain_name"
PORTAL_METADATA_ENDPOINT = "https://{PORTAL_API_BASE_URL}/iam/metadata/"
PORTAL_WORKFLOWS_ENDPOINT = "https://{PORTAL_API_BASE_URL}/iam/workflows"
PORTAL_SEQUENCE_RUNS_ENDPOINT = "https://{PORTAL_API_BASE_URL}/iam/sequencerun"
PORTAL_MAX_ROWS_PER_PAGE = 1000
PORTAL_CTTSO_TYPE_NAME = "tso_ctdna_tumor_only"
PORTAL_WORKFLOW_ORDERING = "-start"  # We generally want the latest

GOOGLE_LIMS_AUTH_JSON_SSM_PARAMETER_PATH = "/umccr/google/drive/lims_service_account_json"
GOOGLE_LIMS_SHEET_ID_SSM_PARAMETER_PATH = "/umccr/google/drive/lims_sheet_id"
CTTSO_LIMS_SHEET_ID_SSM_PARAMETER_PATH = "/cdk/cttso-ica-to-pieriandx/cttso_lims_sheet_id"

PIERIANDX_USER_EMAIL_SSM_PARAMETER_PATH = "/cdk/cttso-ica-to-pieriandx/env_vars/pieriandx_user_email"
PIERIANDX_INSTITUTION_SSM_PARAMETER_PATH = "/cdk/cttso-ica-to-pieriandx/env_vars/pieriandx_institution"
PIERIANDX_USER_PASSWORD_SECRETS_MANAGER_PATH = "PierianDx/UserPassword"
PIERIANDX_USER_PASSWORD_SECRETS_MANAGER_KEY = "PierianDxUserPassword"

PIERIANDX_CDK_SSM_PATH: Path = Path("/cdk") / "cttso-ica-to-pieriandx" / "env_vars"
PIERIANDX_CDK_SSM_LIST: List = [
    "PIERIANDX_USER_EMAIL",
    "PIERIANDX_INSTITUTION",
    "PIERIANDX_BASE_URL"
]

PIERIANDX_PASSWORD_SECRETS_PATH: Path = Path("PierianDx") / "UserPassword"
PIERIANDX_PASSWORD_SECRETS_KEY: str = "PierianDxUserPassword"

PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH = "cttso-ica-to-pieriandx-lambda-function"

MAX_ATTEMPTS_GET_CASES = 5
LIST_CASES_RETRY_TIME = 5

LOGGER_STYLE = "%(asctime)s - %(levelname)-8s - %(module)-25s - %(funcName)-40s : LineNo. %(lineno)-4d - %(message)s"
# Redcap lambda path
REDCAP_LAMBDA_FUNCTION_SSM_PARAMETER_PATH = "redcap-apis-lambda-function"
REDCAP_PROJECT_NAME_PARAMETER_PATH = "/cdk/cttso-ica-to-pieriandx/redcap_project_name"

# Validation lambda path
VALIDATION_LAMBDA_FUNCTION_ARN_SSM_PARAMETER_PATH = "validation-sample-to-pieriandx-lambda-function"

AUS_TIMEZONE = pytz.timezone("Australia/Melbourne")
AUS_TIMEZONE_SUFFIX = datetime.now(AUS_TIMEZONE).strftime("%z")
UTC_TIMEZONE = pytz.timezone("UTC")
# Current time with timezone suffix
CURRENT_TIME = UTC_TIMEZONE.localize(datetime.utcnow())

VALIDATION_DEFAULTS = {
    "sample_type": "validation",
    "indication": "NA",
    "disease_id": 285645000,
    "disease_name": "Disseminated malignancy of unknown primary",
    "is_identified": True,
    "requesting_physicians_first_name": "Sean",
    "requesting_physicians_last_name": "Grimmond",
    "first_name": "John",
    "last_name": "Doe",
    "date_of_birth": datetime.fromtimestamp(0).astimezone(UTC_TIMEZONE),
    "specimen_type": 122561005,
    "date_accessioned": CURRENT_TIME,
    "date_collected": CURRENT_TIME,
    "date_received": CURRENT_TIME,
    "gender": "unknown",
    "ethnicity": "unknown",
    "race": "unknown",
    "hospital_number": 99,
}

# Clinical Defaults
CLINICAL_DEFAULTS = {
    "is_identified": True,
    "specimen_type": 122561005,
    "indication": "NA",
    "hospital_number": 99,
    "date_of_birth": datetime.fromtimestamp(0).astimezone(UTC_TIMEZONE),
    "requesting_physicians_first_name": "Sean",
    "requesting_physicians_last_name": "Grimmond",
    "disease_id": 285645000,
    "disease_name": "Disseminated malignancy of unknown primary",
    "date_collected": CURRENT_TIME,
    "time_collected": CURRENT_TIME,
    "date_received": CURRENT_TIME,
    "patient_urn": "NA",
    "sample_type": "Patient Care Sample",
    "gender": "unknown",
    "pierian_metadata_complete": True,
    "patient_name": {
      "male": "John Doe",
      "female": "Jane Doe",
      "unknown": "John Doe"
    }
}

REDCAP_RAW_FIELDS_CLINICAL: List = [
    "record_id",
    "clinician_firstname",
    "clinician_lastname",
    "patient_urn",
    "disease",
    "date_collection",
    "time_collected",
    "date_receipt",
    "id_sbj",
    "libraryid"
]

REDCAP_LABEL_FIELDS_CLINICAL: List = [
    "record_id",
    "report_type",
    "disease",
    "patient_gender",
    "id_sbj",
    "libraryid",
    "pierian_metadata_complete"
]

PORTAL_FIELDS: List = [
    "subject_id",
    "library_id",
    "external_sample_id",
    "external_subject_id"
]

REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER: str = "redcap-apis-lambda-function"
REDCAP_PROJECT_NAME_SSM_PARAMETER: str = "/cdk/cttso-ica-to-pieriandx/redcap_project_name"

EXPECTED_ATTRIBUTES = [
    "sample_type",
    "disease_id",
    "indication",
    "accession_number",
    "external_specimen_id",
    "date_accessioned",
    "date_collected",
    "date_received",
    "hospital_number",
    "gender",
    "mrn",
    "requesting_physicians_first_name",
    "requesting_physicians_last_name"
]


WFR_NAME_REGEX = re.compile(
    # "umccr__automated__tso_ctdna_tumor_only__SBJ00998__L2101500__202112115d8bdae7"
    rf"umccr__automated__{PORTAL_CTTSO_TYPE_NAME}__(SBJ\d{{5}})__(L\d{{7}})__\S+"
)

