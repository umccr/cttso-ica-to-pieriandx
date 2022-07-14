#!/usr/bin/env bash

set -euo pipefail

date_to_utc_time(){
  local date_str="$1"
  python - << EOF
import pytz
from datetime import datetime
from dateutil.parser import parse as date_parser
print(date_parser("${date_str}").astimezone(pytz.utc).isoformat())
EOF
}

# Get json file based on account
ACCOUNT_ID="$( \
  aws sts get-caller-identity \
    --output json | \
  jq --raw-output \
    '.Account' \
)"

if [[ "${ACCOUNT_ID}" == "472057503814" ]]; then
  echo "Submitting workflow in prod" 1>&2
elif [[ "${ACCOUNT_ID}" == "843407916570" ]]; then
  echo "Submitting workflow in dev" 1>&2
else
  echo "Please ensure you're logged in to either dev or prod" 1>&2
  exit 1
fi

# Globals
AWS_SUBMISSION_LAMBDA_FUNCTION_ARM_SSM_PARAMETER_PATH="cttso-ica-to-pieriandx-lambda-function"

# Get current timestamp
CURRENT_TIMESTAMP="$(date --utc --iso-8601=second)"

# Submission inputs
DISEASE_ID="__BLANK__"                                # From GLIMS.disease_id - example 55342001
REQUESTING_PHYSICIANS_FIRST_NAME="__BLANK__"          # From GLIMS.requesting_physicians_last_name - example Sean
REQUESTING_PHYSICIANS_LAST_NAME="__BLANK__"           # From GLIMS.requesting_physicians_first_name - example Grimmond
SUBJECT_ID="__BLANK__"                                # From GLIMS.subject_id - example SBJ00595
LIBRARY_ID="__BLANK__"                                # From GLIMS.library_id - example L2200417
DATE_COLLECTED="$(date_to_utc_time "__BLANK__")"      # From GLIMS.date_collected - example 2022-05-23T08:00:00+1000
DATE_RECEIVED="$(date_to_utc_time "__BLANK__")"       # From GLIMS.date_recieved - example 2022-05-23T09:00:00+1000
PATIENT_URN="__BLANK__"                               # From GLIMS.external_specimen_id - example SSq-ctDNA-CompMutMix1pc
SAMPLE_TYPE="__BLANK__"                               # From GLIMS.study_id - example validation
GENDER="__BLANK__"                                    # From GLIMS.gender - example unknown
EXTERNAL_SPECIMEN_ID="__BLANK__"                      # From GLIMS.external_specimen_id - example SSq-ctDNA-CompMutMix1pc
MRN="__BLANK__"                                       # From GLIMS.external_specimen_id - example SSq-ctDNA-CompMutMix1pc
SPECIMEN_TYPE="122561005"                             # From GLIMS.specimen_type (overridden to match previous submissions) - example
IS_IDENTIFIED="true"                                  # Default -
INDICATION="NA"                                       # Default -
HOSPITAL_NUMBER="99"                                  # Default -
ACCESSION_NUMBER="${SUBJECT_ID}_${LIBRARY_ID}_001"    # Combination of subject and library id
DATE_ACCESSIONED="${CURRENT_TIMESTAMP}"               # Set as current timestamp
DATE_OF_BIRTH="1970-01-01"                            # Default
FIRST_NAME="John"                                     # Default
LAST_NAME="Doe"                                       # Default

# Workflow Run ID
# ica workflows runs list --max-items=0 | grep "${SUBJECT_ID}__${LIBRARY_ID}"
ICA_WORKFLOW_RUN_ID="__BLANK__"                       # Collect from ICA or portal example - wfr.2e075f4b0f9c4e3aa1f434441ffe765f

# Set dryrun
DRYRUN="false"                                        # Set to 'true' if don't actually want to submit data to pieriandx
VERBOSE="false"                                       # Set to 'true' to turn on debug level logging

cttso_ica_to_pieriandx_payload="$( \
  jq \
    --null-input \
    --raw-output \
    --compact-output \
    --argjson disease_id "${DISEASE_ID}" \
    --arg requesting_physicians_first_name "${REQUESTING_PHYSICIANS_FIRST_NAME}" \
    --arg requesting_physicians_last_name "${REQUESTING_PHYSICIANS_LAST_NAME}" \
    --arg subject_id "${SUBJECT_ID}" \
    --arg library_id "${LIBRARY_ID}" \
    --arg date_collected "${DATE_COLLECTED}" \
    --arg date_received "${DATE_RECEIVED}" \
    --arg patient_urn "${PATIENT_URN}" \
    --arg sample_type "${SAMPLE_TYPE}" \
    --arg gender "${GENDER}" \
    --arg external_specimen_id "${EXTERNAL_SPECIMEN_ID}" \
    --arg mrn "${MRN}" \
    --argjson specimen_type "${SPECIMEN_TYPE}" \
    --argjson is_identified "${IS_IDENTIFIED}" \
    --arg indication "${INDICATION}" \
    --argjson hospital_number "${HOSPITAL_NUMBER}" \
    --arg accession_number "${ACCESSION_NUMBER}" \
    --arg date_accessioned "${DATE_ACCESSIONED}" \
    --arg date_of_birth "${DATE_OF_BIRTH}" \
    --arg first_name "${FIRST_NAME}" \
    --arg last_name "${LAST_NAME}" \
    --arg ica_workflow_run_id "${ICA_WORKFLOW_RUN_ID}" \
    --argjson dryrun "${DRYRUN}" \
    --argjson verbose "${VERBOSE}" \
    '
      {
        "parameters": {
          "accession_json_base64_str": (
            {
              "disease_id": $disease_id,
              "requesting_physicians_first_name": $requesting_physicians_first_name,
              "requesting_physicians_last_name": $requesting_physicians_last_name,
              "subject_id": $subject_id,
              "library_id": $library_id,
              "date_collected": $date_collected,
              "date_received": $date_received,
              "patient_urn": $patient_urn,
              "sample_type": $sample_type,
              "gender": $gender,
              "external_specimen_id": $external_specimen_id,
              "mrn": $mrn,
              "specimen_type": $specimen_type,
              "is_identified": $is_identified,
              "indication": "NA",
              "hospital_number": $hospital_number,
              "accession_number": $accession_number,
              "date_accessioned": $date_accessioned,
              "date_of_birth": $date_of_birth,
              "first_name": $first_name,
              "last_name": $last_name
            } | @base64
          ),
          "ica_workflow_run_id": $ica_workflow_run_id,
          "dryrun": $dryrun,
          "verbose": $verbose
        }
      }
    ' \
)"

cttso_ica_pieriandx_function_arn="$( \
    aws ssm get-parameter \
      --name "${AWS_SUBMISSION_LAMBDA_FUNCTION_ARM_SSM_PARAMETER_PATH}" \
      --output json | \
    jq --raw-output \
      '.Parameter.Value' \
)"


aws lambda invoke \
  --cli-binary-format "raw-in-base64-out" \
  --function-name "${cttso_ica_pieriandx_function_arn}" \
  --payload "${cttso_ica_to_pieriandx_payload}" \
  /dev/stdout
