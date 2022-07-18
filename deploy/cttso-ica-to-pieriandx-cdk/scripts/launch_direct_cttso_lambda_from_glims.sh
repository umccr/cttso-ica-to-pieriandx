#!/usr/bin/env bash

set -euo pipefail

# Help function
print_help(){
  echo "
        Usage: launch-direct-cttso-lambda-from-glims.sh (--subject-id SBJ...)
                                                        (--library-id L..)
                                                        [--ica-workflow-run-id wfr....]
                                                        [--dryrun]
                                                        [--verbose]
                                                        [--help]

        Description:
          Call cttso-ica-to-pieriandx and pull in all required information from GLIMS

        Options:
            --subject-id:                      Required: The subject id
            --library-id:                      Required: The library id
            --ica-workflow-run-id:             Optional: The ica workflow run id for a given sample
            --dryrun:                          Optional: If set, adds --dryrun parameter to pieriandx command
            --verbose:                         Optional: Turn on debugging
            --help:                            Print help

        Requirements:
          * jq
          * pip3 (pip)
          * aws
          * awscurl
          * python requirements:
            * pytz
            * gspread-pandas
            * dateutil

        Environment:
          * Be logged into your AWS environment
        " 1>&2
}

echo "Checking binaries are present" 1>&2
if ! type jq python3 pip3 awscurl aws 1>/dev/null 2>&1; then
  echo "Error! Please ensure all required binaries are installed" 1>&2
  print_help
  exit 1
fi

# Get json file based on account
ACCOUNT_ID="$( \
  aws sts get-caller-identity \
    --output json | \
  jq --raw-output \
    '.Account' \
)"

if [[ "${ACCOUNT_ID}" == "472057503814" ]]; then
  echo "Submitting workflow in prod" 1>&2
else
  echo "Error! Please ensure you're logged in the UMCCR AWS prod account" 1>&2
  print_help
  exit 1
fi


# Globals
AWS_SUBMISSION_LAMBDA_FUNCTION_ARM_SSM_PARAMETER_PATH="cttso-ica-to-pieriandx-lambda-function"
GOOGLE_LIMS_AUTH_JSON_SSM_PARAMETER_PATH="/umccr/google/drive/lims_service_account_json"
GOOGLE_LIMS_SHEET_ID_SSM_PARAMETER_PATH="/umccr/google/drive/lims_sheet_id"
WORKFLOW_TYPE_NAME="tso_ctdna_tumor_only"
REQUIRED_PYTHON_LIBRARIES=( \
  "pytz" \
  "gspread-pandas" \
  "python-dateutil" \
)

# Submission inputs
SPECIMEN_TYPE="122561005"                             # From GLIMS.specimen_type (overridden to match previous submissions) - example
IS_IDENTIFIED="true"                                  # Default -
INDICATION="NA"                                       # Default -
HOSPITAL_NUMBER="99"                                  # Default -
DATE_OF_BIRTH="1970-01-01"                            # Default -
FIRST_NAME="John"                                     # Default -
LAST_NAME="Doe"                                       # Default -

# Glocals
ica_workflow_run_id=""
subject_id=""
library_id=""
dryrun="false"
verbose="false"

# Get args from command line
while [ $# -gt 0 ]; do
  case "$1" in
    --subject-id)
      subject_id="$2"
      shift 1
      ;;
    --library-id)
      library_id="$2"
      shift 1
      ;;
    --ica-workflow-run-id)
      ica_workflow_run_id="$2"
      shift 1
      ;;
    --dryrun)
      dryrun="true"
      ;;
    --verbose)
      verbose="true"
      ;;
    -h | --help)
      print_help
      exit 0
      ;;
  esac
  shift 1
done

# Assert mandatory parameters exit
if [[ -z "${subject_id-}" ]]; then
  echo "Please provide the --subject-id parameter" 1>&2
  print_help
  exit 1
fi

if [[ -z "${library_id-}" ]]; then
  echo "Please provide the --library-id parameter" 1>&2
  print_help
  exit 1
fi

# Check we have all of our python libraries available
echo "Checking python3 pip3 installations"
pip_json_list_str="$( \
  pip3 list \
    --pre \
    --format=json | \
  jq --raw-output \
)"

for python_library in "${REQUIRED_PYTHON_LIBRARIES[@]}"; do
  has_library="$( \
    jq --raw-output \
      --arg python_library "${python_library}" \
      '
        map(
          select(.name==$python_library)
        ) |
        length
      ' \
  )" <<< "${pip_json_list_str}"
  if [[ ! "${has_library}" -lt "1" ]]; then
    echo "Error - python library '${python_library}' is not installed" 1>&2
    print_help
    exit 1
  fi
done

# Find missing ica workflow run id if it doesn't exist
if [[ -z "${ica_workflow_run_id-}" ]]; then
  echo "Attempting to find the ica workflow run id via the portal" 1>&2
  portal_run_id="$( \
    awscurl --region ap-southeast-2 \
      --request GET \
      --header "Content-Type: application/json" \
      --profile "${ACCOUNT_ID}" \
      "https://api.data.prod.umccr.org/iam/libraryrun?library_id=${library_id}" | \
    jq --raw-output \
      '
        .results |
        map(.workflows[-1]) |
        flatten |
        unique |
        .[0]
      ' \
  )"

  ica_workflow_run_id="$( \
    awscurl \
      --request GET \
      --region ap-southeast-2 \
      --header "Content-Type: application/json" \
      --profile "${ACCOUNT_ID}" \
      "https://api.data.prod.umccr.org/iam/workflows/${portal_run_id}" | \
    jq --raw-output \
      --arg type_name "${WORKFLOW_TYPE_NAME}" \
    '
      if ( .type_name == $type_name ) then
        .wfr_name
      else null
      end
    ' \
  )"

  if [[ "${ica_workflow_run_id}" == "null" ]]; then
    echo "Error: Could not get the right workflow, please use the --ica-workflow-run-id parameter instead" 1>&2
    exit 1
  fi

  echo "Collected the ica workflow run id '${ica_workflow_run_id}'" 1>&2

fi

# Get GSPREAD PANDAS LOGIC
GSPREAD_PANDAS_CONFIG_DIR="$(mktemp -d)"
aws ssm get-parameter \
  --output json \
  --with-decryption \
  --name "${GOOGLE_LIMS_AUTH_JSON_SSM_PARAMETER_PATH}" | \
jq --raw-output \
 '.Parameter.Value' > "${GSPREAD_PANDAS_CONFIG_DIR}/google_secret.json"
GSPREAD_SHEET_ID="$( \
  aws ssm get-parameter \
    --output json \
    --with-decryption \
    --name "${GOOGLE_LIMS_SHEET_ID_SSM_PARAMETER_PATH}" | \
  jq --raw-output \
    '.Parameter.Value'
)"


glims_json_str="$( \
(
python - << EOF
import os
import re
import pandas as pd
import pytz
from datetime import datetime
from dateutil.parser import parse as date_parser
from gspread_pandas.spread import Spread

os.environ["GSPREAD_PANDAS_CONFIG_DIR"] = "${GSPREAD_PANDAS_CONFIG_DIR}"
df = Spread(spread="${GSPREAD_SHEET_ID}", sheet="ctTSO500_Metadata").sheet_to_df()
df["library_id"] = df["accession_number"].apply(lambda x: re.match("SBJ\d+_(L\d+)(?:_\d+)?", x).group(1))
df = df.query("participant_id=='${subject_id}' & library_id=='${library_id}'")
df["date_collected"] = df["date_collected"].apply(lambda x: date_parser(x).astimezone(pytz.utc).isoformat())
df["date_received"] = df["date_received"].apply(lambda x: date_parser(x).astimezone(pytz.utc).isoformat())
df["date_accessioned"] = datetime.utcnow().isoformat()
print(df.to_json(orient="records"))
EOF
) | \
jq --raw-output \
  '
    .[0]
  '
)"

# Payload
cttso_ica_to_pieriandx_payload="$( \
  jq \
    --raw-output \
    --compact-output \
    --argjson specimen_type "${SPECIMEN_TYPE}" \
    --argjson is_identified "${IS_IDENTIFIED}" \
    --arg indication "${INDICATION}" \
    --argjson hospital_number "${HOSPITAL_NUMBER}" \
    --arg date_of_birth "${DATE_OF_BIRTH}" \
    --arg first_name "${FIRST_NAME}" \
    --arg last_name "${LAST_NAME}" \
    --arg ica_workflow_run_id "${ica_workflow_run_id}" \
    --argjson dryrun "${dryrun}" \
    --argjson verbose "${verbose}" \
    '
      . |
      {
        "parameters": {
          "accession_json_base64_str": (
            {
              "disease_id": .disease_id,
              "requesting_physicians_first_name": .requesting_physicians_first_name,
              "requesting_physicians_last_name": .requesting_physicians_last_name,
              "subject_id": .participant_id,
              "library_id": .library_id,
              "date_collected": .date_collected,
              "date_received": .date_received,
              "patient_urn": .external_specimen_id,
              "sample_type": ( .study_id | ascii_downcase ),
              "gender": .gender,
              "external_specimen_id": .external_specimen_id,
              "mrn": .external_specimen_id,
              "specimen_type": $specimen_type,
              "is_identified": $is_identified,
              "indication": $indication,
              "hospital_number": $hospital_number,
              "accession_number": .accession_number,
              "date_accessioned": .date_accessioned,
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
    ' <<< "${glims_json_str}" \
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

rm -rf "${GSPREAD_PANDAS_CONFIG_DIR}"
