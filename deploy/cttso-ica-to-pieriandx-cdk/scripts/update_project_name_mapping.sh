#!/usr/bin/env bash

# Set production account as env var
PROD_ACCOUNT="472057503814"
SSM_PATH="cttso-lims-project-name-to-pieriandx-mapping"
MAPPING_PATH_NAME="project-name-to-pieriandx-mapping.json"

# Get json file based on account
ACCOUNT_ID="$( \
  aws sts get-caller-identity \
    --output json | \
  jq --raw-output \
    '.Account' \
)"

if [[ "${ACCOUNT_ID}" == "${PROD_ACCOUNT}" ]]; then
  echo "Updating mapping in prod" 1>&2
else
  echo "Error! Please ensure you're logged in the UMCCR AWS prod account" 1>&2
  print_help
  exit 1
fi

# Get this directory path
get_this_path() {
  : '
  Mac users use greadlink over readlink
  Return the directory of where this install.sh file is located
  '
  local this_dir

  # darwin is for mac, else linux
  if [[ "${OSTYPE}" == "darwin"* ]]; then
    readlink_program="greadlink"
  else
    readlink_program="readlink"
  fi

  # Get directory name of the install.sh file
  this_dir="$(dirname "$("${readlink_program}" -f "${0}")")"

  # Return directory name
  echo "${this_dir}"
}

project_name_mapping_json_file="$(get_this_path)/../${MAPPING_PATH_NAME}"

# Validate json
if ! jq -r < "${project_name_mapping_json_file}" 1>/dev/null; then
  echo "mapping json is not valid json"
fi

compact_json_wrapped_str="$( \
  jq --raw-output --compact-output \
  < "${project_name_mapping_json_file}" \
)"

if current_value="$( \
    aws ssm get-parameter \
      --output json --name "${SSM_PATH}" | \
    jq --raw-output \
      '.Parameter?.Value' \
  )" 2>/dev/null; then
  # Compare on new value
  if [[ "${current_value}" == "${compact_json_wrapped_str}" ]]; then
    echo "Current value for '${SSM_PATH}' already matches '${MAPPING_PATH_NAME}', skipping update" 1>&2
    exit
  fi
fi

# Put the ssm parameter
echo "Updating ssm parameter with contents of '${MAPPING_PATH_NAME}'"
aws ssm put-parameter \
  --name "${SSM_PATH}" \
  --output json \
  --overwrite \
  --type "String" \
  --value "${compact_json_wrapped_str}"




