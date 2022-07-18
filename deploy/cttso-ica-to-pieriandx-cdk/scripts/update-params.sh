#!/usr/bin/env bash

# Set to fail
set -euo pipefail

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

# Get json file based on account
account_id="$( \
  aws sts get-caller-identity \
    --output json | \
  jq --raw-output \
    '.Account' \
)"


if [[ "${account_id}" == "843407916570" ]]; then
  echo "Deploying in dev" 1>&2
  params_file="$(get_this_path)/../params-dev.json"
elif [[ "${account_id}" == "472057503814" ]]; then
  echo "Deploying in prod" 1>&2
  params_file="$(get_this_path)/../params-prod.json"
else
  echo "Could not get params file, please ensure you're logged in to either dev or prod" 1>&2
  exit 1
fi


# Check json file exists
if [[ ! -r "${params_file}" ]]; then
  echo "Error, could not find file '${params_file}'. Exiting" 1>&2
fi

# Get keys
param_keys="$(jq --raw-output 'keys[]' < "${params_file}")"

# Iterate through keys in for loop
for key in ${param_keys}; do
  # Get value
  value="$( \
    jq \
      --raw-output \
      --arg key_name "${key}" \
      '.[$key_name]' < "${params_file}" \
  )"

  if current_value="$( \
      aws ssm get-parameter \
        --output json --name "${key}" | \
      jq --raw-output \
        '.Parameter?.Value' \
    )" 2>/dev/null; then
    # Compare on new value
    if [[ "${current_value}" == "${value}" ]]; then
      echo "Current value for '${key}' is already '${value}', skipping update" 1>&2
      continue
    fi
  fi

  # Put parameter on ssm
  if [[ -n "${current_value-}" ]]; then
    echo "Updating parameter '${key}' from '${current_value}' to '${value}'" 1>&2
  else
    echo "Setting '${key}' as '${value}'" 1>&2
  fi
  aws ssm put-parameter \
    --output json \
    --overwrite \
    --name "${key}" \
    --value "${value}" \
    --type "String"

done
