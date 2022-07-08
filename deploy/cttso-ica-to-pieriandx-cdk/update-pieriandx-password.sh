#!/usr/bin/env bash

: '
Update the pieriandx password
'

set -euo pipefail

# Globals
SECRET_ID="PierianDx/UserPassword"

# Get json file based on account
account_id="$(aws sts get-caller-identity --output json | jq --raw-output '.Account')"

if [[ "${account_id}" == "843407916570" ]]; then
  echo "Updating password in dev" 1>&2
elif [[ "${account_id}" == "472057503814" ]]; then
  echo "Updating password in prod" 1>&2
else
  echo "Please ensure you're logged in to either dev or prod" 1>&2
  exit 1
fi

echo -n "Please enter your password: " 1>&2
# Use -s
read -s pieriandx_password
echo ""

input_secret_json_str="$( \
  jq \
    --null-input \
    --raw-output \
    --compact-output \
    --arg pieriandx_password "${pieriandx_password}" \
    '
      {
        "PierianDxUserPassword": $pieriandx_password
      }
    '
)"

echo "Updating secret in ${account_id}" 1>&2
aws secretsmanager update-secret \
  --secret-id "${SECRET_ID}" \
  --secret-string "${input_secret_json_str}"
