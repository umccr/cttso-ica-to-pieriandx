#!/usr/bin/env bash

: '
Update the pieriandx password
'

set -euo pipefail

# Globals
SECRET_ID="PierianDx/UserPassword"

# Glocals
env_midfix=""
institution=""
base_url=""

# Get json file based on account
account_id="$(aws sts get-caller-identity --output json | jq --raw-output '.Account')"

if [[ "${account_id}" == "843407916570" ]]; then
  echo "Updating password in dev" 1>&2
  env_midfix="dev"
  institution="melbournetest"
  base_url="https://app.uat.pieriandx.com/cgw-api/v2.0.0"
elif [[ "${account_id}" == "472057503814" ]]; then
  echo "Updating password in prod" 1>&2
  env_midfix="prod"
  institution="melbourne"
  base_url="https://app.pieriandx.com/cgw-api/v2.0.0"
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
secret_version_id="$( \
  aws secretsmanager update-secret \
    --output json \
    --secret-id "${SECRET_ID}" \
    --secret-string "${input_secret_json_str}" | \
  jq --raw-output ".VersionId" \
)"

echo "Got secret version id ${secret_version_id}" 1>&2

echo "Sleeping 5 for secret to be fully updated" 1>&2
sleep 5

echo "Running lambda to update token" 1>&2
aws lambda invoke \
  --output json \
  --function-name "cttso-ica-to-pieriandx-${env_midfix}-token-refresh-lambda-stack-lf" \
  /dev/null > /dev/null

echo "Sleep 5 for token to be fully updated" 1>&2
sleep 5

echo "Collecting token from aws secretsmanager" 1>&2
new_auth_token="$( \
  aws secretsmanager get-secret-value \
    --secret-id PierianDx/UserAuthToken \
    --output json | \
  jq --raw-output \
   '.SecretString | fromjson | .PierianDxUserAuthToken'
)"

echo "Listing All Cases" 1>&2
if [[ "$( \
  curl \
    --fail --silent --location --show-error \
    --request "GET" \
    --url "${base_url}/case" \
    --header "Accept: application/json" \
    --header "X-Auth-Email: services@umccr.org" \
    --header "X-Auth-Token: $new_auth_token" \
    --header "X-Auth-Institution: $institution" | \
  jq 'length > 0' \
)" == "true" ]]; then
  echo "Password changed successfully!" 1>&2
else
  echo "Error! Password changed however was not able to use generated token" 1>&2
fi
