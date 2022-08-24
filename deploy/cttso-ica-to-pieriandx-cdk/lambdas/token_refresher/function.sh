#!/usr/bin/env bash

set -euo pipefail

# Set globals
TOKEN_SECRET_ID="PierianDx/UserAuthToken"

# Set preliminary functions
get_aws_ssm_parameter () {
  # Get AWS SSM Parameter
  local ssm_parameter_path="$1"
    aws ssm get-parameter \
    --parameter-name "${ssm_parameter_path}" | \
  jq --raw-output \
    '.Parameter.Value'
}

# Set glocals

# Get values
SERVICE_URL="$( \
  get_aws_ssm_parameter "/cdk/cttso-ica-to-pieriandx/env_vars/pieriandx_base_url"
)"
SERVICE_USERNAME="$( \
  get_aws_ssm_parameter "/cdk/cttso-ica-to-pieriandx/env_vars/pieriandx_user_email"
)"
SERVICE_INSTITUTION="$( \
  get_aws_ssm_parameter "/cdk/cttso-ica-to-pieriandx/env_vars/pieriandx_institution"
)"
SERVICE_PASSWORD="$( \
  aws secretsmanager get-secret-value \
    --secret-id 'PierianDx/UserPassword' | \
  jq --raw-output \
    '
      .SecretString |
      fromjson |
      .PierianDxUserPassword
    ' \
)"

LOGIN_URL="${SERVICE_URL}/login"

function handler () {
  auth_token="$( \
  curl \
    --fail \
    --silent \
    --location \
    --request GET "${LOGIN_URL}" \
    --header "Accept: application/json" \
    --header "X-Auth-Email: ${SERVICE_USERNAME}" \
    --header "X-Auth-Key: ${SERVICE_PASSWORD}" \
    --header "X-Auth-Institution: ${SERVICE_INSTITUTION}" \
    --write-out "%header{X-Auth-Token}" \
  )"

  # Failure
  if [[ -z "${auth_token}" ]]; then
    echo "Error! Couldn't get authentication token"
    exit 1
  fi

  # Get secret as a json string
  input_secret_json_str="$( \
    jq \
      --null-input \
      --raw-output \
      --compact-output \
      --arg auth_token "${auth_token}" \
      '
        {
          "PierianDxUserAuthToken": $auth_token
        }
      '
  )"

  # Success
  aws secretsmanager update-secret \
    --secret-id "${TOKEN_SECRET_ID}" \
    --secret-string "${input_secret_json_str}"
}
