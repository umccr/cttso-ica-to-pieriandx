#!/usr/bin/env bash

set -euo pipefail

# Set globals
TOKEN_SECRET_ID="PierianDx/UserAuthToken"

# Set preliminary functions
get_aws_ssm_parameter () {
  # Get AWS SSM Parameter
  local ssm_parameter_path="$1"
    aws ssm get-parameter \
    --output json \
    --name "${ssm_parameter_path}" | \
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
    --output json \
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

  # Check secret exists 0 for false, 1 for true
  secret_exists="$( \
    aws secretsmanager list-secrets \
      --output json \
      --filters "$( \
        jq --null-input --raw-output \
          '
            [
              {
                "Key": "name",
                "Values": [
                  "PierianDx/UserAuthToken"
                ]
              }
            ]
          ' \
      )" | \
    jq --raw-output \
      '
        .SecretList | length
      ' \
  )"

  if [[ "${secret_exists}" == "1" ]]; then
      # Just update the secret
      echo "Updating token" 1>&2
      aws secretsmanager update-secret \
        --secret-id "${TOKEN_SECRET_ID}" \
        --secret-string "${input_secret_json_str}"
  else
      echo "Creating secret and token" 1>&2
      aws secretsmanager create-secret \
        --name "${TOKEN_SECRET_ID}" \
        --secret-string "${input_secret_json_str}"
  fi

  echo "Successfully created/updated token" 1>&2

}
