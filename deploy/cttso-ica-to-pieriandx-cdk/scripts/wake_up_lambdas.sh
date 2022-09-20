#!/usr/bin/env bash

: '
Go through list of lambdas to wake up and return once complete
'

AWS_LAMBDA_CLINICAL_LAUNCH_FUNCTION="$( \
  aws ssm get-parameter \
    --output json \
    --name "redcap-to-pieriandx-lambda-function" | \
  jq --raw-output \
    '.Parameter.Value'
)"

AWS_LAMBDA_LAUNCH_PIERIANDX_FUNCTION="$( \
  aws ssm get-parameter \
    --output json \
    --name "cttso-ica-to-pieriandx-lambda-function" | \
  jq --raw-output \
    '.Parameter.Value'
)"

AWS_LAMBDA_REDCAP_APIS_FUNCTION="$( \
  aws ssm get-parameter \
    --output json \
    --name "redcap-apis-lambda-function" | \
  jq --raw-output \
    '.Parameter.Value'
)"

AWS_LAMBDA_VALIDATION_LAUNCH_FUNCTION="$( \
  aws ssm get-parameter \
    --output json \
    --name "validation-sample-to-pieriandx-lambda-function" | \
  jq --raw-output \
    '.Parameter.Value'
)"

# List as an array
LAMBDA_FUNCTIONS_ARRAY=( \
  "${AWS_LAMBDA_CLINICAL_LAUNCH_FUNCTION}" \
  "${AWS_LAMBDA_LAUNCH_PIERIANDX_FUNCTION}" \
  "${AWS_LAMBDA_REDCAP_APIS_FUNCTION}" \
  "${AWS_LAMBDA_VALIDATION_LAUNCH_FUNCTION}"
)
INACTIVE_LAMBDA_FUNCTIONS_ARRAY=()

# Iterate through lambda functions
for lambda_function in "${LAMBDA_FUNCTIONS_ARRAY[@]}"; do
  lambda_state="$( \
    aws lambda get-function \
      --output json \
      --function-name "${lambda_function}" | \
      jq --raw-output '.Configuration.State' \
  )"
  if [[ ! "${lambda_state,,}" == "active" ]]; then
    echo "Got lambda state of '${lambda_state}' for '${lambda_function}'" 1>&2
    echo "Adding to list of inactivate lamdas to warm up"
    INACTIVE_LAMBDA_FUNCTIONS_ARRAY+=( "${lambda_function}" )
  fi
done

# Iterate through lambda functions and warm them up
for lambda_function in "${INACTIVE_LAMBDA_FUNCTIONS_ARRAY[@]}"; do
  echo "Launching blank invocation of '${lambda_function}'" 1>&2
  lambda_invocation_output="$( \
    aws lambda invoke \
      --output json \
      --function-name "${lambda_function}" \
      --payload "{}" \
      --invocation-type RequestResponse \
      warmup.json
  )"
  echo "${lambda_invocation_output}"
done

# Iterate through inactive lambdas
while :; do
  # Check if any lambdas have been turned to active
  for lambda_function in "${INACTIVE_LAMBDA_FUNCTIONS_ARRAY[@]}"; do
    lambda_state="$( \
      aws lambda get-function \
        --output json \
        --function-name "${lambda_function}" | \
        jq --raw-output '.Configuration.State' \
    )"
    if [[ "${lambda_state,,}" == "active" ]]; then
      echo "Lambda function '${lambda_function}' is now active" 1>&2
      delete=("${lambda_function}")
      IFS=" " read -r -a INACTIVE_LAMBDA_FUNCTIONS_ARRAY <<< "${INACTIVE_LAMBDA_FUNCTIONS_ARRAY[@]/$delete}"
    fi
  done

  # Check length list
  num_waking_lambdas="${#INACTIVE_LAMBDA_FUNCTIONS_ARRAY[@]}"
  if [[ "${num_waking_lambdas}" == "0" ]]; then
    echo "All lambdas are active" 1>&2
    break
  fi

  echo "Still waiting on ${num_waking_lambdas} lambdas to wake up, checking again in 10 seconds" 1>&2
  sleep 10
done

