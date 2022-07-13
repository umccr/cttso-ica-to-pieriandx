#!/usr/bin/env bash

set -euo pipefail

PAYLOAD_ARRAY=( \
  '__INSERT_PAYLOAD_HERE__' \
  '__INSERT_PAYLOAD_HERE__' \
)

AWS_LAMBDA_FUNCTION="$( \
  aws ssm get-parameter \
    --output json \
    --name "redcap-to-pieriandx-lambda-function" | \
  jq --raw-output \
    '.Parameter.Value'
)"

COUNTER=0
ARRAY_LENGTH="${#PAYLOAD_ARRAY[@]}"

for payload in "${PAYLOAD_ARRAY[@]}"; do
  COUNTER="$((COUNTER + 1))"
  payload_json_str="$( \
    jq --null-input --raw-output --compact-output \
      "${payload}" \
  )"
  library_id="$( \
    jq --raw-output '.library_id' <<< "${payload_json_str}" \
  )"

  echo "Launch sample ${COUNTER} out of ${ARRAY_LENGTH}"
  echo aws lambda invoke \
    --function-name "${AWS_LAMBDA_FUNCTION}" \
    --invocation-type "RequestResponse" \
    --payload "${payload_json_str}" \
    --cli-binary-format raw-in-base64-out \
    "${library_id}.json"

  aws lambda invoke \
      --function-name "${AWS_LAMBDA_FUNCTION}" \
      --invocation-type "RequestResponse" \
      --payload "${payload_json_str}" \
      --cli-binary-format raw-in-base64-out \
      "${library_id}.json"
done
