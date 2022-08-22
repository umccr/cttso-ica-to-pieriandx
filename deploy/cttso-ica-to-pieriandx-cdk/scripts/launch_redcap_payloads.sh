#!/usr/bin/env bash

set -euo pipefail


print_help(){
  echo "
        Usage: launch_payloads_template.sh (--payloads-file payloads.json)

        Description:
          Launch all payloads that exist in a file

        Options:
            --payloads-file      Required: The file that contains all of the payloads
            --help               Optional: Prints this help script

        Requirements:
          * jq
          * aws

        Environment:
          * Be logged into your AWS environment
        " 1>&2
}

payloads_file=""

# Get args from command line
while [ $# -gt 0 ]; do
  case "$1" in
    --payloads-file)
      payloads_file="$2"
      shift 1
      ;;
    -h | --help)
      print_help
      exit 0
      ;;
  esac
  shift 1
done

# Check payloads file exists
if [[ -z "${payloads_file}" ]]; then
  echo "Error! Did not get the --payloads-file parameter" 1>&2
  print_help
  exit 1
elif [[ ! -r "${payloads_file}" ]]; then
  echo "Error could not read file '${payloads_file}'" 1>&2
  exit 1
fi

echo "Checking binaries are present" 1>&2
if ! type jq aws 1>/dev/null 2>&1; then
  echo "Error! Please ensure all required binaries are installed" 1>&2
  print_help
  exit 1
fi


# Defaults / Globals
AWS_LAMBDA_FUNCTION="$( \
  aws ssm get-parameter \
    --output json \
    --name "redcap-to-pieriandx-lambda-function" | \
  jq --raw-output \
    '.Parameter.Value'
)"

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
  echo "Error! Please ensure you're logged in the UMCCR AWS prod or dev account" 1>&2
  print_help
  exit 1
fi

counter=0
array_length="$(wc -l "${payloads_file}")"

while read -r payload_line || [ -n "${payload_line}" ]; do
  # https://stackoverflow.com/questions/12916352/shell-script-read-missing-last-payload_line

  # Check json string is valid
  if ! jq --exit-status '.' >/dev/null 2>&1 <<< "${payload_line}"; then
    # https://stackoverflow.com/a/46955018/6946787
    echo "Failed to parse JSON string '${payload_line}', skipping this payload" 1>&2
    continue
  fi

  counter="$((counter + 1))"
  payload_json_str="$( \
    jq --null-input --raw-output --compact-output \
      "${payload_line}" \
  )"
  subject_id="$( \
    jq --raw-output \
      '.subject_id' <<< "${payload_json_str}" \
  )"
  library_id="$( \
    jq --raw-output \
      '.library_id' <<< "${payload_json_str}" \
  )"

  lambda_output_file="${subject_id}__${library_id}.json"

  echo "Launch sample ${counter} out of ${array_length}: Payload is"
  echo aws lambda invoke \
    --function-name "${AWS_LAMBDA_FUNCTION}" \
    --invocation-type "RequestResponse" \
    --payload "${payload_json_str}" \
    --cli-binary-format "raw-in-base64-out" \
    "${lambda_output_file}"

  aws lambda invoke \
      --function-name "${AWS_LAMBDA_FUNCTION}" \
      --invocation-type "RequestResponse" \
      --payload "${payload_json_str}" \
      --cli-binary-format "raw-in-base64-out" \
      "${lambda_output_file}"

  echo "Wrote output of lambda to '${lambda_output_file}'"

done < "${payloads_file}"
