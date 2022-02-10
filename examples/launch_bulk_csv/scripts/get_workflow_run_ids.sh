#!/usr/bin/env bash

all_workflows="$(ica workflows runs list --max-items=0 --output-format json | jq '.items | sort_by(.timeCreated) | reverse')"

accession_numbers_file="$1"

if [[ ! -f "${accession_numbers_file}" ]]; then
	echo "Please provide a file with a list of the accession numbers" 2>&1
fi

echo "accession_number,ica_workflow_run_id"

while read -r line; do
	if [[ "$line" == "__" ]]; then
		echo "__";
	elif [[ -z "$line" ]]; then
		:
	else
    subject="$(python -c "import re; print(re.match(\"(\w+)_(\w+)_(\d+)\", \"${line}\").group(1))")"
    library="$(python -c "import re; print(re.match(\"(\w+)_(\w+)_(\d+)\", \"${line}\").group(2))")"
		workflow_id="$(jq --raw-output --arg subject_library "${subject}__${library}" \
		                 '[.[] | select(.name | contains($subject_library)) | select(.status=="Succeeded")][0] | .id' \
		               <<< "${all_workflows}")"
		# Check a successful workflow exists for this subject / library type
		if [[ "$workflow_id" == "null" ]]; then
			workflow_id=""
		fi

		# Echo the subject and workflow ID
		echo "$line,$workflow_id"

	fi;
done < "${accession_numbers_file}"
