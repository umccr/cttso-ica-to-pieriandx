## Launch bulk metadata csv

The launch_bulk_csv.py script allows one to launch multiple pieriandx samples through the AWS lambda.

*The example data contains only one sample metadata for privacy reasons.*


### Input csv layout

The layout of the **metadata csv** file is similar to that from the bulk uploader and requires the following columns
* sample_type
* indication
* disease_id
* is_identified
* requesting_physicians_first_name
* requesting_physicians_last_name
* accession_number
* study_id
* participant_id
* specimen_type
* external_specimen_id
* date_accessioned
* date_collected
* data_received
* gender
* ethnicity
* race

The **ica run by accession number csv** file requires the following two columns:
* accession_number
* ica_workflow_run_id

### Running the workflow
You will need to generate a csv of accession number with the associated workflow id.  

The following code may be of use
```bash
ica-context-switcher --scope read-only --project production

bash get_workflow_run_ids.sh > data/workflow_runs_by_accession_number.csv

```

You may then launch the workflow with the following parameters
```bash
python scripts/launch_bulk_csv.py \
  --ica-worklfow-run-by-accession-number-csv data/workflow_runs_by_accession_number.csv \
  --pieriandx-metadata-csv data/metadata.csv \
  --output data/payloads.csv
```

### Notes on the launch_bulk_csv.py script

* Any row with 'date_accessioned' column already filled is ignored (`-` is permitted)
* Any row with 'tba' in any column is ignored
* Payloads are launched 10 seconds apart to alleviate the burden on the PierianDx api.

