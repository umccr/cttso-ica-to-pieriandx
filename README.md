# ctTSO ICA to PierianDX

This is a quick intermediary piece of software designed to be one-day obsolete.  That day will be a good day.  

In the meantime we can take a bulk list of samples in csv format,  
find the matching secondary analysis runs on ICA by matching up the library ids,  
pull down the data from ICA and transfer over to PierianDx's s3 bucket.  

The script then creates a case, sequencing run and informatics job on PierianDx.  

## Installation:

### Option 1: Recommended
> No installation required - just run things

Checkout our [deploy](deploy/cttso-ica-to-pieriandx-cdk) folder for how we automate this service in AWS. 

## Option 2: (Installation through conda)
> Few hacky bits

1. Clone this repo
```bash
git clone git@github.com:umccr/cttso-ica-to-pieriandx.git
```

2. Enter this repo and checkout version
```bash
cd cttso-ica-to-pieriandx
git checkout v1.0.0
```

3. Create the conda env
```bash
conda env create \
  --name 'cttso-ica-to-pieriandx' \
  --file 'cttso-ica-to-pieriandx-conda-env.yaml'
```

4. Activate the conda env
```bash
conda activate cttso-ica-to-pieriandx
```

5. Run setup.py whilst inside conda env
```bash
python setup.py install
```

6. Copy across references whilst inside conda env to the right location
```bash
rsync --archive \
  "references/" \
  "$(find "${CONDA_PREFIX}" -type d -name "references")/"
```

## Command usage

### ctTSO ICA to PierianDx

```
usage: cttso-ica-to-pieriandx.py [-h] [--ica-workflow-run-ids ICA_WORKFLOW_RUN_IDS] [--accession-json ACCESSION_JSON]
                                 [--accession-csv ACCESSION_CSV] [--verbose]

Given an input.json file, pull information from gds, upload to s3 for a single sample,
 create a case, run and informatics job

Given an input csv, pull information from gds, upload to s3 and create a case, run and 
 informatics job is for all samples in the csv.  
 
One may also specify the ica workflow run ids. If these are not specified, the list of workflow runs are searched to find
the workflow run on the bases of the library name.  

The following environment variables are expected:
  * ICA_BASE_URL
  * ICA_ACCESS_TOKEN
  * PIERIANDX_BASE_URL
  * PIERIANDX_INSTITUTION
  * PIERIANDX_AWS_REGION
  * PIERIANDX_AWS_S3_PREFIX
  * PIERIANDX_AWS_ACCESS_KEY_ID
  * PIERIANDX_AWS_SECRET_ACCESS_KEY
  * PIERIANDX_USER_EMAIL
  * PIERIANDX_USER_PASSWORD

optional arguments:
  -h, --help            show this help message and exit
  --ica-workflow-run-ids ICA_WORKFLOW_RUN_IDS
                        List of ICA workflow run IDs (comma separated), if not specified, script will look through the workflow run list for matching patterns
  --accession-json ACCESSION_JSON
                        Path to accession json containing redcap information for sample list
  --accession-csv ACCESSION_CSV
                        Path to accession csv containing redcap information for sample list
  --verbose             Set log level from info to debug
  
example usage:
./cttso-ica-to-pieriandx.py --accession-csv samples.csv
./cttso-ica-to-pieriandx.py --accession-json samples.json
```

### Check Status

```
usage: check-pieriandx-status.py [-h] [--case-ids CASE_IDS] [--case-accession-numbers CASE_ACCESSION_NUMBERS]
                                 [--verbose]

Given a comma-separated list of case accession numbers or case accession ids,
return a list of informatics jobs, the informatics job ids and the status of each.
If both case ids and case accession numbers are provided, an outer-join is performed.

The following environment variables are expected:
  * PIERIANDX_BASE_URL
  * PIERIANDX_INSTITUTION
  * PIERIANDX_USER_EMAIL
  * PIERIANDX_USER_PASSWORD


optional arguments:
  -h, --help            show this help message and exit
  --case-ids CASE_IDS   List of case ids
  --case-accession-numbers CASE_ACCESSION_NUMBERS
                        List of case accession numbers
  --verbose             Set logging level to DEBUG
```

### Download reports

```
usage: download-pieriandx-reports.py [-h] [--case-ids CASE_IDS] [--case-accession-numbers CASE_ACCESSION_NUMBERS]
                                     --output-file OUTPUT_FILE [--pdf] [--json] [--verbose]

Given a comma-separated list of case accession numbers or case accession ids,
download a list of reports to the zip file specified in --output-file
If both case ids and case accession numbers are provided, an outer-join is performed.
Must specify one (and only one) of pdf and json. Parent directory of output file must exist. 
Output file must end in '.zip'.  

The zip file will contain a directory which is the nameroot of the zip file,
The naming convention of the reports is '<case_accession_number>_<report_id>.<output_file_type>'

The following environment variables are expected:
  * PIERIANDX_BASE_URL
  * PIERIANDX_INSTITUTION
  * PIERIANDX_USER_EMAIL
  * PIERIANDX_USER_PASSWORD

optional arguments:
  -h, --help            show this help message and exit
  --case-ids CASE_IDS   List of case ids
  --case-accession-numbers CASE_ACCESSION_NUMBERS
                        List of case accession numbers
  --output-file OUTPUT_FILE
                        Path to output zip file
  --pdf                 Download reports as pdfs
  --json                Download reports as jsons
```

## Environment variable hints

### ICA_BASE_URL
* Base url to ica endpoint.
* Set to `https://aps2.platform.illumina.com`

### ICA_ACCESS_TOKEN
* The access token for the project context that contains the files on ICA
* Run `ica-context-switcher --scope read-only --project-name <project-name>` 
to add `ICA_ACCESS_TOKEN` to your environment

### PIERIANDX_BASE_URL
* For prod this is `https://app.pieriandx.com/cgw-api/v2.0.0`.  
* For dev this is `https://app.uat.pieriandx.com/cgw-api/v2.0.0`

### PIERIANDX_INSTITUTION
* For prod this is `melbourne`
* For dev this is `melbournetest`

### PIERIANDX_AWS_REGION
* Set to `us-east-1` for both dev and prod accounts

### PIERIANDX_AWS_S3_PREFIX
* Set to `s3://pdx-xfer/melbourne` for prod
* Set to `s3://pdx-cgwxfer-test/melbournetest` for dev

### PIERIANDX_AWS_ACCESS_KEY_ID
* Can be found in Keybase for both dev and prod accounts

### PIERIANDX_AWS_SECRET_ACCESS_KEY
* Can be found in Keybase for both dev and prod accounts

### PIERIANDX_USER_EMAIL
* Your email address used to log in to PierianDx

### PIERIANDX_USER_PASSWORD
* Your password used to log in to PierianDx

## Launching via AWS Lambda:

### Assumptions:
* Assumes you're in the development account (843407916570)
  * You can run `aws sts get-caller-identity` to confirm
  * This will collect the rolling ICA_ACCESS_TOKEN for the development project in ICA when running the batch command.  

* Deployment in production coming soon :construction:
  * This will collect the rolling ICA_ACCESS_TOKEN for the production project in ICA when running the batch command.  

The AWS Lambda call expects two input parameter arguments:
* ica_workflow_run_id
  * This is the ica workflow run id for the cttso workflow
* accession_json_base64_str
  * This is a base64 encoded version of the accession json object
  

### Single sample example
The example below shows an example deployment of the lambda

#### De-identified sample
```bash
#!/usr/bin/env bash

# Set to fail
set -euo pipefail

## Set inputs
ica_workflow_run_id="wfr.55ee9135cd88442b8810d7224c88c03f"
accession_json_base64_str="$(
  jq \
    --raw-output \
    '@base64' <<< \
  '
    {
      "sample_type":"Validation",
      "disease":285645000,
      "is_identified":False,
      "accession_number":"SBJ00001_L2100001",
      "study_id":"Validation",
      "participant_id":"SBJ00001",
      "specimen_type":119361006,
      "external_specimen_id":"pDNA_Super_1085",
      "date_accessioned":"2021-10-04t09:00:00+1000",
      "date_collected":"2021-10-02t09:00:00+1000",
      "date_received":"2021-10-03t09:00:00+1000",
    }
  ' \
)"

# Get payload
payload="$(jq --raw-output \
              --arg ica_workflow_run_id "${ica_workflow_run_id}" \
              --arg accession_json_base64_str "${accession_json_base64_str}" \
              '{
                 parameters: {
                   ica_workflow_run_id: $ica_workflow_run_id,
                   accession_json_base64_str: $accession_json_base64_str
                 }
               }' <<< {})"

# Call lambda - write output to stdout
aws lambda invoke \
  --cli-binary-format "raw-in-base64-out" \
  --function-name "ctTSOICAToPierianDx_batch_lambda" \
  --payload "${payload}" \
  /dev/stdout
```

#### Identified sample

```bash
#!/usr/bin/env bash

# Set to fail
set -euo pipefail

## Set inputs
ica_workflow_run_id="wfr.55ee9135cd88442b8810d7224c88c03f"
accession_json_base64_str="$(
  jq \
    --raw-output \
    '@base64' <<< \
  '
    {      
      "sample_type": "Validation",
      "disease": 285645000,
      "is_identified": True,
      "accession_number": "SBJ00001_L2100001",
      "specimen_type": 119361006,
      "external_specimen_id": "pDNA_Super_1085.1",
      "date_accessioned": "2021-10-04t09:00:00+1000",
      "date_collected": "2021-10-02t09:00:00+1000",
      "date_received": "2021-10-03t09:00:00+1000",
      "date_of_birth": "2021-10-04t09:00:00+1000",
      "first_name": "John",
      "last_name": "Doe",
      "mrn": "pDNA_Super_1085",
      "hospital_number": "99",
      "facility": "PeterMac",
      "requesting_physicians_first_name": "Dr",
      "requesting_physicians_last_name": "DoLittle"
    }
  ' \
)"

# Get payload
payload="$(jq --raw-output \
              --arg ica_workflow_run_id "${ica_workflow_run_id}" \
              --arg accession_json_base64_str "${accession_json_base64_str}" \
              '{
                 parameters: {
                   ica_workflow_run_id: $ica_workflow_run_id,
                   accession_json_base64_str: $accession_json_base64_str
                 }
               }' <<< {})"

# Call lambda - write output to stdout
aws lambda invoke \
  --cli-binary-format "raw-in-base64-out" \
  --function-name "ctTSOICAToPierianDx_batch_lambda" \
  --payload "${payload}" \
  /dev/stdout
```

## Accession CSV format reference

The accession csv will have the following columns (all columns are reduced to lower cases with spaces converted to underscores):
* Sample Type / SampleType / sample_type
  * One of `[ 'patientcare', 'clinical_trial', 'validation', 'proficiency_testing' ]`
* Indication / indication
  * Generally the disease name
* Disease / disease / disease_id (alternatively use 'disease_name')
  * The SNOMED disease id
* Disease Name / DiseaseName / disease_name  # Optional (you can just set the 'disease_id' instead)
  * The SNOMED disease name
* Is Identified? / is_identified?
  > Deprecated - always set to false anyway.
  * `True | False`
* Requesting Physicians First Name / requesting_physicians_first_name  # Optional (not used)
  * First name of the requesting physician 
* Requesting Physicians Last Name / requesting_physicians_last_name  # Optional (not used)
  * Last name of the requesting physician 
* Accession Number / accession_number
  * The Case Accession Number, should be `<subject_id>_<library_id>`
  * i.e `SBJ00123_L2100456`
* Specimen Label / specimen_label:  # Optional
  * Mapping to the panel's specimen scheme
  * Default is `primarySpecimen`
* Specimen Type / SpecimenType / specimen_type  # Optional (alternatively use 'specimen_type_name')
  * The specimen type SNOWMED id
* Specimen Type Name / specimen_type_name  # Optional (alternative use 'specimen_type')
  * The specimen type SNOWMED name
* External Specimen ID / external_specimen_id
  * The external specimen ID
* Date Accessioned / date_accessioned
  * Date Time string in UTC time
* Date collected
  * Date Time string in UTC time
* Date Received
  * Date Time string in UTC time
* Gender  # Optional (default "unknown")
  * One of `[ unknown, male, female, unspecified, other, ambiguous, not_applicable ]`

### De-identified specific options
* Study ID / StudyID / study_id
  * Could be the name of the study
  * Leave blank and falls back to sample type attribute?
* Participant ID / ParticipantID / participant_id
  * The subject ID
  * Leave blank and falls back to accession number

### Identified specific options
* Date Of Birth:
  * Patient's Date of birth, we just set this as the current date
* First Name:
  * Patient's first name, set always to John or Jane
* Last Name:
  * Patient's last name, set always to Doe
* Mrn
  * Patient's medical record number
* Hospital Number
  * Patient's hospital number (set to 99 currently)
* Institution
  * Institution of clinician
* Requesting Physicians First Name
  * First name of Clinician
* Requesting Physicians Last Name
  * Last nameof Clinician

### Unused options
* Ethnicity # Optional (default "unknown")
  * One of `[ hispanic_or_latino, not_hispanic_or_latino, not_reported, unknown ]`
* Race       # Optional (default "unknown")
  * One of `[ american_indian_or_alaska_native, asian, black_or_african_american, native_hawaiian_or_other_pacific_islander, not_reported, unknown, white ]`
* Medical Record Numbers / medical_record_numbers  # Optional (not used) 
* Hospital Numbers / hospital_numbers  # Optional (not used)
* Usable MSI Sites / usable_msi_sites  # Optional (not used)
* Tumor Mutational Burden (Mutations/Mb) / tumor_mutational_burden_mutations_per_mb  # Optional (not used)
* Percent Unstable Sites / percent_unstable_sites  # Optional (not used)
* Percent Tumor Cell Nuclei in the Selected Areas / percent_tumor_cell_nuclei_in_the_selected_areas  # Optional (not used)
