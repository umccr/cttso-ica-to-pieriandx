# ctTSO ICA to PierianDX

This is a quick intermediary piece of software designed to be one-day obsolete.  That day will be a good day.  

In the meantime we can take a bulk list of samples in csv format,  
find the matching secondary analysis runs on ICA by matching up the library ids,  
pull down the data from ICA and transfer over to PierianDx's s3 bucket.  

The script then creates a case, sequencing run and informatics job on PierianDx.  

## Command usage

### ctTSO ICA to PierianDx

```
usage: cttso-ica-to-pieriandx.py [-h] [--ica-workflow-run-ids ICA_WORKFLOW_RUN_IDS] [--accession-json ACCESSION_JSON]
                                 [--accession-csv ACCESSION_CSV] [--verbose]

Given an input.json file, pull information from gds, upload to s3 for a single sample,
 create a case, run and informatics job # Not yet supported

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
./cttso-ica-to-pieriandx.py --accession-json samples.json  # Not yet supported
```

## Check Status

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

## Download reports

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
* Is Identified?
  > Deprecated - always set to false anyway.
  * `True | False`
* Requesting Physicians First Name / requesting_physicians_first_name  # Optional (not used)
  * First name of the requesting physician 
* Requesting Physicians Last Name / requesting_physicians_last_name  # Optional (not used)
  * Last name of the requesting physician 
* Accession Number / accession_number
  * The Case Accession Number, should be `<subject_id>_<library_id>`
  * i.e `SBJ00123_L2100456`
* Study ID / StudyID / study_id
  * Could be the name of the sample or 'Validation'?
* Participant ID
  * The subject ID
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