# Deployment Changelog

Changes in this log refer only to changes that make it to the 'main' branch and
are nested under deploy/cttso-ica-to-pieriandx-cdk.  

## 2023-10-18

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)


### Bugfixes

* Dont resubmit deleted samples - also remove deleted samples from the lims sheet (https://github.com/umccr/cttso-ica-to-pieriandx/pull/151)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/146 
* Use loc over iloc for pandas selection (https://github.com/umccr/cttso-ica-to-pieriandx/pull/154)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/153
* Replace mrn with external_subject_id for list of required fields (https://github.com/umccr/cttso-ica-to-pieriandx/pull/159)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/158


## 2023-10-18

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Enhancements

* Move to project owner / project name mapping logic (https://github.com/umccr/cttso-ica-to-pieriandx/pull/141)
  * And restructure LIMS sheet
  * Diagram also updated
  * Resolves:
    * https://github.com/umccr/cttso-ica-to-pieriandx/issues/131
    * https://github.com/umccr/cttso-ica-to-pieriandx/issues/132
    * https://github.com/umccr/cttso-ica-to-pieriandx/issues/134
    * https://github.com/umccr/cttso-ica-to-pieriandx/issues/135

* Add deleted sheet (https://github.com/umccr/cttso-ica-to-pieriandx/pull/140)
  * All cases assigned to user ToBe Deleted, are moved to a separate sheet



## 2023-08-13

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

* Updated submission columns

### Hot fixes

* Updated submission column [#124](https://github.com/umccr/cttso-ica-to-pieriandx/pull/124) (fixes [#123](https://github.com/umccr/cttso-ica-to-pieriandx/issues/123))
* Update is_validation logic [#127](https://github.com/umccr/cttso-ica-to-pieriandx/pull/127) (fixes [#126](https://github.com/umccr/cttso-ica-to-pieriandx/issues/126)) 

## 2023-08-09

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

* Improved diagram for automation pathway

### Bug fixes

* Make sure RedCap Sample Type is used if RedCap Entry is not null
* Ensure Lambda Pathways are as expected on the diagram
  * GLIMS ProjectName is set to _Validation_ or _Control_
  * **OR**
    * Sample NOT in RedCap
    * **AND**
    * GLIMS Workflow is set to 'Research'

## 2023-07-31

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### HotFix

* Fix submission time issue when merge adds suffixes to columns ([#118](https://github.com/umccr/cttso-ica-to-pieriandx/pull/118))
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/116

### Bugfix

* Fix cancelled cases when workflow is still NA (so needs an update) ([#117](https://github.com/umccr/cttso-ica-to-pieriandx/pull/117))
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/115

## 2023-07-17

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Hot fixes

* Fixed issue where collection of pieriandx submission time resulted in error when pieriandx case id was null (https://github.com/umccr/cttso-ica-to-pieriandx/pull/112)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/111

### Bug fixes

* Fixed issue where changelog for main changelog was being skipped (https://github.com/umccr/cttso-ica-to-pieriandx/pull/112)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/110


## 2023-07-10

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Bug fixes

* Gh Actions 'Check Changelog' was not using the correct attributes (https://github.com/umccr/cttso-ica-to-pieriandx/pull/96)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/94
* Check comparing portal workflow run end date and pieriandx submission date should be done in EST time (https://github.com/umccr/cttso-ica-to-pieriandx/pull/104)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/101
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/100
  * This issue was likely the cause of many 'duplicate' submissions
* Update the pieriandx_submission_time column using processing_df (https://github.com/umccr/cttso-ica-to-pieriandx/pull/103)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/102
* Use subject id, library id and portal run id to submission time from lims df (https://github.com/umccr/cttso-ica-to-pieriandx/pull/106)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/107

### Enhancements

* Use pieriandx sample type column in LIMS df (https://github.com/umccr/cttso-ica-to-pieriandx/pull/105)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/99


## 2023-06-28

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Enhancements

* Batch Stack Upgrade (https://github.com/umccr/cttso-ica-to-pieriandx/pull/79)
  * Resolves (https://github.com/umccr/cttso-ica-to-pieriandx/issues/76)

* Upgrade CDK to version 2.85.0 (https://github.com/umccr/cttso-ica-to-pieriandx/pull/79)
  * Resolves (https://github.com/umccr/cttso-ica-to-pieriandx/issues/78)

### Fixes

* Lambda utils miscell python file was importing the wrong get_logger function (https://github.com/umccr/cttso-ica-to-pieriandx/pull/82)
  * Resolves (https://github.com/umccr/cttso-ica-to-pieriandx/issues/81)

* Use Job ARN over Job Name for batch submission from lambda (https://github.com/umccr/cttso-ica-to-pieriandx/pull/91)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/90

* Create disable rule policy document as a separate entity (https://github.com/umccr/cttso-ica-to-pieriandx/pull/93)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/92

* Create SSM Get parameter policy as separate entity (https://github.com/umccr/cttso-ica-to-pieriandx/pull/95)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/92

#### Various fixes after migration to new aws-batch-alpha

* https://github.com/umccr/cttso-ica-to-pieriandx/pull/85 
  * https://github.com/umccr/cttso-ica-to-pieriandx/issues/86
* https://github.com/umccr/cttso-ica-to-pieriandx/pull/88
  * https://github.com/umccr/cttso-ica-to-pieriandx/issues/87

## 2023-06-23

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Fixes

* Fix Type Hinting for EventClient Type (https://github.com/umccr/cttso-ica-to-pieriandx/pull/71)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/68
* Don't use RequestResponse event type when manually deploying lambdas (https://github.com/umccr/cttso-ica-to-pieriandx/pull/64)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/63
* Get Incomplete Job Df from Gsuite doesn't collect pending jobs at times (https://github.com/umccr/cttso-ica-to-pieriandx/pull/66
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/61

### Enhancements

* Added GH Actions check to ensure this changelog file has been updated before deployment (https://github.com/umccr/cttso-ica-to-pieriandx/pull/73)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/70

* Added EventBridge kill switch if processing_df contains items included in the update_df (shouldn't happen) (https://github.com/umccr/cttso-ica-to-pieriandx/pull/67)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/62

* Check if new password can successfully generate pieriandx session token before successfully exiting script (https://github.com/umccr/cttso-ica-to-pieriandx/pull/57)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/56

### Dependabot Updates

* Migrated from AWS 2.39.1 to 2.80.0 (https://github.com/umccr/cttso-ica-to-pieriandx/pull/74)

## 2023-04-14

* Prevent NTCs from being uploaded to PierianDx (https://github.com/umccr/cttso-ica-to-pieriandx/pull/52)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/49

* Add new PierianDx Submission Time Column to LIMS Sheet (https://github.com/umccr/cttso-ica-to-pieriandx/issues/47)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/47

* Handle submission edge case of SBJ01666 (https://github.com/umccr/cttso-ica-to-pieriandx/pull/50)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/46
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/48

## 2023-02-15

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Fixes

* Bugfix for redcap_is_complete column in ctTSO LIMS lambda (https://github.com/umccr/cttso-ica-to-pieriandx/pull/23)
  * Fixes (https://github.com/umccr/cttso-ica-to-pieriandx/issues/22)
* Fix issue if cttso lims is empty (from reset) 
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/26
* Panel type needed axis=columns keyword argument
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/27
* submission function needs to consider panel type as an option (https://github.com/umccr/cttso-ica-to-pieriandx/issues/32)
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/37
* Use Workflow column name over ProjectName in GLIMS to determine column type (https://github.com/umccr/cttso-ica-to-pieriandx/issues/24)
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/29
  * Updated flowchart to reflect this change (https://github.com/umccr/cttso-ica-to-pieriandx/pull/30)
* Fix Research Sample not completing in validation pipeline edge case (https://github.com/umccr/cttso-ica-to-pieriandx/issues/38)
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/43
* Fix unhelpful merged row logging error (https://github.com/umccr/cttso-ica-to-pieriandx/issues/39)
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/42
* Removed debug line in portal helpers code (https://github.com/umccr/cttso-ica-to-pieriandx/issues/40)
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/44
* Fixed issue where ctTSO lims was not always collecting the latest job or report (https://github.com/umccr/cttso-ica-to-pieriandx/issues/41)
  * Fixed by https://github.com/umccr/cttso-ica-to-pieriandx/pull/45

### Enhancements

* Allow for 20 lambda submissions per cycle (https://github.com/umccr/cttso-ica-to-pieriandx/pull/28)
* Update lambda layers requirements file with updated boto3 and botocore to allow for new sso config syntax (https://github.com/umccr/cttso-ica-to-pieriandx/pull/25) and (https://github.com/umccr/cttso-ica-to-pieriandx/pull/36)
  * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/34

## 2023-01-23

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Updates

* Updated panel from VALIDATION, PATIENTCARE names to MAIN, SUBPANEL respectively (https://github.com/umccr/cttso-ica-to-pieriandx/pull/19)
  * This change better reflects the flexibility between panel and sample types.  
  * Some clinical samples or research samples may need to go through the main panel
  * We may also wish to test the subpanel with validation samples
* Added glims_is_research column to cttso lims (if glims matches 'research' in the ProjectName column)
  * If glims_is_research is True then a sapmle is processed through the MAIN panel (even if it's set as a clinical sample in RedCap)
* Added panel type column to cttso lims (scraped from pieriandx)
* Added panel_type option to clinical and research payload parameters
  * Default payload for clinical is "SUBPANEL"
  * Default payload for validation is "MAIN"\

## 2022-11-16

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Fixes

* Reduce number of items returned by portal from 1000 to 100 to prevent internal server errors.
* Version control gspread to prevent pip install failure for lambdas
* Reduce number of submissions to pieriandx to reduce timeouts 

## 2022-09-22

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Wake up Lambdas

* Lambdas are woken up before payloads are submitted, documentation added to deployment ReadMe

### Run jobs sequentially

* Set max availble vCPUs to 3 and specify 2 cpus per batch run instance

## 2022-08-30  

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Summary 

* Created a suite of lambdas to improve automatic processing of pieriandx data from ICA
  * Launch validation workflow - takes data from portal and defaults and creates case in PierianDx
  * Create a Google SpreadSheet using APIs from redcap, portal, pieriandx and GLIMS inputs.
    * This can also find available launch payloads and launches them accordingly
    
* Created a token lambda that creates a new pieriandx authentication token every five minutes

* Restructured the pipeline stack to include new lambda and removed some stages outside of the wave due to dependency chains

* Set tags to have key 'Stack' rather than _prefix_-Stack

* Updated user to create cases in PierianDx from alexisl@unimelb.edu to services@umccr.org

* Fixed issue for users wanting to launch redcap payloads manually

* Updated to cdk 2.39.1

