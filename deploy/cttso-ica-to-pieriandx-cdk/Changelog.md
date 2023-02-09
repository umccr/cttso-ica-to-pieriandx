# Deployment Changelog

Changes in this log refer only to changes that make it to the 'main' branch and
are nested under deploy/cttso-ica-to-pieriandx-cdk.  

## 2023-02-10

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Fixes

* Bugfix for redcap_is_complete column in ctTSO LIMS lambda (https://github.com/umccr/cttso-ica-to-pieriandx/pull/23)
  * Fixes (https://github.com/umccr/cttso-ica-to-pieriandx/issues/22)

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

