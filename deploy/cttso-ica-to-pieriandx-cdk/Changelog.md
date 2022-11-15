# Deployment Changelog

Changes in this log refer only to changes that make it to the 'main' branch and
are nested under deploy/cttso-ica-to-pieriandx-cdk.  

## 2022-11-16

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Fixes

* Reduce number of items returned by portal from 1000 to 100 to prevent internal server errors.

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

