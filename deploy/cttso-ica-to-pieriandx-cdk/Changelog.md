# Deployment Changelog

Changes in this log refer only to changes that make it to the 'main' branch and
are nested under deploy/cttso-ica-to-pieriandx-cdk.  

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
