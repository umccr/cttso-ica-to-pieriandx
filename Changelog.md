# CtTSO-ICA-To-PierianDx Changelog

Changes in this log refer only to changes that make it to the 'main' branch. and

For changes in deployment, please see the [deployment changelog](deploy/cttso-ica-to-pieriandx-cdk/Changelog.md) 

## 2022-08-30  

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@umccr.org](mailto:alexis.lucattini@umccr.org)

### Summary 

* Wrapped portal Run ID name collector in try-exception clause
    * Don't want workflow to break if we can't collect the run name

* Added date handlers to be better accessible to times that aren't utc times  

* Use PIERIANDX_USER_AUTH_TOKEN env var instead of PIERIANDX_USER_PASSWORD
  * See deployment changes for more information

* Added PanelType as an enum
  * Validation samples continue to go through "tso500_ctDNA_vcf_workflow_university_of_melbourne"
  * While clinical samples now go through "tso500_ctDNA_vcf_subpanel_workflow_university_of_melbourne"

* Conda env 
  * Updated to pyriandx 0.3.0 to allow for token usage

* Readme
  * Swapped out password for auth_token in environment variables