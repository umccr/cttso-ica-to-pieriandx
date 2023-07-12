# CtTSO-ICA-To-PierianDx Changelog

Changes in this log refer only to changes that make it to the 'main' branch. and

For changes in deployment, please see the [deployment changelog](deploy/cttso-ica-to-pieriandx-cdk/Changelog.md) 

## 2023-07-10

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Bugfix

* Re-raise ValueError after catching ValueError in .item() (https://github.com/umccr/cttso-ica-to-pieriandx/pull/108)
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/97
  * Resolves https://github.com/umccr/cttso-ica-to-pieriandx/issues/98

## 2023-05-07

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Hotfix

Force pandas downgrade (https://github.com/umccr/cttso-ica-to-pieriandx/pull/55)
 * Fixes https://github.com/umccr/cttso-ica-to-pieriandx/issues/54

## 2023-02-15

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Updates

* Upgraded Docker container base image (https://github.com/umccr/cttso-ica-to-pieriandx/pull/35)
  * Fixes (https://github.com/umccr/cttso-ica-to-pieriandx/issues/33)

## 2023-01-23

> Author: Alexis Lucattini
> Email: [Alexis.Lucattini@ummcr.org](mailto:alexis.lucattini@umccr.org)

### Updates

* Updated panel from VALIDATION, PATIENTCARE names to MAIN, SUBPANEL respectively (https://github.com/umccr/cttso-ica-to-pieriandx/pull/19)
  * This change better reflects the flexibility between panel and sample types.  
  * Some clinical samples or research samples may need to go through the main panel
  * We may also wish to test the subpanel with validation samples
  

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