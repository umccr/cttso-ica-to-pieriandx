# Welcome to the CDK V2 for cttso-ica-to-pieriandx in typescript

## Getting started

This AWS CDK project comprises two AWS CodePipeline stacks 'dev' and 'prod' that are both deployed into the UMCCR Bastion account. 

If the 'dev' GitHub branch of this repo is pushed to, the dev codepipeline stack is automatically deployed and updated. 

If the 'main' GitHub branch of this repo is pushed to, the prod codepipeline stack requires a user to press the 'approval' step in the AWS CodePipeline UI. 

## Helpful Pointers

The bulk of the CDK logic resides in the 'lib' directory and is called by the 'bin' directory. 

Code constants are held in _constants.ts_.

AWS SSM Parameters for the dev pipeline stack can be found in _params-dev.json_.

AWS SSM Parameters for the prod pipeline stack can be found in _params-prod.json_.

## Helpful scripts

### scripts/update-params.sh

To update an ssm parameter, edit the respective _params.json_ and log into the appropriate AWS account. 

Run `update-params.sh` in your console and changed ssm parameters will be updated.

### scripts/update-pieriandx-passowrd.sh

The PierianDx password must be updated every three months and is a manual process. 

This requires the user to log in to [app.pieriandx.com](app.pieriandx.com) and update their password. 

Once updated in PierianDx, the user should run the update-pieriandx-password.sh in both the dev and prod accounts.  

This will prompt the user for the new password and will update the AWS secretsmanager respectively.  

### scripts/launch_direct_cttso_lambda_from_glims

To update the direct payloads from Google LIMS this script asks the user to provide a subject ID and library ID
in order to trigger the cttso submission collecting all of the available data from Google LIMS. 

### scripts/launch_redcap_payloads

In order to trigger a pieriandx run the launch redcap payloads script takes in a list of json strings via a file.

An example of the payloads file is as below:

```json lines
{ "subject_id": "SBJ12345", "library_id": "L000123", "ica_workflow_run_id": "wfr.abcdef123456" }
{ "subject_id": "SBJ67890", "library_id": "L000456", "ica_workflow_run_id": "wfr.a1b2c3d4e5f6" }
```


