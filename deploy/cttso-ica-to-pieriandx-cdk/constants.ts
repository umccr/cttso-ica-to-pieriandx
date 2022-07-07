// Names
export const STACK_PREFIX: string = "cttso-ica-to-pieriandx"

export const DEPLOYMENT_DIR: string = "deploy/cttso-ica-to-pieriandx-cdk"

export const ECR_REPOSITORY_NAME: string = "cttso-ica-to-pieriandx"

// AWS Things
export const AWS_REGION: string = "ap-southeast-2"

export const AWS_BUILD_ACCOUNT_ID: string = "383856791668"
export const AWS_DEV_ACCOUNT_ID: string = "843407916570"
export const AWS_PROD_ACCOUNT_ID: string = "472057503814"

// GitHub things
export const REPO_NAME: string = "umccr/cttso-ica-to-pieriandx"
export const GITHUB_DEV_BRANCH_NAME: string = "dev"
export const GITHUB_PROD_BRANCH_NAME: string = "main"

export const DEV_STACK_SUFFIX:  string = "dev"
export const PROD_STACK_SUFFIX: string = "prod"
export const DATA_PORTAL_API_ID_SSM_PARAMETER: string = "/data_portal/backend/api_id"
export const DATA_PORTAL_API_DOMAIN_NAME_SSM_PARAMETER: string = "/data_portal/backend/api_domain_name"

// Redcap connection things
export const REDCAP_LAMBDA_FUNCTION_SSM_KEY: string = "redcap-apis-lambda-function"
export const SSM_REDCAP_LAMBDA_FUNCTION_ARN_VALUE: string = "redcap-to-pieriandx-lambda-function"
export const SSM_PIERIANDX_PATH: string = "/cdk/cttso-ica-to-pieriandx"
export const SECRETS_MANAGER_PIERIANDX_PATH: string = "PierianDx/UserPassword"

// Output things
export const SSM_LAMBDA_FUNCTION_ARN_VALUE: string = "cttso-ica-to-pieriandx-lambda-function"
