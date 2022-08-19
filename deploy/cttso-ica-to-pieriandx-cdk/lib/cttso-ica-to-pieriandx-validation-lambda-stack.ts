import {
    CfnOutput, Stack, StackProps, Duration
} from 'aws-cdk-lib'
import { Construct } from 'constructs';
import { DockerImageFunction, DockerImageCode } from "aws-cdk-lib/aws-lambda";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Role, ManagedPolicy, ServicePrincipal, PolicyStatement } from "aws-cdk-lib/aws-iam";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import {
    DATA_PORTAL_API_ID_SSM_PARAMETER, DATA_PORTAL_API_DOMAIN_NAME_SSM_PARAMETER,
    SSM_VALIDATION_LAMBDA_FUNCTION_ARN_VALUE,
    SECRETS_MANAGER_PIERIANDX_PATH, SSM_LAMBDA_FUNCTION_ARN_VALUE,
    SSM_PIERIANDX_PATH,
} from "../constants";


interface CttsoIcaToPieriandxValidationLambdaStackProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
}

export class CttsoIcaToPieriandxValidationLambdaStack extends Stack {

    public readonly validationLambdaFunctionArnOutput: CfnOutput
    public readonly validationLambdaFunctionSSMParameterOutput: CfnOutput

    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxValidationLambdaStackProps) {
        super(scope, id, props)

        // Pull out env parameters from property
        const env = props.env

        // Create role
        const lambda_role = new Role(this,
            `${props.stack_prefix}-LambdaExecutionRole`,
            {
                assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
                roleName: props.stack_prefix + "-lambda-role",
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName(
                        'service-role/AWSLambdaBasicExecutionRole'
                    ),
                    ManagedPolicy.fromAwsManagedPolicyName(
                        'service-role/AWSLambdaVPCAccessExecutionRole'
                    )
                ]
            }
        )

        // Create DockerImage-based lambda Function
        const lambda_function = new DockerImageFunction(
            this,
            props.stack_prefix + "-LF", {
                functionName: props.stack_prefix + "-lf",
                description: "validation sample to cttso submission lambda function deployed using AWS CDK with Docker Image",
                code: DockerImageCode.fromImageAsset(
                    "./lambdas/get_metadata_from_portal_and_validation_and_launch_clinical_workflow",
                ),
                role: lambda_role,
                timeout: Duration.seconds(300),
            }
        )

        // Add pieriandx ssm access to lambda policy
        const pieriandx_vars_ssm_access_arn_as_array = [
            "arn", "aws", "ssm",
            env.region, env.account,
            "parameter" + SSM_PIERIANDX_PATH + "/*"
        ]

        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "ssm:GetParameter"
                    ],
                    resources: [
                        pieriandx_vars_ssm_access_arn_as_array.join(":")
                    ]
                }
            )
        )

        // Get portal api id
        const data_portal_api_domain_name_ssm_parameter_as_array = [
            "arn", "aws", "ssm",
            env.region, env.account,
            "parameter" + DATA_PORTAL_API_DOMAIN_NAME_SSM_PARAMETER
        ]

        // Get access to data portal api domain name ssm parameter
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "ssm:GetParameter"
                    ],
                    resources: [
                        data_portal_api_domain_name_ssm_parameter_as_array.join(":")
                    ]
                }
            )
        )


        // Add pieriandx secrets access to lambda policy
        const pieriandx_secrets_path = Secret.fromSecretNameV2(
            this,
            `${props.stack_prefix}-pieriandx-user-password-arn`,
            SECRETS_MANAGER_PIERIANDX_PATH
        ).secretArn

        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "secretsmanager:GetSecretValue"
                    ],
                    resources: [
                        `${pieriandx_secrets_path}/*`
                    ]
                }
            )
        )

        // Get portal api id
        const data_portal_id = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-data-portal-api-id`,
            DATA_PORTAL_API_ID_SSM_PARAMETER
        ).stringValue

        // Add portal access to lambda policy
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "execute-api:Invoke"
                    ],
                    resources: [
                        `arn:aws:execute-api:${env.region}:${env.account}:${data_portal_id}/*`
                    ]
                }
            )
        )

        // Need to be able to invoke cttso-ica-to-pieriandx-lambda-function value
        // Step 1: Get the resource object
        const pieriandx_launch_function_arn = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-cttso-ica-to-pieriandx-lambda-function-arn`,
            SSM_LAMBDA_FUNCTION_ARN_VALUE
        )

        // Step 2: Add ssm to policy
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "ssm:GetParameter"
                    ],
                    resources: [
                        pieriandx_launch_function_arn.parameterArn
                    ]
                }
            )
        )
        // Step 3: Add invoke function to policy
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                actions: [
                    "lambda:InvokeFunction"
                ],
                resources: [
                    pieriandx_launch_function_arn.stringValue
                ]
            })
        )

        // Create the ssm parameter to represent the cttso lambda function
        const ssm_parameter = new StringParameter(
            this,
            props.stack_prefix + "ssm-cdk-lambda-parameter",
            {
                stringValue: lambda_function.functionArn,
                parameterName: SSM_VALIDATION_LAMBDA_FUNCTION_ARN_VALUE,
            }
        )

        // Assign values to cfn outputs
        this.validationLambdaFunctionArnOutput = new CfnOutput(this, "validationLambdaFunctionArn", {
            value: lambda_function.functionArn,
        });

        // Add ssm parameter
        this.validationLambdaFunctionSSMParameterOutput = new CfnOutput(this, "validationLambdaFunctionSSMParameterArn", {
            value: ssm_parameter.parameterArn,
        });
    }

}