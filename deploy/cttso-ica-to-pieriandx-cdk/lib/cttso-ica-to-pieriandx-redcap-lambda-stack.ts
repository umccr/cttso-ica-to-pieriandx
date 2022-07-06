import {
    CfnOutput, Stack, StackProps, Duration
} from 'aws-cdk-lib'
import { Construct } from 'constructs';
import { DockerImageFunction, DockerImageCode } from "aws-cdk-lib/aws-lambda";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Role, ManagedPolicy, ServicePrincipal, PolicyStatement } from "aws-cdk-lib/aws-iam";
import {
    REDCAP_LAMBDA_FUNCTION_SSM_KEY,
    SECRETS_MANAGER_PIERIANDX_PATH,
    SSM_PIERIANDX_ENV_VARS_PATH,
    SSM_REDCAP_LAMBDA_FUNCTION_ARN_VALUE
} from "../constants";


interface CttsoIcaToPieriandxRedcapLambdaStackProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
}

export class CttsoIcaToPieriandxRedcapLambdaStack extends Stack {

    public readonly lambdaFunctionArnOutput: CfnOutput
    public readonly lambdaFunctionSSMParameterOutput: CfnOutput

    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxRedcapLambdaStackProps) {
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
            props.stack_prefix + "-LambdaFunction", {
                functionName: props.stack_prefix + "-lambda-function",
                description: "redcap to cttso submission lambda function deployed using AWS CDK with Docker Image",
                code: DockerImageCode.fromImageAsset(
                    "./lambda",
                ),
                role: lambda_role,
                timeout: Duration.seconds(300),
            }
        )

        // Add pieriandx ssm access to lambda policy
        const pieriandx_vars_ssm_access_arn_as_array = [
            "arn", "aws", "ssm",
            env.region, env.account,
            "parameter/" + SSM_PIERIANDX_ENV_VARS_PATH + "/*"
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

        // Add pieriandx secrets access to lambda policy
        const secretsmanager_access_arn_as_array = [
            "arn", "aws", "secretsmanager",
            env.region, env.account, "secret",
            SECRETS_MANAGER_PIERIANDX_PATH ]
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "secretsmanager:GetSecretValue"
                    ],
                    resources: [
                        secretsmanager_access_arn_as_array.join(":")
                    ]
                }
            )
        )

        // Add portal access to lambda policy
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "execute-api:Invoke"
                    ],
                    resources: [
                        "*"
                    ]
                }
            )
        )

        // Add redcap lambda execution to lambda policy
        // Step 1 is get the resource from SSM
        const redcap_lambda_function_arn = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-redcap-lambda-function-arn`,
            REDCAP_LAMBDA_FUNCTION_SSM_KEY
        ).stringValue

        lambda_function.addToRolePolicy(
            new PolicyStatement({
                actions: [
                    "lambda:InvokeFunction"
                ],
                resources: [
                    redcap_lambda_function_arn
                ]
            })
        )


        // Create the ssm parameter to represent the cttso lambda function
        const ssm_parameter = new StringParameter(
            this,
            props.stack_prefix + "ssm-cdk-lambda-parameter",
            {
                stringValue: lambda_function.functionArn,
                parameterName: SSM_REDCAP_LAMBDA_FUNCTION_ARN_VALUE,
            }
        )

        // Assign values to cfn outputs
        this.lambdaFunctionArnOutput = new CfnOutput(this, "lambdaFunctionArn", {
            value: lambda_function.functionArn,
        });

        // Add ssm parameter
        this.lambdaFunctionSSMParameterOutput = new CfnOutput(this, "lambdaFunctionSSMParameterArn", {
            value: ssm_parameter.parameterArn,
        });

    }

}