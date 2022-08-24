import {
    CfnOutput, Stack, StackProps, Duration
} from 'aws-cdk-lib'
import { Construct } from 'constructs';
import { DockerImageFunction, DockerImageCode } from "aws-cdk-lib/aws-lambda";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Role, ManagedPolicy, ServicePrincipal, PolicyStatement } from "aws-cdk-lib/aws-iam";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { Rule, Schedule } from "aws-cdk-lib/aws-events";
import { LambdaFunction as LambdaFunctionTarget } from "aws-cdk-lib/aws-events-targets"
import {
    SSM_TOKEN_REFRESH_LAMBDA_FUNCTION_ARN_VALUE,
    SECRETS_MANAGER_PIERIANDX_PATH,
    SSM_PIERIANDX_PATH,
} from "../constants";


interface CttsoIcaToPieriandxTokenRefreshLambdaStackProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
}

export class CttsoIcaToPieriandxTokenRefreshLambdaStack extends Stack {

    public readonly tokenRefreshLambdaFunctionArnOutput: CfnOutput
    public readonly tokenRefreshLambdaFunctionSSMParameterOutput: CfnOutput

    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxTokenRefreshLambdaStackProps) {
        super(scope, id, props)

        // Pull out env parameters from property
        const env = props.env

        // Create role
        const lambda_role = new Role(this,
            `${props.stack_prefix}-LambdaExecutionRole`,
            {
                assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
                roleName: props.stack_prefix + "-lr",
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
                description: "Token Refresher every thirty minutes!",
                code: DockerImageCode.fromImageAsset(
                    "./lambdas/token_refresher",
                ),
                role: lambda_role,
                timeout: Duration.seconds(300)
            }
        )

         // Create a schedule for the lambda
        const lambda_schedule_rule = new Rule(
            this,
            props.stack_prefix + "-lf-trig",
            {
                schedule: Schedule.expression("rate(30 minutes)")
            }
        )

        // Add target for lambda schedule
        lambda_schedule_rule.addTarget(
            new LambdaFunctionTarget(
                lambda_function
            )
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

        // Add pieriandx secrets access to lambda policy
        const pieriandx_secrets_path = Secret.fromSecretNameV2(
            this,
            `${props.stack_prefix}-pieriandx-user-password-arn`,
            SECRETS_MANAGER_PIERIANDX_PATH
        ).secretArn

        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                        "secretsmanager:CreateSecret",
                        "secretsmanager:UpdateSecret"
                    ],
                    resources: [
                        `${pieriandx_secrets_path}/*`
                    ]
                }
            )
        )

        // Create the ssm parameter to represent the cttso lambda function
        const ssm_parameter = new StringParameter(
            this,
            props.stack_prefix + "ssm-cdk-lambda-parameter",
            {
                stringValue: lambda_function.functionArn,
                parameterName: SSM_TOKEN_REFRESH_LAMBDA_FUNCTION_ARN_VALUE,
            }
        )

        // Assign values to cfn outputs
        this.tokenRefreshLambdaFunctionArnOutput = new CfnOutput(this, "tokenRefreshLambdaFunctionArn", {
            value: lambda_function.functionArn,
        });

        // Add ssm parameter
        this.tokenRefreshLambdaFunctionSSMParameterOutput = new CfnOutput(this, "tokenRefreshLambdaFunctionSSMParameterArn", {
            value: ssm_parameter.parameterArn,
        });
    }

}