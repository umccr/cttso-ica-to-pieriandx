import {
    CfnOutput, Stack, StackProps, Duration
} from 'aws-cdk-lib'
import { Construct } from 'constructs';
import { DockerImageFunction, DockerImageCode } from "aws-cdk-lib/aws-lambda";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Role, ManagedPolicy, ServicePrincipal, PolicyStatement, Policy } from "aws-cdk-lib/aws-iam";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import {
    DATA_PORTAL_API_ID_SSM_PARAMETER,
    DATA_PORTAL_API_DOMAIN_NAME_SSM_PARAMETER,
    SECRETS_MANAGER_PIERIANDX_PATH,
    SSM_PIERIANDX_PATH,
    SSM_LAMBDA_FUNCTION_ARN_VALUE,
    SSM_LIMS_LAMBDA_FUNCTION_ARN_VALUE,
    SSM_VALIDATION_LAMBDA_FUNCTION_ARN_VALUE,
    SSM_CLINICAL_LAMBDA_FUNCTION_ARN_VALUE,
    GLIMS_SSM_PARAMETER_PATH,
    REDCAP_LAMBDA_FUNCTION_SSM_KEY,
    SSM_LIMS_LAMBDA_FUNCTION_EVENT_RULE_NAME_VALUE
} from "../constants";
import {Rule, Schedule} from "aws-cdk-lib/aws-events";
import { LambdaFunction as LambdaFunctionTarget } from "aws-cdk-lib/aws-events-targets"


interface CttsoIcaToPieriandxLimsMakerLambdaStackProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    }
}

export class CttsoIcaToPieriandxLimsMakerLambdaStack extends Stack {

    public readonly lambdaFunctionArnOutput: CfnOutput
    public readonly lambdaFunctionSSMParameterOutput: CfnOutput

    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxLimsMakerLambdaStackProps) {
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
            props.stack_prefix + "-lf", {
                functionName: props.stack_prefix + "-lf",
                description: "lambda function to create lims sheet and deploy available workflows",
                code: DockerImageCode.fromImageAsset(
                    "./lambdas/",
                    {
                        file: "launch_available_payloads_and_update_cttso_lims_sheet/Dockerfile"
                    }
                ),
                role: lambda_role,
                timeout: Duration.seconds(900),  // Maximum length of lambda duration is 15 minutes
                retryAttempts: 0,  // Never perform a retry if it fails
                memorySize: 2048  // Don't want pandas to kill the lambda
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

        // Get GDrive ssm parameter paths
        const glims_ssm_parameter_path_as_array = [
            "arn", "aws", "ssm",
            env.region, env.account,
            "parameter" + GLIMS_SSM_PARAMETER_PATH
        ]

        // Add access to google lims
        lambda_function.addToRolePolicy((
            new PolicyStatement({
                actions: [
                    "ssm:GetParameter"
                ],
                resources: [
                    glims_ssm_parameter_path_as_array.join(":") + "/*"
                ]
            })
        ))

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

        // Add redcap lambda execution to lambda policy
        // Step 1 is get the resource from SSM
        const redcap_lambda_function_ssm = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-redcap-lambda-function-arn`,
            REDCAP_LAMBDA_FUNCTION_SSM_KEY
        )
        // Step 2: Add ssm to policy
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "ssm:GetParameter"
                    ],
                    resources: [
                        redcap_lambda_function_ssm.parameterArn
                    ]
                }
            )
        )
        // Step 3: Add Invoke Function permission arn
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                actions: [
                    "lambda:GetFunction",
                    "lambda:InvokeFunction"
                ],
                resources: [
                    redcap_lambda_function_ssm.stringValue
                ]
            })
        )

        // Add clinical lambda execution to lambda policy
        // And validation lambda execution to lambda policy
        // Step 1 is get the resource from SSM
        const clinical_lambda_function_ssm = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-redcap-to-pieriandx-function-arn`,
            SSM_CLINICAL_LAMBDA_FUNCTION_ARN_VALUE
        )
        const validation_lambda_function_ssm = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-validation-to-pieriandx-function-arn`,
            SSM_VALIDATION_LAMBDA_FUNCTION_ARN_VALUE
        )
        const cttso_ica_to_pieriandx_ssm = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-ica-to-pieriandx-function-arn`,
            SSM_LAMBDA_FUNCTION_ARN_VALUE
        )
        // Step 2: Add ssm to policy
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                    actions: [
                        "ssm:GetParameter"
                    ],
                    resources: [
                        clinical_lambda_function_ssm.parameterArn,
                        validation_lambda_function_ssm.parameterArn,
                        cttso_ica_to_pieriandx_ssm.parameterArn
                    ]
                }
            )
        )

        // Step 3: Add Invoke Function permission arn
        lambda_function.addToRolePolicy(
            new PolicyStatement({
                actions: [
                    "lambda:GetFunction",
                    "lambda:InvokeFunction"
                ],
                resources: [
                    clinical_lambda_function_ssm.stringValue,
                    validation_lambda_function_ssm.stringValue,
                    cttso_ica_to_pieriandx_ssm.stringValue
                ]
            })
        )

        // Step 4: Add ssm access to get Rule


        // Create a rule to trigger this lambda
        const lambda_schedule_rule = new Rule(
            this,
            props.stack_prefix + "-lf-trig",
            {
                schedule: Schedule.expression("rate(60 minutes)")
            }
        )

        // Create the ssm parameter to represet the cttso lambda function event rule ARN Value
        const ssm_parameter_event_rule = new StringParameter(
            this,
            props.stack_prefix + "ssm-cdk-lambda-event-rule-parameter",
            {
                stringValue: lambda_schedule_rule.ruleName,
                parameterName: SSM_LIMS_LAMBDA_FUNCTION_EVENT_RULE_NAME_VALUE,
            }
        )

        // Add permissions so that lambda function can determine the rule Name that triggers it
        // We need to set this policy statement as its own policy object
        const get_event_rule_policy = new Policy(
            this,
            `${props.stack_prefix}-rule-ssm-get-parameter`,
            {
                statements: [
                    new PolicyStatement({
                        actions: [
                            "ssm:GetParameter"
                        ],
                        resources: [
                            ssm_parameter_event_rule.parameterArn
                        ]
                    })
                ]
            }
        )

        get_event_rule_policy.attachToRole(
            <Role> lambda_function.role
        )

        

        // Add permissions so that lambda function can deactivate its own rule
        // This needs to go through a policy that is added to a role (instead of a role
        // thats added to a policy - https://github.com/aws/aws-cdk/issues/11020
        const disable_rule_policy = new Policy(
            this,
            `${props.stack_prefix}-disable-rule`,
            {
                statements: [
                    new PolicyStatement({
                        actions: [
                            "events:DisableRule"
                        ],
                        resources: [
                            lambda_schedule_rule.ruleArn
                        ]
                    })
                ]
            }
        )

        // Now attach this policy to the role used by the lambda function
        disable_rule_policy.attachToRole(<Role> lambda_function.role)

        // Add target for lambda schedule
        lambda_schedule_rule.addTarget(
            new LambdaFunctionTarget(
                lambda_function
            )
        )

        // Create the ssm parameter to represent the cttso lambda function
        const ssm_parameter = new StringParameter(
            this,
            props.stack_prefix + "ssm-cdk-lambda-parameter",
            {
                stringValue: lambda_function.functionArn,
                parameterName: SSM_LIMS_LAMBDA_FUNCTION_ARN_VALUE,
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