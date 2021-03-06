import { CfnOutput, Duration, Fn, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Repository } from "aws-cdk-lib/aws-ecr";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { ContainerImage } from "aws-cdk-lib/aws-ecs";
import {
    AllocationStrategy,
    ComputeEnvironment,
    ComputeResourceType,
    JobDefinition,
    JobQueue
} from "@aws-cdk/aws-batch-alpha";
import {
    CfnInstanceProfile,
    CompositePrincipal,
    ManagedPolicy,
    PolicyStatement,
    Role,
    ServicePrincipal
} from "aws-cdk-lib/aws-iam";
import {
    BlockDeviceVolume,
    EbsDeviceVolumeType,
    LaunchTemplate,
    MachineImage,
    SecurityGroup,
    SubnetType,
    UserData,
    Vpc
} from "aws-cdk-lib/aws-ec2";
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import { readFileSync } from "fs";
import { Runtime, Function as LambdaFunction, Code } from "aws-cdk-lib/aws-lambda";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";

import {
    ECR_REPOSITORY_NAME,
    REDCAP_LAMBDA_FUNCTION_SSM_KEY,
    AWS_BUILD_ACCOUNT_ID,
    AWS_REGION,
    SSM_LAMBDA_FUNCTION_ARN_VALUE,
    DATA_PORTAL_API_ID_SSM_PARAMETER,
    SECRETS_MANAGER_PIERIANDX_PATH,
    SECRETS_MANAGER_ICA_SECRETS_PATH, SSM_PIERIANDX_PATH, DATA_PORTAL_API_DOMAIN_NAME_SSM_PARAMETER
} from "../constants";


interface CttsoIcaToPieriandxBatchStackProps extends StackProps {
    stack_prefix: string
    env: {
        account: string
        region: string
    },
    stack_suffix: string
}


export class CttsoIcaToPieriandxBatchStack extends Stack {

    public readonly BatchJobDefinitionArn: CfnOutput;
    public readonly LambdaFunctionArn: CfnOutput;

    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxBatchStackProps) {

        super(scope, id, props);

        // Pull out env parameters from property
        const env = props.env

        // Get the base ami from ssm
        const compute_env_ami = StringParameter.fromStringParameterName(
                this,
                `${props.stack_prefix}-compute-env-ami`,
                "/cdk/cttso-ica-to-pieriandx/batch/ami"
        ).stringValue

        const docker_image_tag = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-docker-image-tag`,
            "/cdk/cttso-ica-to-pieriandx/batch/docker-image-tag"
        ).stringValue

        // Get the container repo
        const container_registry: string = `arn:aws:ecr:${AWS_REGION}:${AWS_BUILD_ACCOUNT_ID}:repository/${ECR_REPOSITORY_NAME}`

        // Get the image repo
        const image_repo = ContainerImage.fromEcrRepository(
            Repository.fromRepositoryArn(
                this,
                `${props.stack_prefix}-ecr-arn`,
                container_registry
            ),
            docker_image_tag
        )

        // Add batch service role
        const batch_service_role = new Role(
            this,
            `${props.stack_prefix}-batch-service-role`,
            {
                assumedBy: new ServicePrincipal("batch.amazonaws.com"),
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSBatchServiceRole")
                ]
            }
        )

        // Add spot-fleet role
        const spotfleet_role = new Role(
            this,
            `${props.stack_prefix}-ec2-spotfleet-role`,
            {
                assumedBy: new ServicePrincipal("spotfleet.amazonaws.com"),
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonEC2SpotFleetTaggingRole")
                ]
            }
        )

        // Create role for batch instances
        const batch_instance_role = new Role(
            this,
            `${props.stack_prefix}-batch-instance-role`,
            {
                roleName: `${props.stack_prefix}-batch-instance-role`,
                assumedBy: new CompositePrincipal(
                    new ServicePrincipal("ec2.amazonaws.com"),
                    new ServicePrincipal("ec2.amazonaws.com")
                ),
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonEC2RoleforSSM"),
                    ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonEC2ContainerServiceforEC2Role'),
                    ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonECSTaskExecutionRolePolicy')
                ]
            }
        )

        // Add pieriandx secrets access to lambda policy
        // Add pieriandx secrets access to lambda policy
        const pieriandx_secrets_path = Secret.fromSecretNameV2(
            this,
            `${props.stack_prefix}-pieriandx-secrets-arn-prefix`,
            SECRETS_MANAGER_PIERIANDX_PATH
        ).secretArn

        batch_instance_role.addToPolicy(
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

        // Add ICA secrets access to batch instance role
        const ica_secrets_path = Secret.fromSecretNameV2(
            this,
            `${props.stack_prefix}-ica-secrets-path`,
            SECRETS_MANAGER_ICA_SECRETS_PATH
        ).secretArn
        batch_instance_role.addToPolicy(
            new PolicyStatement({
                    actions: [
                        "secretsmanager:GetSecretValue"
                    ],
                    resources: [
                        `${ica_secrets_path}*`
                    ]
                }
            )
        )

        // Get access to PierianDx SSM parameters
        // Add pieriandx ssm access to lambda policy
        const pieriandx_vars_ssm_access_arn_as_array = [
            "arn", "aws", "ssm",
            env.region, env.account,
            "parameter" + SSM_PIERIANDX_PATH + "/*"
        ]

        batch_instance_role.addToPolicy(
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
        batch_instance_role.addToPolicy(
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

        // Get portal api id
        const data_portal_id = StringParameter.fromStringParameterName(
            this,
            `${props.stack_prefix}-data-portal-api-id`,
            DATA_PORTAL_API_ID_SSM_PARAMETER
        ).stringValue

        // Add portal access to batch run policy
        batch_instance_role.addToPolicy(
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

        // Turn the instance role into an Instance Profile
        const batch_instance_profile = new CfnInstanceProfile(
            this,
            `${props.stack_prefix}-batch-instance-profile`,
            {
                instanceProfileName: `${props.stack_prefix}-batch-instance-profile`,
                roles: [
                    batch_instance_role.roleName
                ]
            }
        )

        // VPC
        const vpc = Vpc.fromLookup(
            this,
            `${props.stack_prefix}UmccrMainVpc`,
            {
                tags: {
                    "Name": "main-vpc",
                    "Stack": "networking"
                }
            }
        )

        // Batch security group
        const batch_security_group = new SecurityGroup(
            this,
            `${props.stack_prefix}-batchsecuritygroup`,
            {
                vpc: vpc,
                description: "Allow all outbound, no inbound traffic"
            }
        )

        // Create the user data
        const cttso_ica_to_pieriandx_wrapper_asset = new Asset(
            this,
            `${props.stack_prefix}-wrapper-asset`,
            {
                path: "./assets/cttso-ica-to-pieriandx-wrapper.sh"
            }
        )

        // Add read access to batch instance role
        cttso_ica_to_pieriandx_wrapper_asset.grantRead(batch_instance_role)

        // Add cloudwatch asset
        const cw_agent_config_asset = new Asset(
            this,
            `${props.stack_prefix}-cloudwatchagent-config-asset`,
            {
                path: "./assets/cw-agent-config-addon.json"
            }
        )

        // Add read access to batch instance role
        cw_agent_config_asset.grantRead(batch_instance_role)

        // Set up mime asset
        const userdata_mappings = {
            "__S3_WRAPPER_SCRIPT_URL__": `s3://${cttso_ica_to_pieriandx_wrapper_asset.bucket.bucketName}/${cttso_ica_to_pieriandx_wrapper_asset.s3ObjectKey}`,
            "__S3_CWA_CONFIG_URL__": `s3://${cw_agent_config_asset.bucket.bucketName}/${cw_agent_config_asset.s3ObjectKey}`
        }

        // Setup user data
        const userdata_sub = Fn.sub(
            readFileSync("./assets/batch-user-data.sh", 'utf8'),
            userdata_mappings
        )

        // Import substitution object into userdata set
        const userdata = UserData.custom(userdata_sub)

        // Set up mime wrapper
        const mime_wrapper = UserData.custom("MIME-Version: 1.0")

        mime_wrapper.addCommands('Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="')
        mime_wrapper.addCommands('')
        mime_wrapper.addCommands('--==MYBOUNDARY==')
        mime_wrapper.addCommands('Content-Type: text/x-shellscript; charset="us-ascii"')

        // Add user data to mime wrapper
        mime_wrapper.addCommands(userdata.render())

        // Add ending to mime wrapper
        mime_wrapper.addCommands('--==MYBOUNDARY==--')

        // Set Launch Template
        const launch_template = new LaunchTemplate(
            this,
            `${props.stack_prefix}-ec2-launch-template`,
            {
                launchTemplateName: `${props.stack_prefix}-batch-compute-launch-template`,
                userData: mime_wrapper,
                blockDevices: [
                    {
                        deviceName: "/dev/xvdf",
                        volume: BlockDeviceVolume.ebs(
                            16,
                            {
                                volumeType: EbsDeviceVolumeType.GP2,
                                encrypted: true,
                                deleteOnTermination: true
                            }
                        )
                    }
                ]
            }
        )

        // Set compute environment
        const compute_environment = new ComputeEnvironment(
            this,
            `${props.stack_prefix}-batch-compute-env`,
            {
                serviceRole: batch_service_role,
                computeResources: {
                    type: ComputeResourceType.ON_DEMAND,
                    allocationStrategy: AllocationStrategy.BEST_FIT,
                    desiredvCpus: 0,
                    maxvCpus: 5,
                    minvCpus: 0,
                    image: MachineImage.genericLinux(
                        {
                            [this.region]: compute_env_ami
                        },
                    ),
                    launchTemplate: {
                        launchTemplateName: `${props.stack_prefix}-batch-compute-launch-template`,
                        version: launch_template.versionNumber
                    },
                    spotFleetRole: spotfleet_role,
                    instanceRole: batch_instance_profile.instanceProfileName,
                    vpc: vpc,
                    vpcSubnets: {
                        subnetType: SubnetType.PUBLIC,
                        availabilityZones: this.availabilityZones
                    },
                    securityGroups: [
                        batch_security_group
                    ],
                    computeResourcesTags: {
                        "Creator": "Batck",
                        "Stack": props.stack_prefix,
                        "Name": "BatchWorker"
                    }
                }
            }
        )

        const job_queue = new JobQueue(
            this,
            `${props.stack_prefix}-jobqueue`,
            {
                jobQueueName: `${props.stack_prefix}-jobqueue`,
                computeEnvironments: [
                    {
                        computeEnvironment: compute_environment,
                        order: 10
                    }
                ],
                priority: 10
            }
        )

        const job_definition = new JobDefinition(
            this,
            `${props.stack_prefix}-job-definition`,
            {
                jobDefinitionName: `${props.stack_prefix}-job-definition`,
                parameters: {
                    ["dryrun"]: "-",
                    ["verbose"]: "-"
                },
                container: {
                    image: image_repo,
                    vcpus: 1,
                    user: "cttso_ica_to_pieriandx_user:cttso_ica_to_pieriandx_group",
                    memoryLimitMiB: 1024,
                    command: [
                        "/opt/container/cttso-ica-to-pieriandx-wrapper.sh",
                        "--ica-workflow-run-id", "Ref::ica_workflow_run_id",
                        "--accession-json-base64-str", "Ref::accession_json_base64_str",
                        "Ref::dryrun",
                        "Ref::verbose"
                    ],
                    mountPoints: [
                        {
                            containerPath: "/work",
                            readOnly: false,
                            sourceVolume: "work"
                        },
                        {
                            containerPath: "/opt/container",
                            readOnly: true,
                            sourceVolume: "container"
                        }
                    ],
                    volumes: [
                        {
                            name: "container",
                            host: {
                                sourcePath: "/opt/container"
                            }
                        },
                        {
                            name: "work",
                            host: {
                                sourcePath: "/mnt"
                            }
                        }
                    ]
                },
                retryAttempts: 1,
                timeout: Duration.hours(1)
            }
        )

        // Set up job submission lambda
        const lambda_role = new Role(
            this,
            `${props.stack_prefix}-lambda-role`,
            {
                assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
                    ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMReadOnlyAccess")
                ]
            }
        )

        // Set up policy for submitting job to batch
        lambda_role.addToPolicy((
            new PolicyStatement({
                actions: [
                    "batch:SubmitJob"
                ],
                resources: [
                    job_definition.jobDefinitionArn,
                    job_queue.jobQueueArn
                ]
            })
        ))

        // Set up lambda function
        const aws_lambda_function = new LambdaFunction(
            this,
            `${props.stack_prefix}-lambda-function`,
            {
                functionName: `${props.stack_prefix}-lambda-function`,
                handler: "cttso_ica_to_pieriandx.lambda_handler",
                runtime: new Runtime(
                    "python3.9"
                ),
                code: Code.fromAsset(
                    "./lambdas/cttso_ica_to_pieriandx"
                ),
                environment: {
                    "JOBDEF": job_definition.jobDefinitionName,
                    "JOBQUEUE": job_queue.jobQueueName,
                    "JOBNAME_PREFIX": `${props.stack_prefix}_`,
                    "MEM": "1000",
                    "VCPUS": "1"
                },
                role: lambda_role
            }
        )

        // Update the ssm parameter to the new function arn
        const ssm_parameter = new StringParameter(
            this,
            props.stack_prefix + "ssm-cdk-lambda-parameter",
            {
                stringValue: aws_lambda_function.functionArn,
                parameterName: SSM_LAMBDA_FUNCTION_ARN_VALUE,
            }
        )

        // Return the batch arn as an output
        this.BatchJobDefinitionArn = new CfnOutput(this, "BatchJobDefinitionArn", {
            value: job_definition.jobDefinitionArn,
        });

        // Return the lambda function arn as an output
        this.LambdaFunctionArn = new CfnOutput(this, "LambdaFunctionArn", {
            value: aws_lambda_function.functionArn,
        });
    }
}
