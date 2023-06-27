import { CfnOutput, Duration, Fn, Stack, StackProps, Tags, Size } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Repository } from "aws-cdk-lib/aws-ecr";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { ContainerImage, LogDriver } from "aws-cdk-lib/aws-ecs";
import {
    AllocationStrategy,
    ManagedEc2EcsComputeEnvironment,
    EcsEc2ContainerDefinition,
    EcsJobDefinition,
    JobQueue,
    HostVolume
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
    LaunchTemplateAttributes,
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
                machineImage: MachineImage.genericLinux(
                    {
                        [this.region]: compute_env_ami
                    },
                ),
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

        // We generate launch attributes to be parsed into the compute environment
        // under launch template
        const launch_template_attributes: LaunchTemplateAttributes = {
          launchTemplateId: launch_template.launchTemplateId,
          launchTemplateName: launch_template.launchTemplateName,
          versionNumber: launch_template.versionNumber,
        };


        // Set compute environment
        const compute_environment = new ManagedEc2EcsComputeEnvironment(
            this,
            `${props.stack_prefix}-batch-compute-env`,
            {
                serviceRole: batch_service_role,
                allocationStrategy: AllocationStrategy.BEST_FIT_PROGRESSIVE,
                maxvCpus: 3,
                minvCpus: 0,
                launchTemplate: LaunchTemplate.fromLaunchTemplateAttributes(
                  this,
                  `${props.stack_prefix}-ec2-from-launch-template-attributes`,
                   launch_template_attributes
                ),
                instanceRole: Role.fromRoleName(
                    this,
                    `${props.stack_prefix}-batch-instance-role-from-role-name`,
                    batch_instance_role.roleName
                ),
                vpc: vpc,
                vpcSubnets: {
                    subnetType: SubnetType.PUBLIC,
                    availabilityZones: this.availabilityZones
                },
                securityGroups: [
                    batch_security_group
                ]
            }
        )

        // Add tags to compute environment
        let compute_environment_tags = {
            "Creator": "Batch",
            "Stack": props.stack_prefix,
            "Name": "BatchWorker"
        }

        for (let [key, value] of Object.entries(compute_environment_tags)) {
            Tags.of(compute_environment).add(
                key, value
            )
        }

        // Initialise job queue
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

        // Create volumes to be mounted into container definition
        const work_volume = new HostVolume({
            containerPath: "/work",
            readonly: false,
            name: "work",
            hostPath: "/mnt"
        });

        const script_volume = new HostVolume(
            {
                containerPath: "/opt/container",
                readonly: true,
                name: "container",
                hostPath: "/opt/container"
            }
        )

        // Create the ECS Job Definition
        // This wraps the container definition too
        const job_definition = new EcsJobDefinition(
            this,
            `${props.stack_prefix}-ecs-job-definition`,
            {
                parameters: {
                    ["dryrun"]: "-",
                    ["verbose"]: "-"
                },
                container: new EcsEc2ContainerDefinition(
                    this,
                    `${props.stack_prefix}-ecs-container-job-definition`,
                    {
                        image: image_repo,
                        cpu: 2,
                        user: "cttso_ica_to_pieriandx_user:cttso_ica_to_pieriandx_group",
                        memory: Size.mebibytes(1024),
                        command: [
                            "/opt/container/cttso-ica-to-pieriandx-wrapper.sh",
                            "--ica-workflow-run-id", "Ref::ica_workflow_run_id",
                            "--accession-json-base64-str", "Ref::accession_json_base64_str",
                            "Ref::dryrun",
                            "Ref::verbose"
                        ],
                        volumes: [
                            script_volume,
                            work_volume
                        ],
                        logging: LogDriver.awsLogs({
                            streamPrefix: "cttso-ica-to-pieriandx"
                        })
                    },
                ),
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

        // FIXME a workaround for now
        let submit_job_permission_array = [
            "arn", "aws", "batch",
            env.region, env.account,
            "job-definition/" + "cttsoicatopieriandx" + "*"
        ]

        // Set up policy for submitting job to batch
        lambda_role.addToPolicy((
            new PolicyStatement({
                actions: [
                    "batch:SubmitJob"
                ],
                resources: [
                    // Weird cloudformation template developed
                    // https://github.com/aws/aws-cdk/issues/26128
                    // job_definition.jobDefinitionArn,
                    // Instead just all batch (which is like one anyway)
                    submit_job_permission_array.join(":"),
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
                    "VCPUS": "2"
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
