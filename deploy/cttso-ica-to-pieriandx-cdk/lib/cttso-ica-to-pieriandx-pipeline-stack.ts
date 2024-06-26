import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {REPO_NAME, DEPLOYMENT_DIR, ECR_REPOSITORY_NAME} from "../constants";
import {StringParameter} from "aws-cdk-lib/aws-ssm";
import {pipelines} from "aws-cdk-lib";
import {ManagedPolicy, PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import {CttsoIcaToPieriandxBatchStage} from "./cttso-ica-to-pieriandx-batch-stage"
import { LinuxBuildImage } from "aws-cdk-lib/aws-codebuild";
import { CodeBuildStep } from "aws-cdk-lib/pipelines";
import {CttsoIcaToPieriandxRedcapLambdaStage} from "./cttso-ica-to-pieriandx-redcap-lambda-stage";
import {CttsoIcaToPieriandxValidationLambdaStage} from "./cttso-ica-to-pieriandx-validation-lambda-stage";
import {CttsoIcaToPieriandxLimsMakerLambdaStage} from "./cttso-ica-to-pieriandx-lims-make-stage";


interface CttsoIcaToPieriandxPipelineStackProps extends StackProps {
    stack_prefix: string
    github_branch_name: string
    aws_account_id: string
    aws_region: string
    stack_suffix: string
}


export class CttsoIcaToPieriandxPipelineStack extends Stack {
    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxPipelineStackProps) {
        super(scope, id, props);

        // Step one, collect the codestar arn
        const codestar_arn = StringParameter.valueFromLookup(
            this,
            "codestar_github_arn"
        )

        // Get the codestar connection
        const codestar_connection = pipelines.CodePipelineSource.connection(REPO_NAME, props.github_branch_name, {
                connectionArn: codestar_arn
            }
        )

        // Collect the commit id - to check if there exists a tag
        const commit_id: string = codestar_connection.sourceAttribute("CommitId")

        // Step two, generate pipeline
        // Much taken from https://github.com/umccr/holmes/blob/main/holmes-pipeline-stack.ts#L38
        // Credit A. Patterson
        const pipeline = new pipelines.CodePipeline(this, props.stack_prefix + "-pipeline", {
            dockerEnabledForSynth: true,
            dockerEnabledForSelfMutation: true,
            synth: new pipelines.CodeBuildStep("Synth", {
                input: codestar_connection,
                commands: [
                    `cd ${DEPLOYMENT_DIR}`,
                    "npm ci",
                    "npx cdk synth"
                ],
                rolePolicyStatements: [
                    new PolicyStatement({
                        actions: ["sts:AssumeRole"],
                        resources: ["*"]
                    })
                ],
                // Since we did a cd to get into cdk directory we need to set the primary output directory
                // https://github.com/aws/aws-cdk/issues/9996#issuecomment-949329402
                primaryOutputDirectory: `${DEPLOYMENT_DIR}/cdk.out`
            }),
            crossAccountKeys: true
        })

        // Create manual approval step in a wave
        if ( props.stack_suffix === "prod") {
            const wave_pre_steps = []  // Default, if stack suffix is prod we add a Manual Approval Step
            wave_pre_steps.push(
                new pipelines.ManualApprovalStep("PromoteToProd")
            )
            const manual_approval_stage_wave = pipeline.addWave(
                "manual-approval-wave",
                {
                    pre: wave_pre_steps,
                }
            )
        }

        // First stage is the batch stage (which generates an ssm parameter required for downstream steps
        // Generate the batch stage
        const batch_stage = new CttsoIcaToPieriandxBatchStage(this, props.stack_prefix + "-BatchStage", {
            stack_prefix: `${props.stack_prefix}-batch-stack`,
            env: {
                account: props.aws_account_id,
                region: props.aws_region
            },
            stack_suffix: props.stack_suffix
        })


        // Add the batch stage to the pipeline
        pipeline.addStage(
            batch_stage
        )

        // Create wave for lambda stacks
        const pipeline_lambdas_wave = pipeline.addWave(
            "build-lambdas-wave",
        )

        const redcap_lambda_stage = new CttsoIcaToPieriandxRedcapLambdaStage(this, props.stack_prefix + "-RedCapLambdaStage", {
            stack_prefix: `${props.stack_prefix}-redcap-lambda-stack`,
            env: {
                account: props.aws_account_id,
                region: props.aws_region
            },
            stack_suffix: props.stack_suffix
        })

        // Add the redcap stage to the pipeline wave
        pipeline_lambdas_wave.addStage(
            redcap_lambda_stage
        )

        // Add the validation stage to the pipeline wave
        const validation_lambda_stage = new CttsoIcaToPieriandxValidationLambdaStage(this, props.stack_prefix + "-ValidationLambdaStage", {
            stack_prefix: `${props.stack_prefix}-validation-lambda-stack`,
            env: {
                account: props.aws_account_id,
                region: props.aws_region
            },
            stack_suffix: props.stack_suffix
        })

        // Add the validation lambda stage to the pipeline wave
        pipeline_lambdas_wave.addStage(
            validation_lambda_stage
        )

        // Add the launch all available payloads and update cttso lims sheet as a new pipeline wave
        // Create wave for lambda stacks
        // Due to metadata restrictions in dev, this lims is a prod-only component
        if ( props.stack_suffix === "prod") {
            const pipeline_lims_wave = pipeline.addWave(
                "build-lims-wave",
            )
            const lims_maker_lambda_stage = new CttsoIcaToPieriandxLimsMakerLambdaStage(this,
                props.stack_prefix + "-LimsMakerLambdaStage",
                {
                    stack_prefix: `${props.stack_prefix}-lims-maker-lambda-stack`,
                    env: {
                        account: props.aws_account_id,
                        region: props.aws_region
                    },
                    stack_suffix: props.stack_suffix
                }
            )
            pipeline_lims_wave.addStage(
                lims_maker_lambda_stage
            )
        }

        // Add stacks and docker image build docker image as a 'wave'
        // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.pipelines-readme.html#using-docker-image-assets-in-the-pipeline
        const pipeline_build_wave = pipeline.addWave(
            "build-docker-image-wave",
            {
                post: [
                    this.createBuildStage(
                        props.stack_prefix,
                        ECR_REPOSITORY_NAME,
                        props.stack_suffix,
                        commit_id
                    )
                ]
            }
        )
    }

    // Create the build stage
    private createBuildStage(stack_prefix: string, container_name: string, stack_suffix: string, commit_id: string): CodeBuildStep {
        // Set up role for codebuild
        const codebuild_role = new Role(
            this,
            `${stack_prefix}-codebuild-role`,
            {
                assumedBy: new ServicePrincipal("codebuild.amazonaws.com"),
                managedPolicies: [
                    ManagedPolicy.fromAwsManagedPolicyName("AmazonEC2ContainerRegistryFullAccess")
                ],
            }
        )
        return new CodeBuildStep(
            `${stack_prefix}-build-docker-image-codebuild-step`,
            {
                commands: [
                    `bash "./${DEPLOYMENT_DIR}/build-and-deploy-docker-image.sh"`
                ],
                buildEnvironment: {
                    buildImage: LinuxBuildImage.STANDARD_5_0,
                    privileged: true
                },
                env: {
                    ["CONTAINER_REPO"]: `${this.account}.dkr.ecr.${this.region}.amazonaws.com`,
                    ["CONTAINER_NAME"]: container_name,
                    ["REGION"]: this.region,
                    ["STACK_SUFFIX"]: stack_suffix,
                    ["GIT_COMMIT_ID"]: commit_id
                },
                role: codebuild_role
            }
        )
    }
}
