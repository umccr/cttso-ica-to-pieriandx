import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {REPO_NAME, DEPLOYMENT_DIR, ECR_REPOSITORY_NAME} from "../constants";
import {StringParameter} from "aws-cdk-lib/aws-ssm";
import {pipelines} from "aws-cdk-lib";
import {ManagedPolicy, PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import {CttsoIcaToPieriandxCodeBuildStage} from "./cttso-ica-to-pieriandx-build-stage";
import {CttsoIcaToPieriandxBatchStage} from "./cttso-ica-to-pieriandx-batch-stage"
import {Artifact} from "aws-cdk-lib/aws-codepipeline";
import {BuildSpec, LinuxBuildImage} from "aws-cdk-lib/aws-codebuild";
import {CodeBuildStep} from "aws-cdk-lib/pipelines";


interface CttsoIcaToPieriandxPipelineStackProps extends StackProps {
    stack_prefix: string
    github_branch_name: string
    aws_account_id: string
    aws_region: string
}


export class CttsoIcaToPieriandxPipelineStack extends Stack {
    constructor(scope: Construct, id: string, props: CttsoIcaToPieriandxPipelineStackProps) {
        super(scope, id, props);

        // Step one, collect the codestar arn
        const code_star_arn = StringParameter.valueFromLookup(
            this,
            "codestar_github_arn"
        )

        // Step two, generate pipeline
        // Much taken from https://github.com/umccr/holmes/blob/main/holmes-pipeline-stack.ts#L38
        // Credit A. Patterson
        const pipeline = new pipelines.CodePipeline(this, props.stack_prefix + "-pipeline", {
            dockerEnabledForSynth: true,
            dockerEnabledForSelfMutation: true,
            synth: new pipelines.CodeBuildStep("Synth", {
                input: pipelines.CodePipelineSource.connection(REPO_NAME, props.github_branch_name, {
                    connectionArn: code_star_arn
                }),
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

        // Add the build docker image as a 'wave'
        // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.pipelines-readme.html#using-docker-image-assets-in-the-pipeline
        pipeline.addWave(
            "build-docker-image-wave",
            {
                post: [
                    this.createBuildStage(
                        props.stack_prefix,
                        ECR_REPOSITORY_NAME,
                        props.aws_account_id,
                        props.aws_region
                    )
                ]
            }
        )

        // // Generate the build stage
        // const build_stage = new CttsoIcaToPieriandxCodeBuildStage(this, props.stack_prefix + "-CodeBuildStage", {
        //     env: {
        //         account: props.aws_account_id,
        //         region: props.aws_region
        //     },
        //     stack_prefix: props.stack_prefix,
        //     github_branch_name: props.github_branch_name,
        // })
        //
        // // Add the build stage to the pipeline
        // pipeline.addStage(
        //     build_stage
        // )

        // Generate the batch stage
        const batch_stage = new CttsoIcaToPieriandxBatchStage(this, props.stack_prefix + "-BatchStage", {
            env: {
                account: props.aws_account_id,
                region: props.aws_region
            },
            stack_prefix: props.stack_prefix,
        })

        // Add the batch stage to the pipeline
        pipeline.addStage(
            batch_stage
        )
    }

    private createBuildStage(stack_prefix: string, container_name: string, aws_account_id: string, aws_region: string): CodeBuildStep {
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
                    "bash", `./${DEPLOYMENT_DIR}/build-and-deploy-docker-image.sh`
                ],
                buildEnvironment: {
                    buildImage: LinuxBuildImage.STANDARD_5_0,
                    privileged: true
                },
                env: {
                    ["CONTAINER_REPO"]: `${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com`,
                    ["CONTAINER_NAME"]: container_name,
                    ["REGION"]: aws_region
                },
                rolePolicyStatements: [
                    new PolicyStatement({
                        actions: ["sts:AssumeRole"],
                        resources: [`arn:aws:iam::${aws_account_id}:role/*`],
                    }),
                ],
                role: codebuild_role
            }
        )
    }
}
