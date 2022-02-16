from aws_cdk import (
    aws_ssm as ssm,
    aws_s3 as s3,
    Stack,
    aws_codepipeline as codepipeline,
    pipelines,
    RemovalPolicy,
    Stage,
    aws_iam as iam,
    aws_codepipeline_actions as codepipeline_actions,
    Duration,
    aws_codebuild as codebuild,
    aws_iam as iam,
    aws_ecr as ecr,
    RemovalPolicy,
)

from constructs import Construct
from stacks.cttso_ica_to_pieriandx import CttsoIcaToPieriandxStack
from stacks.cttso_docker_codebuild import CttsoIcaToPieriandxDockerBuildStack

from pathlib import Path
from typing import Dict

# As semver dictates: https://regex101.com/r/Ly7O1x/3/
semver_tag_regex = '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$'

"""
Much of this work is from this stackoverflow answer: https://stackoverflow.com/a/67864008/6946787 
"""


# class CttsoIcaToPieriandxDockerBuildStage(Stage):
#     def __init__(self, scope: Construct, construct_id: str, code_pipeline_source, **kwargs) -> None:
#         props = kwargs.pop("props")
#         super().__init__(scope, construct_id, **kwargs)
#
#         # Create stack defined on stacks folder
#         CttsoIcaToPieriandxDockerBuildStack(
#             self,
#             "CttsoIcaToPieriandx",
#             stack_name="cttso-ica-to-pieriandx-docker-build-stack",
#             props=props,
#             code_pipeline_source=code_pipeline_source,
#             env=kwargs.get("env")
#         )


class CttsoIcaToPieriandxStage(Stage):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        props = kwargs.pop("props")
        super().__init__(scope, construct_id, **kwargs)

        # Create stack defined on stacks folder
        CttsoIcaToPieriandxStack(
            self,
            "CttsoIcaToPieriandx",
            stack_name="cttso-ica-to-pieriandx-stack",
            props=props,
            env=kwargs.get("env")
        )


class PipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, props: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get env
        env = kwargs.get("env")
        account_id = env.get("account")
        aws_region = env.get("region")

        # Set a prefix - rather than writing cttso-ica-to-pieriandx many times
        cdk_attribute_prefix = "ctTSOICAToPierianDx"

        # As taken from https://github.com/umccr/samplesheet-check-frontend
        codestar_arn = ssm.\
            StringParameter.from_string_parameter_attributes(self,
                                                             "codestarArn",
                                                             parameter_name="codestar_github_arn").string_value

        # Load SSM parameter for GitHub repo (Created via Console)
        artifacts_bucket_name = ssm.\
            StringParameter.from_string_parameter_attributes(self,
                                                             "artifactsBucketName",
                                                             parameter_name="/cdk/cttso-ica-to-pieriandx/artifacts/pipeline_artifact_bucket_name").string_value

        # Get branch source
        branch_source = ssm.\
            StringParameter.from_string_parameter_attributes(self,
                                                             "branchSource",
                                                             parameter_name="/cdk/cttso-ica-to-pieriandx/branch_source").string_value

        # Set props for codebuild
        codebuild_props = {
            'namespace': 'CttsoIcaToPieriandxDockerBuildStack',
            'repository_source': 'umccr/cttso-ica-to-pieriandx',
            'pipeline_name': 'cttso-ica-to-pieriandx',
            'container_repo': f'{account_id}.dkr.ecr.{aws_region}.amazonaws.com',
            'codebuild_project_name': 'cttso-ica-to-pieriandx-codebuild',
            'container_name': 'cttso-ica-to-pieriandx',
            'region': aws_region
        }

        # Set props for batch
        batch_props = {
            'namespace': "CttsoIcaToPieriandxStack",
            #'compute_env_ami': ec2_ami,  # Should be Amazon ECS optimised Linux 2 AMI
            #"image_name": image_name
        }

        # Create S3 bucket for artifacts
        pipeline_artifact_bucket = s3.Bucket(
            self,
            "cttso-ica-to-pieriandx-artifact-bucket",
            bucket_name=artifacts_bucket_name,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        # Create the build pipeline
        cttso_ica_to_pieriandx_build_pipeline = codepipeline.Pipeline(
            self,
            "CttsoIcaToPieriandxBuildPipeline",
            artifact_bucket=pipeline_artifact_bucket,
            restart_execution_on_update=True,
            cross_account_keys=False,
            pipeline_name=codebuild_props["pipeline_name"],
        )

        # Create codestar connection fileset
        code_pipeline_source = pipelines.CodePipelineSource.connection(
            repo_string=codebuild_props["repository_source"],
            branch=branch_source,
            connection_arn=codestar_arn
        )

        # Grab code_pipeline_source artifact
        artifact_map = pipelines.ArtifactMap()
        source_artifact = artifact_map.to_code_pipeline(
            x=code_pipeline_source.primary_output
        )

        # Create a pipeline for cdk stack build
        self_mutate_pipeline = pipelines.CodePipeline(
            self,
            "CodePipeline",
            code_pipeline=cttso_ica_to_pieriandx_build_pipeline,
            synth=pipelines.ShellStep(
                "CDKShellScript",
                input=code_pipeline_source,
                commands=[
                    "cdk synth",
                    # "mkdir -p ./cfnnag_output",
                    # "while IFS= read -r -d'' file; do",
                    # "  cp $template ./cfnnag_output/",
                    # "done < <(find ./cdk.out -type f -maxdepth 2 -name '*.template.json')",
                    # "cfn_nag_scan --input-path ./cfnnag_output/"
                ],
                install_commands=[
                    "cd deploy",
                    "npm install -g aws-cdk",
                    "gem install cfn-nag",
                    "pip install -r requirements.txt"
                ],
                primary_output_directory="deploy/cdk.out"
            ),
            code_build_defaults=pipelines.CodeBuildOptions(
                role_policy=[
                    iam.PolicyStatement(
                        actions=["ec2:Describe*", "ec2:Get*"],
                        resources=["*"]
                    )
                ]
            )
        )\

        # ECR repo to push docker container into
        ecr_repo = ecr.Repository(
            self, "ECR",
            repository_name=codebuild_props.get("container_name"),
            removal_policy=RemovalPolicy.DESTROY
        )

        # Define buildspec
        build_spec_object = codebuild.BuildSpec.from_object({
            "version": "0.2",
            "env": {
                "variables": {
                    "CONTAINER_REPO": codebuild_props.get("container_repo"),
                    "CONTAINER_NAME": ecr_repo.repository_name,
                    "REGION": aws_region
                },
            },
            "phases": {
                "install": {
                    "runtime-versions": {
                        "docker": "19",
                        "python": "3.9"
                    }
                },
                "pre_build": {
                    "commands": [
                        # Set DEBIAN_FRONTEND env var to noninteractive
                        "export DEBIAN_FRONTEND=noninteractive",
                        # Update
                        "apt-get update -y -qq"
                        # Install git, unzip and wget
                        "apt-get install -y -qq git unzip wget"
                        # Install aws v2
                        "wget --quiet --output-document \"awscliv2.zip\" \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\"",
                        "unzip -qq \"awscliv2.zip\"",
                        "./aws/install",
                        # Clean up
                        "rm -rf \"awscliv2.zip\" \"aws/\""
                    ]
                },
                "build": {
                    "commands": [
                        # Convenience CODEBUILD VARS, need more? Check https://github.com/thii/aws-codebuild-extras
                        "export CTTSO_ICA_TO_PIERIANDX_GIT_TAG=\"$(git describe --tags --exact-match 2>/dev/null)\"",
                        # Build as latest tag
                        "docker build --tag \"${CONTAINER_REPO}/${CONTAINER_NAME}:latest\" ./",
                        # Also add in tag if applicable
                        "if [ -n \"${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}\" ]; then",
                        "  docker tag \"${CONTAINER_REPO}/${CONTAINER_NAME}:latest\" \"${CONTAINER_REPO}/${CONTAINER_NAME}:${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}\""
                        "fi",
                        "export \"CTTSO_ICA_TO_PIERIANDX_GIT_TAG\"",
                        "echo \"Container version is ${CTTSO_ICA_TO_PIERIANDX_GIT_TAG-latest}\" 1>&2"
                    ]
                },
                "post_build": {
                    "commands": [
                        # Login to aws and push Docker image to ECR
                        "aws ecr get-login-password --region \"${REGION}\" | docker login --username AWS --password-stdin \"${CONTAINER_REPO}\""
                        "docker push \"${CONTAINER_REPO}/${CONTAINER_NAME}:latest\""
                        "if [ -n \"${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}\" ]; then",
                        "  docker push \"${CONTAINER_REPO}/${CONTAINER_NAME}:${CTTSO_ICA_TO_PIERIANDX_GIT_TAG}\""
                        "fi",
                    ]
                }
            }
        })

        # Create PipelineProject
        code_build_project = codebuild.PipelineProject(
            self,
            f"{cdk_attribute_prefix}CodeBuildPipelineProject",
            description="Pipline project from codebuild to build docker container",
            project_name=f"{cdk_attribute_prefix}CodeBuildPipelineProject",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            build_spec=build_spec_object,
            timeout=Duration.hours(3)
        )

        # Tackle IAM permissions
        # https://stackoverflow.com/questions/38587325/aws-ecr-getauthorizationtoken/54806087
        code_build_project.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('AmazonEC2ContainerRegistryPowerUser')
        )

        # For adding container to ssm
        code_build_project.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMFullAccess")
        )

        code_pipeline_build_action = codepipeline_actions.CodeBuildAction(
            input=source_artifact,
            project=code_build_project,
            type=codepipeline_actions.CodeBuildActionType.BUILD,
            action_name="DockerBuildAction"
        )

        # Add batch as a stage
        self_mutate_pipeline.add_stage(
            CttsoIcaToPieriandxStage(
                self,
                "CttsoIcaToPieriandxStage",
                props=batch_props,
                env=env
            )
        )

        # Build pipeline
        self_mutate_pipeline.build_pipeline()

        # Add buildaction as a stage
        self_mutate_pipeline.pipeline.add_stage(
            stage_name="DockerBuildStage",
            actions=[
                code_pipeline_build_action
            ]
        )
