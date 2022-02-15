from aws_cdk import (
    Stack,
    Duration,
    aws_codebuild as codebuild,
    aws_iam as iam,
    aws_ecr as ecr,
    RemovalPolicy,
    pipelines,
    aws_ssm as ssm,
    aws_codepipeline_actions as codepipeline_actions
)

from constructs import Construct
from typing import Dict

# As semver dictates: https://regex101.com/r/Ly7O1x/3/
semver_tag_regex = '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$'

"""
Much of this work is from this stackoverflow answer: https://stackoverflow.com/a/67864008/6946787 
"""


class CttsoIcaToPieriandxDockerBuildStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, props: Dict, code_pipeline_source, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Defining app stage

        # Set a prefix - rather than writing cttso-ica-to-pieriandx many times
        cdk_attribute_prefix = "ctTSOICAToPierianDx"

        code_pipeline_source = code_pipeline_source

        # Grab code_pipeline_source artifact
        artifact_map = pipelines.ArtifactMap()
        source_artifact = artifact_map.to_code_pipeline(
            x=code_pipeline_source.primary_output
        )

        # ECR repo to push docker container into
        ecr_repo = ecr.Repository(
            self, "ECR",
            repository_name=props.get("container_name"),
            removal_policy=RemovalPolicy.DESTROY
        )

        # Define buildspec
        build_spec_object = codebuild.BuildSpec.from_object({
            "version": "0.2",
            "env": {
                "variables": {
                    "CONTAINER_REPO": props.get("container_repo"),
                    "CONTAINER_NAME": ecr_repo.repository_name,
                    "REGION": props.get("region")
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

        # Create a build-action for the project
        codepipeline_actions.CodeBuildAction(
            input=source_artifact,
            project=code_build_project,
            type=codepipeline_actions.CodeBuildActionType.BUILD,
            action_name="DockerBuildAction"
        )
