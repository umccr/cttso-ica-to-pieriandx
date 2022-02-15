from aws_cdk import (
    aws_ssm as ssm,
    aws_s3 as s3,
    Stack,
    aws_codepipeline as codepipeline,
    pipelines,
    RemovalPolicy,
    Stage,

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


class CttsoIcaToPieriandxDockerBuildStage(Stage):
    def __init__(self, scope: Construct, construct_id: str, props: Dict, code_pipeline_source, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create stack defined on stacks folder
        CttsoIcaToPieriandxDockerBuildStack(
            self,
            "CttsoIcaToPieriandx",
            stack_name="cttso-ica-to-pieriandx-stack",
            props=props,
            code_pipeline_source=code_pipeline_source,
            env=kwargs.get("env")
        )


class CttsoIcaToPieriandxStage(Stage):
    def __init__(self, scope: Construct, construct_id: str, props, **kwargs) -> None:
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
    def __init__(self, scope: Construct, construct_id: str, props: Dict, codebuild_props: Dict, batch_props: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get env
        env = kwargs.get("env")

        # Get props for each pipeline stage
        codebuild_props = codebuild_props
        batch_props = batch_props

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

        branch_source = ssm.\
            StringParameter.from_string_parameter_attributes(self,
                                                             "branchSource",
                                                             parameter_name="/cdk/cttso-ica-to-pieriandx/branch_source").string_value

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
                    "mkdir -p ./cfnnag_output",
                    "while IFS= read -r -d'' file; do",
                    "  cp $template ./cfnnag_output/",
                    "done < <(find ./cdk.out -type f -maxdepth 2 -name '*.template.json')",
                    "cfn_nag_scan --input-path ./cfnnag_output/"
                ],
                install_commands=[
                    "cd deploy",
                    "npm install -g aws-cdk",
                    "gem install cfn-nag",
                    "pip install -r requirements.txt"
                ],
                primary_output_directory="deploy/cdk.out"
            )
        )

        self_mutate_pipeline.add_stage(
            CttsoIcaToPieriandxDockerBuildStage(
                self,
                "CttsoIcaToPieriandxDockerBuildStage",
                code_pipeline_source=code_pipeline_source,
                props=codebuild_props,
                env=env
            )
        )

        self_mutate_pipeline.add_stage(
            CttsoIcaToPieriandxStage(
                self,
                "CttsoIcaToPieriandxStage",
                props=batch_props,
                env=env
            )
        )
