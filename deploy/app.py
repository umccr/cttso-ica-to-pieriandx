#!/usr/bin/env python3
import os
import aws_cdk as cdk
import boto3

from stacks.cttso_ica_to_pieriandx import CttsoIcaToPieriandxStack
from stacks.cttso_docker_codebuild import CttsoIcaToPieriandxDockerBuildStack
from stacks.pipeline_stack import PipelineStack


# Call app
app = cdk.App()

# Use CDK_DEFAULT_ACCOUNT and CDK_DEFAULT_REGION
account_id = os.environ.get('CDK_DEFAULT_ACCOUNT')
aws_region = os.environ.get('CDK_DEFAULT_REGION')
aws_env = {"account": account_id, "region": aws_region}

# Set properties for cdk
codepipeline_props = {
    'namespace': "CttsoIcaToPieriandxPipelineStack"
}

# Get pipeline stack
PipelineStack(app, codepipeline_props.get("namespace"),
              stack_name=codepipeline_props.get("namespace").lower(),
              props=codepipeline_props,
              env=aws_env)


# Get synth
app.synth()
