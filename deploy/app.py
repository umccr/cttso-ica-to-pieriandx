#!/usr/bin/env python3
import os
import aws_cdk as cdk
import boto3

from stacks.cttso_ica_to_pieriandx import CttsoIcaToPieriandxStack
from stacks.cttso_docker_codebuild import CttsoIcaToPieriandxDockerBuildStack
from stacks.pipeline_stack import PipelineStack

# Get ssm client
ssm_client = boto3.client('ssm')

# Get ssm parameters
# TODO - move ssm parameters to inside app - use ssm get rather than boto client
ec2_ami = ssm_client.get_parameter(Name='/cdk/cttso-ica-to-pieriandx/batch/ami')['Parameter']['Value']
# rw_bucket = ssm_client.get_parameter(Name='/cdk/cttso-ica-to-pieriandx/batch/rw_buckets')['Parameter']['Value']
image_name = ssm_client.get_parameter(Name='/cdk/cttso-ica-to-pieriandx/batch/docker-image-tag')['Parameter']['Value']

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

# Get codebuild properties
codebuild_props = {
    'repository_source': 'umccr/cttso-ica-to-pieriandx',
    'pipeline_name': 'cttso-ica-to-pieriandx',
    'namespace': 'CttsoIcaToPieriandxDockerBuildStack',
    'container_repo': f'{account_id}.dkr.ecr.{aws_region}.amazonaws.com',
    'codebuild_project_name': 'cttso-ica-to-pieriandx-codebuild',
    'container_name': 'cttso-ica-to-pieriandx',
    'region': aws_region,
    'image_name': image_name
}

batch_props = {
    'namespace': "CttsoIcaToPieriandxStack",
    'compute_env_ami': ec2_ami,  # Should be Amazon ECS optimised Linux 2 AMI
    "image_name": image_name
    # 'rw_bucket': rw_bucket,  # For writing out wrapper script
}

# Get pipeline stack
PipelineStack(app, codepipeline_props.get("namespace"),
              stack_name=codepipeline_props.get("namespace").lower(),
              props=codepipeline_props,
              env=aws_env)


# Get synth
app.synth()
