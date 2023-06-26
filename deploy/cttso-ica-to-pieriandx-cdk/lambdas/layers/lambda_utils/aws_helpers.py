#!/usr/bin/env python3

"""
General AWS and boto3 helper functions
"""

from botocore.client import BaseClient
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from mypy_boto3_events.client import EventBridgeClient
from typing import Union
import boto3


def get_boto3_session() -> boto3.Session:
    """
    Get a regular boto3 session
    :return:
    """
    return boto3.session.Session()


def get_aws_region() -> str:
    """
    Get AWS region using boto3
    :return:
    """
    boto3_session = get_boto3_session()
    return boto3_session.region_name


def get_boto3_lambda_client() -> Union[LambdaClient, BaseClient]:
    return boto3.client("lambda")


def get_boto3_ssm_client() -> Union[SSMClient, BaseClient]:
    return boto3.client("ssm")


def get_boto3_secretsmanager_client() -> Union[SecretsManagerClient, BaseClient]:
    return boto3.client("secretsmanager")


def get_boto3_events_client() -> Union[EventBridgeClient, BaseClient]:
    return boto3.client("events")
