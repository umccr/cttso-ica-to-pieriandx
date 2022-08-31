#!/usr/bin/env python3

"""
Set of functions to quickly grab arn values
"""

from typing import Dict

from .globals import \
    PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH, \
    REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER, \
    VALIDATION_LAMBDA_FUNCTION_ARN_SSM_PARAMETER_PATH

from .aws_helpers import \
        SSMClient, get_boto3_ssm_client

from .logger import get_logger

logger = get_logger()


def get_lambda_function_arn_from_ssm_parameter(ssm_parameter_path: str) -> str:
    """
    Get function from ssm parameter path
    :param ssm_parameter_path:
    :return:
    """
    # Get lambda function arn
    ssm_client: SSMClient = get_boto3_ssm_client()

    lambda_function_arn_dict: Dict = ssm_client.get_parameter(
        Name=ssm_parameter_path
    )

    # Get the function dict
    lambda_function_arn_dict_parameter: Dict
    if (lambda_function_arn_dict_parameter := lambda_function_arn_dict.get(
            "Parameter", None)) is None:
        logger.error("Could not get Parameter key from ssm value ")
        raise ValueError
    lambda_function_arn_dict_parameter_value: str

    # Get the parameter value
    if (
            lambda_function_arn_dict_parameter_value :=
            lambda_function_arn_dict_parameter.get("Value", None)
    ) is None:
        logger.error("Could not get value key from ssm parameter")
        raise ValueError

    return lambda_function_arn_dict_parameter_value


def get_cttso_ica_to_pieriandx_lambda_function_arn() -> str:
    """
    Get the parameter from the parameter name
    :return:
    """

    return get_lambda_function_arn_from_ssm_parameter(PIERIANDX_LAMBDA_LAUNCH_FUNCTION_ARN_SSM_PATH)


def get_validation_lambda_arn():
    """
    Get the lambda validation launcher
    :return:
    """

    # Get lambda function arn
    return get_lambda_function_arn_from_ssm_parameter(VALIDATION_LAMBDA_FUNCTION_ARN_SSM_PARAMETER_PATH)


def get_clinical_lambda_arn():
    """
    Get the lambda clinical launcher
    :return:
    """

    # Get lambda function arn
    return get_lambda_function_arn_from_ssm_parameter(REDCAP_APIS_FUNCTION_ARN_SSM_PARAMETER)
