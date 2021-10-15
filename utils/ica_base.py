#!/usr/bin/env python3

"""
Very low level ica functions
"""
from os import environ
from utils.logging import get_logger

logger = get_logger()


def get_configuration(configuration_import):
    # Get url and token
    ica_base_url = get_base_url_env_var()
    ica_access_token = get_access_token_env_var()

    # Return configuration
    return configuration_import(
        host=ica_base_url,
        api_key_prefix={
            "Authorization": "Bearer"
        },
        api_key={
            'Authorization': ica_access_token
        }
    )


def get_base_url_env_var() -> str:
    """
    Check the ica base url env var
    :return:
    """
    # Get env vars
    ica_base_url = environ.get("ICA_BASE_URL", None)

    # Check env vars
    if ica_base_url is None:
        logger.error("Please ensure the following environment variables are set: 'ICA_BASE_URL'")
        raise ValueError

    return ica_base_url


def get_access_token_env_var() -> str:
    ica_access_token = environ.get("ICA_ACCESS_TOKEN", None)

    if ica_access_token is None:
        logger.error("Please ensure the following environment variables are set: 'ICA_ACCESS_TOKEN")
        raise ValueError

    return ica_access_token

