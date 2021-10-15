#!/usr/bin/env python

"""
Functions to facilitate upload to s3
"""

from os import environ
from utils.logging import get_logger
from utils.errors import S3UploadError

from typing import Dict
from urllib.parse import urlparse
from pathlib import Path
import subprocess

logger = get_logger()


def get_s3_creds_from_environment() -> Dict:
    """
    Looking for the following credentials
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION
    :return:
    """
    # Get aws region
    pieriandx_aws_region = environ.get("PIERIANDX_AWS_REGION", None)

    # Get aws access key id
    pieriandx_aws_access_key_id = environ.get("PIERIANDX_AWS_ACCESS_KEY_ID", None)

    # Get aws secret access key
    pieriandx_aws_secret_access_key = environ.get("PIERIANDX_AWS_SECRET_ACCESS_KEY", None)

    # Check all variables are set
    if any(env_var is None for env_var in [pieriandx_aws_region,
                                           pieriandx_aws_access_key_id, pieriandx_aws_secret_access_key]):
        logger.error("Please ensure you have set the following environment variables: "
                     "PIERIANDX_AWS_REGION "
                     "PIERIANDX_AWS_S3_PREFIX "
                     "PIERIANDX_AWS_ACCESS_KEY_ID "
                     "PIERIANDX_AWS_SECRET_ACCESS_KEY ")
        raise EnvironmentError

    # Return creds in dict
    return {
        "aws_region": pieriandx_aws_region,
        "aws_access_key_id": pieriandx_aws_access_key_id,
        "aws_secret_access_key": pieriandx_aws_secret_access_key
    }


def get_s3_prefix_env_var() -> str:
    pieriandx_s3_key_prefix = environ.get("PIERIANDX_AWS_S3_PREFIX", None)

    if pieriandx_s3_key_prefix is None:
        logger.error(f"Please set the env var PIERIANDX_AWS_S3_PREFIX")
        raise ValueError

    return pieriandx_s3_key_prefix


def get_s3_bucket() -> str:
    """
    Upload to s3
    :return:
    """

    s3_key_prefix = get_s3_prefix_env_var()

    return urlparse(s3_key_prefix).netloc


def get_s3_key_prefix() -> str:
    """
    Upload to s3
    :return:
    """

    s3_key_prefix = get_s3_prefix_env_var()

    return str(Path(urlparse(s3_key_prefix).path))


def pieriandx_file_uploader(src_path: Path,
                            upload_path: Path,
                            bucket: str):
    """
    Upload to s3
    :return:
    """
    # Get credentials from environment
    creds_dict = get_s3_creds_from_environment()

    # Try through the cli
    logger.debug(f"Uploading {src_path.absolute()} to s3://{bucket}{upload_path} via aws cli")
    upload_proc = subprocess.run(
        [
          "aws", "s3", "cp",
          "--sse", "AES256",
          f"{src_path.absolute()}", f"s3://{bucket}{upload_path}"
        ],
        env={
            "PATH": environ["PATH"],
            "AWS_REGION": creds_dict.get("aws_region"),
            "AWS_ACCESS_KEY_ID": creds_dict.get("aws_access_key_id"),
            "AWS_SECRET_ACCESS_KEY": creds_dict.get("aws_secret_access_key")
        }, capture_output=True
    )

    if not upload_proc.returncode == 0:
        logger.error(f"Got the following response when trying to upload s3 file.\n"
                     f"Stdout: {upload_proc.stdout.decode()}\n"
                     f"Stderr: {upload_proc.stderr.decode()}\n")
        raise S3UploadError


