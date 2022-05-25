#!/usr/bin/env python3

"""
List of rogue errors
"""


class ArgumentError(Exception):
    """
    Incorrect Argument
    """
    pass


class CaseNotFoundError(Exception):
    """
    Case has not been registered on PierianDx
    """
    pass


class CaseExistsError(Exception):
    """
    Case already exists
    """
    pass


class RunNotFoundError(Exception):
    """
    Run has not been registered on PierianDx
    """
    pass


class RunExistsError(Exception):
    """
    Run has been regiestered on PierianDx
    """
    pass


class S3UploadError(Exception):
    """
    Could not upload a file to s3
    """
    pass


class UploadCaseFileError(Exception):
    """
    Could not upload a case file
    """
    pass


class CaseCreationError(Exception):
    """
    Could not create case
    """
    pass


class SequencingRunCreationError(Exception):
    """
    Could not create sequencing run
    """
    pass


class JobCreationError(Exception):
    """
    Could not create job
    """
    pass


class ListCasesError(Exception):
    """
    Couldn't list all of the cases
    """
    pass
