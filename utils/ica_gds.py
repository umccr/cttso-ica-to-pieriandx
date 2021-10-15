#!/usr/bin/env python

"""
ICA GDS shortcut handlers
"""

from urllib.parse import urlparse
from pathlib import Path
from libica.openapi.libwes import WorkflowRun
from libica.openapi import libgds
from libica.openapi.libgds import FileResponse
from typing import List, Optional
from wget import download
from utils.globals import CTTSO_FILE_SUFFIXES, ICA_GDS_MAX_PAGE_SIZE
import gzip
import shutil

from utils.logging import get_logger

logger = get_logger()


def get_ica_gds_configuration() -> libgds.Configuration:
    """
    Get the configuration object for ica wes
    :return:
    """
    from utils.ica_base import get_configuration
    return get_configuration(libgds.Configuration)


def get_presigned_url_for_gds_file(gds_file_path: str) -> str:
    """
    Get a presigned url from a gds file
    :param gds_file_path:
    :return:
    """
    # Get configuration
    gds_configuration = get_ica_gds_configuration()

    # Get file listing on gds file
    # Enter a context with an instance of the API client
    with libgds.ApiClient(gds_configuration) as api_client:
        # Create an instance of the API class
        api_instance = libgds.FilesApi(api_client)

    # Get files list
    files_list: List[FileResponse] = api_instance.list_files(volume_name=[urlparse(gds_file_path).netloc],
                                                             path=[urlparse(gds_file_path).path], recursive=True, page_size=10000,
                                                             include="presignedUrl").items

    # Check there's at least one item in the list
    if len(files_list) == 0:
        logger.error(f"Could not find the file at {gds_file_path}")
        raise FileNotFoundError

    # Get file object from file list
    file_object = files_list[0]

    return file_object.presigned_url


def download_gds_file(gds_file_path: str, output_file_path: Path):
    """
    Download a gds file path to a file on the local system
    :param gds_file_path:
    :param output_file_path:
    :return:
    """
    presigned_url = get_presigned_url_for_gds_file(gds_file_path)

    if not output_file_path.parent.is_dir():
        logger.error(f"Please create the parent directory of {output_file_path} before downloading")
        raise FileNotFoundError

    logger.debug(f"Downloading {gds_file_path} to {output_file_path}")
    download(presigned_url, out=output_file_path)


def list_files_from_gds_directory(gds_folder_path: str, recursive: bool = False) -> List[FileResponse]:
    """
    List all files in a gds directory
    :return:
    """
    # Get configuration
    next_page = True
    page_token = None
    gds_configuration = get_ica_gds_configuration()

    logger.debug(f"Listing files in {gds_folder_path}")

    gds_files_list = []

    with libgds.ApiClient(gds_configuration) as api_client:
        # Create an instance of the API class
        api_instance = libgds.FilesApi(api_client)

    while next_page:
        api_response: libgds.FileListResponse = api_instance.list_files(
            volume_name=[urlparse(gds_folder_path).netloc],
            path=[str(Path(urlparse(gds_folder_path).path)) + "/*"],
            include="presignedUrl",
            recursive=recursive,
            page_size=ICA_GDS_MAX_PAGE_SIZE,
            page_token=page_token
        )

        # Do we need to go another round?
        if api_response.next_page_token is None:
            next_page = False
        else:
            page_token = api_response.next_page_token

        # Extend the list
        gds_files_list.extend(api_response.items)

    logger.debug(f"Found {len(gds_files_list)} files in folder")

    return gds_files_list


def find_files_in_gds_directory(gds_folder_path: str, file_list: List[str], recursive: bool = False) -> List[Optional[FileResponse]]:
    """
    Given a gds folder path, returns a list of files inside that directory
    :param gds_folder_path:
    :param file_list:
    :param recursive:
    :return:
    """


    gds_files_list: List[FileResponse] = list_files_from_gds_directory(gds_folder_path, recursive=recursive)
    file_obj_list = []

    for file_name in file_list:
        matching_files_list = list(filter(lambda x: x.name == file_name,
                                          gds_files_list))

        # Make sure list is not zero
        if len(matching_files_list) == 0:
            logger.warning(f"Could not find {file_name} inside {gds_folder_path}")
            file_obj_list.append(None)
            continue

        file_obj_list.append(matching_files_list[0])

    return file_obj_list


def get_cttso_analysis_files_from_directory(sample_name: str, gds_folder_path: str, skip_suffixes: Optional[List] = None) -> List[Optional[FileResponse]]:
    """
    Get the analysis files from a gds output directory path
    :param sample_name: The name of the sample (prefix for the files)
    :param gds_folder_path:
    :param skip_suffixes:
    :return:
    """

    requested_file_list = [f"{sample_name}{file_suffix}"
                           for file_suffix in CTTSO_FILE_SUFFIXES
                           if skip_suffixes is None or
                           not file_suffix in skip_suffixes]

    if len(requested_file_list) == 0:
        # Already found all of the files
        return []

    return find_files_in_gds_directory(gds_folder_path=gds_folder_path,
                                       file_list=requested_file_list,
                                       recursive=True)


def collect_and_download_cttso_files_from_ica_workflow_run(sample_name: str,
                                                           ica_workflow_run_obj: WorkflowRun,
                                                           output_dir: Path, staging_dir: Path):
    """
    Given a sample name, ica workflow run object and output directory, collect and download all output files from gds
    :param sample_name:
    :param ica_workflow_run_obj:
    :param output_dir:
    :param staging_dir:
    :param basecalls_dir:
    :return:
    """
    from utils.ica_wes import get_output_directory_from_workflow_run_obj, get_working_directory_from_workflow_run_obj

    # Get the output and working directories from the run object
    output_directory = get_output_directory_from_workflow_run_obj(ica_workflow_run_obj)
    working_directory = get_working_directory_from_workflow_run_obj(ica_workflow_run_obj)

    # Check output directory exists first
    if not output_dir.is_dir():
        logger.error(f"Please create the directory {output_dir} before continuing")
        raise NotADirectoryError

    # Check staging directory exists (for compressed files)
    if not staging_dir.is_dir():
        logger.error(f"Please create the directory {staging_dir} before continuing")
        raise NotADirectoryError

    # Get the output files and missing file suffixes
    output_files = get_cttso_analysis_files_from_directory(sample_name=sample_name, gds_folder_path=output_directory)
    missing_file_suffixes = []

    # Check if there are any missing files
    for index, output_file in enumerate(output_files):
        if output_file is None:
            missing_file_suffixes.append(CTTSO_FILE_SUFFIXES[index])

    # Skip the following suffixes (we've already found these files)
    found_suffixes = [file_suffix
                      for file_suffix in CTTSO_FILE_SUFFIXES
                      if file_suffix not in missing_file_suffixes]

    # Get remaining files from the working directory
    files_in_working_dir = get_cttso_analysis_files_from_directory(sample_name=sample_name,
                                                                   gds_folder_path=working_directory,
                                                                   skip_suffixes=found_suffixes)

    # Reorder outputs as per the entry in CTTSO_FILE_SUFFIXES (just so we can make sure they're all there!)
    ordered_file_list: List[FileResponse] = []
    missing_files = False
    for file_suffix in CTTSO_FILE_SUFFIXES:
        for file_obj in output_files + files_in_working_dir:
            if file_obj is None:
                continue
            if file_obj.name == f"{sample_name}{file_suffix}":
                ordered_file_list.append(file_obj)
                # Move on to next suffix
                break
        else:
            missing_files = True
            logger.error(f"Could not find file {sample_name}{file_suffix} in either output or working directory of workflow run {ica_workflow_run_obj.id}")
    if missing_files:
        raise FileNotFoundError

    if not len(ordered_file_list) == len(CTTSO_FILE_SUFFIXES):
        logger.error("Could not find all all of the files")

    # Download all of the files
    for file_obj in ordered_file_list:
        if file_obj.name.endswith(".gz"):
            output_path = staging_dir / Path(file_obj.name)
        else:
            output_path = output_dir / Path(file_obj.name)

        logger.debug(f"Downloading gds://{file_obj.volume_name}/{file_obj.path} to {output_path}")
        download(url=file_obj.presigned_url, out=str(output_path))

    # Decompress staging files
    for compressed_file in staging_dir.glob(pattern='*'):
        decompressed_output_file = output_dir / Path(compressed_file.name.replace(".gz", ""))
        with gzip.open(compressed_file, 'rb') as f_in:
            with open(decompressed_output_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)



def collect_and_download_cttso_samplesheet_from_ica_workflow_run(ica_workflow_run_obj: WorkflowRun, output_dir: Path):
    from utils.ica_wes import get_output_directory_from_workflow_run_obj, get_working_directory_from_workflow_run_obj

    # Get the output and working directories from the run object
    output_directory = get_output_directory_from_workflow_run_obj(ica_workflow_run_obj)
    working_directory = get_working_directory_from_workflow_run_obj(ica_workflow_run_obj)

    # Check output directory exists first
    if not output_dir.is_dir():
        logger.error(f"Please create the directory {output_dir} before continuing")
        raise NotADirectoryError

    # Find the samplesheet in the output directory
    logger.debug("Searching for samplesheet in output directory")
    samplesheet_file_obj: Optional[FileResponse] = find_files_in_gds_directory(gds_folder_path=output_directory,
                                                                               file_list=["SampleSheet_Intermediate.csv"],
                                                                               recursive=True)[0]

    # Or the working directory
    if samplesheet_file_obj is None:
        logger.debug("Searching for samplesheet in working directory")
        samplesheet_file_obj: Optional[FileResponse] = find_files_in_gds_directory(gds_folder_path=working_directory,
                                                                                   file_list=["SampleSheet_Intermediate.csv"],
                                                                                   recursive=True)[0]

    # Reassure samplesheet is none
    if samplesheet_file_obj is None:
        logger.error(f"Could not find the samplesheet in either the "
                     f"output directory {output_directory} or the working directory {working_directory}")
        raise FileNotFoundError

    # Download the samplesheet
    output_path: Path = output_dir / Path("SampleSheet.csv")
    logger.debug(f"Downloading gds://{samplesheet_file_obj.volume_name}/{samplesheet_file_obj.path} to {output_path}")
    download(url=samplesheet_file_obj.presigned_url, out=str(output_path))
