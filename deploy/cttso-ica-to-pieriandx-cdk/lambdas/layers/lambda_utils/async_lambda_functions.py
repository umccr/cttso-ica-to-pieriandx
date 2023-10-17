#!/usr/bin/env python3

"""
Process in parallel with these asynchonous AWS lambda functions
"""

from typing import List
import pandas as pd

from .pieriandx_helpers import get_existing_pieriandx_case_accession_numbers
from .logger import get_logger
from .redcap_helpers import get_clinical_metadata_from_redcap_for_subject
from .portal_helpers import \
    get_clinical_metadata_information_from_portal_for_subject, \
    get_ica_workflow_run_id_from_portal

logger = get_logger()


async def async_get_existing_pieriandx_case_accession_numbers() -> List:
    """
    Asynchronise the collection of the existing pieriandx case accession numbers
    List of accession numbers
    :return: [
      "SBJ12345_L2345655_001",
      ...
    ]
    """
    logger.info("Starting async function 'get_existing_pieriandx_case_accession_numbers'")

    return get_existing_pieriandx_case_accession_numbers()


async def async_get_metadata_information_from_redcap(subject_id: str, library_id: str, allow_missing_data=False) -> pd.DataFrame:
    """
    Get the following information from redcap
    * Clinician Name
    * Subject ID (to confirm with portal data)
    * Library ID (to confirm with portal data)
    * Patient URN (to confirm with portal data)
    * Disease (Both ID and code)
    * Date Collection (Collection date of specimen)
    * Time Collection (Collection time of specimen)
    * Date Received (Date Specimen was received)
    * Record Type (Is this a validation workflow or a Patient Sample?)
    * Gender (The gender of the patient)
    * Pierian Metadata Complete (Is the row complete?)
    :param subject_id:
    :param library_id:
    :param allow_missing_data:
    :return: A pandas DataFrame with the following columns:
      * requesting_physicians_first_name
      * requesting_physicians_last_name
      * subject_id
      * library_id
      * patient_urn
      * disease_id
      * disease_name
      * date_collected
      * time_collected
      * date_received
      * sample_type
      * gender
      * pierian_metadata_complete
    """
    logger.info("Starting async function and returning metadata information from redcap")
    metadata_df = get_clinical_metadata_from_redcap_for_subject(subject_id=subject_id, library_id=library_id, allow_missing_data=allow_missing_data)
    logger.info("Completed async function and returning metadata information from redcap")
    return metadata_df


async def async_get_metadata_information_from_portal(subject_id: str, library_id: str) -> pd.DataFrame:
    """
    Get the required information from the data portal
    * External Sample ID -> External Specimen ID
    * External Subject ID -> Patient URN
    :param subject_id:
    :param library_id:
    :return: A pandas DataFrame with the following columns:
      * subject_id
      * library_id
      * project_name
      * external_sample_id
      * external_subject_id
    """

    logger.info("Starting async function - 'Getting metadata information from the portal'")
    return get_clinical_metadata_information_from_portal_for_subject(subject_id=subject_id, library_id=library_id)


async def async_get_ica_workflow_run_id_from_portal(subject_id: str, library_id: str) -> str:
    """
    Get the ICA workflow run ID from the portal name
    wfr_name will look something like: umccr__automated__tso_ctdna_tumor_only__SBJ02091__L2200593__202205245ae2e876
    Get latest successful run
    :param subject_id:
    :param library_id:
    :return:
    """
    logger.info("Starting async function 'collecting ICA workflow run ID from portal'")

    return get_ica_workflow_run_id_from_portal(subject_id=subject_id, library_id=library_id)
