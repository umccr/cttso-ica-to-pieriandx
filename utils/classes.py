#!/usr/bin/env python

"""
Sequence Run, Case and Specimen classes
"""
from datetime import datetime
from os import remove
from pathlib import Path
from shutil import move
from tempfile import TemporaryDirectory
from typing import Dict, Optional, List

import pandas as pd

from utils.enums import Ethnicity, Race, Gender, SampleType
from utils.errors import RunNotFoundError, CaseNotFoundError, RunExistsError
from utils.globals import DAG, PANEL_NAME
from utils.micro_classes import SpecimenType, Disease
from utils.pieriandx_helper import get_pieriandx_client
from utils.s3_uploader import get_s3_bucket, get_s3_key_prefix, pieriandx_file_uploader
from utils.logging import get_logger

# Load other classes inside class saves circular import errors

logger = get_logger()


class Specimen:
    """
    The specimen object
    """

    def __init__(self, name: str, case_accession_number: str,
                 date_accessioned: datetime, date_received: datetime, date_collected: datetime,
                 ethnicity: Ethnicity, race: Race, gender: Gender,
                 external_specimen_id: str,
                 study_identifier: str, study_subject_identifier: str, specimen_type: SpecimenType):
        # Populate specimen objects from input
        self.name = name
        self.case_accession_number = case_accession_number
        self.date_accessioned = date_accessioned
        self.date_received = date_received
        self.date_collected = date_collected
        # Add optionals
        self.ethnicity: Ethnicity = ethnicity
        self.race: Race = race
        self.gender: Gender = gender
        # Add external identifiers
        self.external_specimen_id = external_specimen_id
        self.study_identifier = study_identifier
        self.study_subject_identifier = study_subject_identifier
        self.specimen_type: SpecimenType = specimen_type

        # Additional objects to be added in down the line
        self.sample_id = None
        self.barcode = None
        self.lane = None
        self.sample_type = None

    def add_sample_id(self, sample_id):
        self.sample_id = sample_id

    def add_samplesheet_attributes(self, samplesheet_data_df):
        sample_df = samplesheet_data_df.query(f"Sample_ID=='{self.sample_id}'")
        self.barcode = f"{sample_df['index'].tolist()[0]}-{sample_df['index2'].tolist()[0]}"
        self.lane = sample_df['Lane'].tolist()[0]
        self.sample_type = f"{sample_df['Sample_Type'].item()}" if 'Sample_Type' in list(sample_df.columns) else 'DNA'

    @classmethod
    def from_dict(cls, specimen_dict: Dict):
        """
        Create a specimen object from a dictionary
        :return:
        """
        return cls(name=specimen_dict.get("specimen_label", None),
                   case_accession_number=specimen_dict.get("accession_number", None),
                   date_accessioned=specimen_dict.get("date_accessioned", None),
                   date_received=specimen_dict.get("date_received", None),
                   date_collected=specimen_dict.get("date_collected", None),
                   ethnicity=Ethnicity(specimen_dict.get("ethnicity", None)),
                   race=Race(specimen_dict.get("race", None)),
                   gender=Gender(specimen_dict.get("gender", None)),
                   external_specimen_id=specimen_dict.get("external_specimen_id", None),
                   study_identifier=specimen_dict.get("study_identifier", None),
                   study_subject_identifier=specimen_dict.get("study_subject_identifier", None),
                   specimen_type=specimen_dict.get("specimen_type_obj", None))

    @classmethod
    def from_json(cls, specimen_json: Dict):
        """

        :return:
        """
        raise NotImplementedError


class PierianDXSequenceRun:
    """
    This sequence run imports a list of cases and sample files
    """


    def __init__(self, run_name: str, cases):
        # Set initial inputs
        self.run_name = run_name

        # Initialise directories
        self.tmp_directory: Path = Path(TemporaryDirectory(prefix=run_name).name)
        self.run_dir: Optional[Path] = None
        self.staging_dir: Optional[Path] = None
        self.basecalls_dir: Optional[Path] = None
        self.file_list: Optional[List[Path]] = None

        # Add specimens
        self.specimens: List[Specimen] = [case.specimen for case in cases]

        # Run ID added when object is called
        self.run_id = None

        # Create run dirs
        self.make_run_dirs()

    def __call__(self):
        """
        Create the run
        :return:
        """

        pyriandx_client = get_pieriandx_client()

        data = self.get_run_creation_request_data()

        logger.debug(f"Creating a sequencing run with the following data requests {data}")
        response = pyriandx_client._post_api(endpoint="/sequencerRun",
                                             data=data).json()

        # Get the id
        self.run_id = response.get("id")

    def get_run_creation_request_data(self):
        """
        Create the run creation request data
        :return:
        """
        return {
            "runId": self.run_name,
            "type": "pairedEnd",
            "specimens": [
                {
                    "accessionNumber": f"{specimen.case_accession_number}",
                    "lane": specimen.lane,
                    "barcode": specimen.barcode,
                    "sampleId": specimen.sample_id,
                    "sampleType": specimen.sample_type
                }
                for specimen in self.specimens
            ]
        }

    def rename_run(self, new_run_name: str):
        """
        Once we have the ica workflow run object, we should rename the run to include the flowcell id
        :return:
        """
        if self.run_id is not None:
            logger.error("Cannot rename a run that already has an ID")
            raise RunExistsError

        self.run_name = new_run_name
        logger.debug("Moving run directory")
        move(self.run_dir, self.tmp_directory / Path(new_run_name))
        logger.debug("Re-assigning run dir path and basecalls path")
        self.run_dir = self.tmp_directory / Path(new_run_name)
        self.basecalls_dir = self.run_dir / Path("Data") / Path("Intensities") / Path("BaseCalls")

    def make_run_dirs(self):
        """
        Create the temporary run directories
        :return:
        """
        # Creating and assigning tmp directories
        self.run_dir = self.tmp_directory / Path(self.run_name)
        self.staging_dir = self.tmp_directory / Path("staging")
        self.basecalls_dir = self.run_dir / Path("Data") / Path("Intensities") / Path("BaseCalls")

        # Iterate through directories
        for dir_item in [self.run_dir, self.staging_dir, self.basecalls_dir]:
            dir_item.mkdir(exist_ok=True, parents=True)

        # Add VcfWorkflow to top run directory
        (self.run_dir / Path("VcfWorkflow.txt")).touch()

    def upload_to_s3_bucket(self):
        """
        Upload items in run_dir to the s3 bucket
        :return:
        """

        s3_bucket = get_s3_bucket()
        s3_key_prefix = get_s3_key_prefix()

        # Ensure done.txt does not exist
        done_file = (self.run_dir / Path("done.txt"))
        if done_file.is_file():
            remove(done_file)

        # Iterate through file names
        for file_name in self.run_dir.rglob("*"):
            if not file_name.is_file():
                continue
            upload_path: Path = Path(s3_key_prefix) / self.run_name / file_name.relative_to(self.run_dir)

            logger.debug(f"Uploading {file_name.name} to {upload_path}")
            pieriandx_file_uploader(src_path=file_name,
                                    upload_path=upload_path,
                                    bucket=s3_bucket)

        # Add "done.txt"
        done_file.touch()
        pieriandx_file_uploader(src_path=done_file,
                                upload_path=Path(s3_key_prefix) / self.run_name / done_file.relative_to(self.run_dir),
                                bucket=s3_bucket)


class Case:
    """
    Case is an object that launches an InformaticsJob, and is also associated with SequencerRuns
    """

    # List of globals that exist for all cases
    dag_name = "cromwell_tso500_ctdna_workflow_1.0.1"
    dag_description = "tso500_ctdna_workflow"
    identified = False
    panel_name = "tso500_ctDNA_vcf_workflow_university_of_melbourne"

    def __init__(self, case_accession_number: str,
                 disease: Disease,
                 indication: str,
                 sample_type: SampleType,
                 specimen: Specimen):
        """
        Initialise the case object
        :param case_accession_number:
        :param disease:
        :param sample_type:
        :param specimen:
        """

        # Need to import
        self.case_accession_number: str = case_accession_number
        self.disease: Disease = disease
        self.indication: str = indication
        self.sample_type: SampleType = sample_type
        self.specimen: Specimen = specimen

        # Added afterwards
        self.case_id = None
        self.informatics_job_id = None
        self.run_objs: Optional[List[PierianDXSequenceRun]] = None

    def __call__(self):
        """
        Create the case object on PierianDx
        :return:
        """
        pyriandx_client = get_pieriandx_client()

        # Get data response
        data = self.get_case_creation_request_data()

        # Debug logger
        logger.debug(f"Launching the case creation data endpoint with following data inputs {data}")
        # Create the case and get the response
        response = pyriandx_client._post_api(endpoint="/case", data=data).json()

        # Get the id
        self.case_id = response.get("id")

    def add_sample_id_to_specimen(self, sample_id):
        """
        The sample id in the sample sheet
        :param sample_id:
        :return:
        """
        self.specimen.add_sample_id(sample_id)

    def add_samplesheet_attributes_to_specimen(self, samplesheet_data_df: pd.DataFrame):
        """
        Add the necessary attributes to this case from the sample data frame
        :param samplesheet_data_df:
        :return:
        """
        self.specimen.add_samplesheet_attributes(samplesheet_data_df)

    def add_run_to_case(self, run_objs: List[PierianDXSequenceRun]):
        """
        Add run to case
        :param run_objs:
        :return:
        """
        for run_obj in run_objs:
            if run_obj.run_id is None:
                logger.error(f"Please register {run_obj.run_name} on PierianDx before adding the run to case")
                raise RunNotFoundError

        self.run_objs = run_objs

    def get_case_creation_request_data(self):
        """
        Create the case creation request data
        :return:
        """
        data = {
            "dagName": DAG.get("name"),
            "dagDescription": DAG.get("description"),
            "disease": self.disease.to_dict(),
            "identified": False,
            "indication": self.indication,
            "panelName": PANEL_NAME,
            "sampleType": self.sample_type.value,
            "specimens": [
                {
                    'name': self.specimen.name,
                    "accessionNumber": self.specimen.case_accession_number,
                    "dateAccessioned": self.specimen.date_accessioned.isoformat().replace("+00:00", "Z"),
                    "dateReceived": self.specimen.date_received.isoformat().replace("+00:00", "Z"),
                    "datecollected": self.specimen.date_collected.isoformat().replace("+00:00", "Z"),
                    'ethnicity': self.specimen.ethnicity.value,
                    "externalSpecimenId": self.specimen.external_specimen_id,
                    'gender': self.specimen.gender.value,
                    'race': self.specimen.race.value,
                    "studyIdentifier": self.specimen.study_identifier,
                    "studySubjectIdentifier": self.specimen.study_subject_identifier,
                    "type": self.specimen.specimen_type.to_dict()
                }
            ]
        }

        return data

    def get_informatics_job_creation_request_data(self):
        """

        :return:
        """
        data = {
                   "input": [
                       {
                           "accessionNumber": self.case_accession_number,
                           "sequencerRunInfos": [
                               {
                                   "runId": run_obj.run_name,
                                   "lane": specimen.lane,
                                   "barcode": specimen.barcode,
                                   "sampleId": specimen.sample_id,
                                   "sampleType": specimen.sample_type
                               }
                               for run_obj in self.run_objs
                               for specimen in run_obj.specimens
                               if specimen.case_accession_number == self.specimen.case_accession_number
                           ]
                       }
                   ]
               }

        return data

    def launch_informatics_job(self):
        """
        Launch an informatics job
        :return:
        """
        # Check case exists
        if self.case_id is None:
            logger.error("Cannot launch an informatics job when we don't have a case id")
            raise CaseNotFoundError

        # Check run objs populated
        if self.run_objs is None or len(self.run_objs) == 0:
            logger.error("No run objects exist yet")
            raise RunNotFoundError

        # Launch informatics job
        pyriandx_client = get_pieriandx_client()

        # Create the informatics job and collect the response
        data = self.get_informatics_job_creation_request_data()

        logger.debug(f"Creating informatics job for case {self.case_id} with data {data}")
        response = pyriandx_client._post_api(endpoint=f"/case/{self.case_id}/informaticsJobs",
                                             data=data).json()

        # Get the id
        self.informatics_job_id = response.get("jobId")

    @classmethod
    def from_dict(cls, case_dict: Dict):
        """
        From a redcap dict file, read in the dictionary
        :param case_dict:
        :return:
        """
        return cls(case_accession_number=case_dict.get("accession_number"),
                   disease=case_dict.get("disease_obj"),
                   sample_type=SampleType(case_dict.get("sample_type")),
                   specimen=Specimen.from_dict(case_dict),
                   indication=case_dict.get("indication"))

    @classmethod
    def from_json(cls, case_json: Dict):
        """
        From a redcap json file, read in the
        :param redcap_json:
        :return:
        """

        raise NotImplementedError


