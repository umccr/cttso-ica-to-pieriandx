#!/usr/bin/env python

"""
Sequence Run, Case and Specimen classes
"""
from os import remove
from pathlib import Path
from shutil import move
from tempfile import TemporaryDirectory
from typing import Dict, Optional, List

import pandas as pd
import time
from datetime import datetime
from requests import Response
import json

from utils.enums import Ethnicity, Race, Gender, SampleType, PanelType
from utils.errors import RunNotFoundError, \
    CaseNotFoundError, RunExistsError, CaseCreationError, \
    SequencingRunCreationError, JobCreationError
from utils.globals import DAG, \
    MAX_CASE_FILE_UPLOAD_ATTEMPTS, CASE_FILE_RETRY_TIME, \
    MAX_CASE_CREATION_ATTEMPTS, CASE_CREATION_RETRY_TIME, \
    MAX_RUN_CREATION_ATTEMPTS, RUN_CREATION_RETRY_TIME, \
    MAX_JOB_CREATION_ATTEMPTS, JOB_CREATION_RETRY_TIME
from utils.micro_classes import SpecimenType, Disease, MedicalRecordNumber, Physician
from utils.pieriandx_helper import get_pieriandx_client
from utils.s3_uploader import get_s3_bucket, get_s3_key_prefix, pieriandx_file_uploader
from utils.logging import get_logger
from utils.errors import UploadCaseFileError, CaseExistsError

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
                 specimen_type: SpecimenType):
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
        Read in the class from a dictionary object
        :param specimen_dict:
        :return:
        """
        raise NotImplementedError

    def to_dict(self) -> Dict:
        """
        Write out the class as a dictionary object for case submission
        :return:
        """
        raise NotImplementedError


class DeIdentifiedSpecimen(Specimen):
    """
    Specimen that is deidentified 
    """

    def __init__(self, **kwargs):
        self.study_identifier: Optional[str] = kwargs.pop("study_identifier")
        self. study_subject_identifier: Optional[str] = kwargs.pop("study_subject_identifier")
        super(DeIdentifiedSpecimen, self).__init__(**kwargs)

    @classmethod
    def from_dict(cls, specimen_dict: Dict):
        """
        Create a specimen object from a dictionary
        :return:
        """
        return cls(
            name=specimen_dict.get("specimen_label", None),
            case_accession_number=specimen_dict.get("accession_number", None),
            date_accessioned=specimen_dict.get("date_accessioned", None),
            date_received=specimen_dict.get("date_received", None),
            date_collected=specimen_dict.get("date_collected", None),
            ethnicity=Ethnicity(specimen_dict.get("ethnicity", None)),
            race=Race(specimen_dict.get("race", None)),
            gender=Gender(specimen_dict.get("gender", None)),
            external_specimen_id=specimen_dict.get("external_specimen_id", None),
            specimen_type=specimen_dict.get("specimen_type_obj", None),
            # De-identified specific fields
            study_identifier=specimen_dict.get("study_identifier", None),
            study_subject_identifier=specimen_dict.get("study_subject_identifier", None)
        )

    def to_dict(self) -> Dict:
        """
        Write out the class as a dictionary object for case submission
        :return:
        """
        return {
            "name": self.name,
            "accessionNumber": self.case_accession_number,
            "dateAccessioned": self.date_accessioned.isoformat().replace("+00:00", "Z"),
            "dateReceived": self.date_received.isoformat().replace("+00:00", "Z"),
            "datecollected": self.date_collected.isoformat().replace("+00:00", "Z"),
            "ethnicity": self.ethnicity.value,
            "externalSpecimenId": self.external_specimen_id,
            "gender": self.gender.value,
            "race": self.race.value,
            "type": self.specimen_type.to_dict(),
            # De-identified specific fields
            "studyIdentifier": self.study_identifier,
            "studySubjectIdentifier": self.study_subject_identifier
        }


class IdentifiedSpecimen(Specimen):
    """
    Specimen that is 'identified'
    """
    def __init__(self, **kwargs):
        """
        Identified Speciment specific arguments
        :param kwargs:
        """
        self.date_of_birth: Optional[datetime] = kwargs.pop("date_of_birth")  # All fake though
        self.first_name: Optional[str] = kwargs.pop("first_name")  # All fake though
        self.last_name: Optional[str] = kwargs.pop("last_name")  # All fake though
        self.medical_record_numbers: Optional[List[MedicalRecordNumber]] = kwargs.pop("medical_record_numbers")

        super(IdentifiedSpecimen, self).__init__(**kwargs)

    @classmethod
    def from_dict(cls, specimen_dict: Dict):
        """
        Create a specimen object from a dictionary
        :return:
        """
        return cls(
            name=specimen_dict.get("specimen_label", None),
            case_accession_number=specimen_dict.get("accession_number", None),
            date_accessioned=specimen_dict.get("date_accessioned", None),
            date_received=specimen_dict.get("date_received", None),
            date_collected=specimen_dict.get("date_collected", None),
            ethnicity=Ethnicity(specimen_dict.get("ethnicity", None)),
            race=Race(specimen_dict.get("race", None)),
            gender=Gender(specimen_dict.get("gender", None)),
            external_specimen_id=specimen_dict.get("external_specimen_id", None),
            specimen_type=specimen_dict.get("specimen_type_obj", None),
            # Identified specific fields
            date_of_birth=specimen_dict.get("date_of_birth", None),
            first_name=specimen_dict.get("first_name", None),
            last_name=specimen_dict.get("last_name", None),
            medical_record_numbers=[
                MedicalRecordNumber.from_dict(mrn)
                for mrn in specimen_dict.get("medical_record_numbers", None)
            ]
        )

    def to_dict(self) -> Dict:
        """
        Write out the class as a dictionary object for case submission
        :return:
        """
        return {
            "name": self.name,
            "accessionNumber": self.case_accession_number,
            "dateAccessioned": self.date_accessioned.isoformat().replace("+00:00", "Z"),
            "dateReceived": self.date_received.isoformat().replace("+00:00", "Z"),
            "datecollected": self.date_collected.isoformat().replace("+00:00", "Z"),
            "ethnicity": self.ethnicity.value,
            "externalSpecimenId": self.external_specimen_id,
            "gender": self.gender.value,
            "race": self.race.value,
            "type": self.specimen_type.to_dict(),
            # De-identified specific fields
            "dateOfBirth": str(self.date_of_birth.date()),
            "firstName": self.first_name,
            "lastName": self.last_name,
            "medicalRecordNumbers": [
                mrn.to_dict()
                for mrn in self.medical_record_numbers
            ]
        }


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
        self.case_files_dir: Optional[Path] = None
        self.file_list: Optional[List[Path]] = None

        # Add specimens
        self.specimens: List[Specimen] = [case.specimen for case in cases]

        # Run ID added when object is called
        self.run_id = None

        # Create run dirs
        self.make_run_dirs()

    def __call__(self, dryrun: bool = False):
        """
        Create the run
        :return:
        """
        data = self.get_run_creation_request_data()

        if dryrun:
            logger.debug(f"Would submit run data as "
                         f"'{data}'")
            self.run_id = 0
            return

        self.create_run(data)

    @staticmethod
    def get_timestamp():
        """
        Get epoch timestamp to append to run name
        :return:
        """
        return \
            str(
                int(
                    datetime.timestamp(
                        datetime.utcnow().replace(microsecond=0)
                    )
                )
            )

    def create_run(self, data):
        """
        Create the run
        :param data:
        :return:
        """

        # Get the pyriandx client
        pyriandx_client = get_pieriandx_client()

        logger.debug(f"Creating a sequencing run with the following data requests {data}")

        iter_count = 0
        while True:
            # Add iter
            iter_count += 1
            if iter_count >= MAX_RUN_CREATION_ATTEMPTS:
                logger.error(f"Tried to create the run {str(MAX_CASE_CREATION_ATTEMPTS)} times and failed!")
                raise SequencingRunCreationError

            # Log this
            logger.debug(f"Creating sequencing run {self.run_dir} with attempt {str(iter_count)}")

            # Call end point
            response: Response = pyriandx_client._post_api(endpoint="/sequencerRun",
                                                           data=data)

            logger.debug("Printing response")
            if not response.status_code == 200:
                logger.warning(f"Received code {response.status_code} and {response.content} trying "
                               f"to create a sequencer run entrance")
                logger.warning(f"Trying upload again - attempt {iter_count}")
                time.sleep(RUN_CREATION_RETRY_TIME)
            else:
                response_json: Dict = response.json()
                break

        # Get the id
        self.run_id = response_json.get("id")

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
        self.case_files_dir = self.tmp_directory / Path("case_files")

        # Iterate through directories
        for dir_item in [self.run_dir, self.staging_dir, self.basecalls_dir, self.case_files_dir]:
            dir_item.mkdir(exist_ok=True, parents=True)

        # Add VcfWorkflow to top run directory
        (self.run_dir / Path("VcfWorkflow.txt")).touch()

    def upload_to_s3_bucket(self, dryrun: bool = False):
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

            if dryrun:
                logger.debug(f"Would have uploaded file '{file_name}' to {upload_path}")
                continue

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

    # Set identified as object
    identified: bool

    def __init__(self, case_accession_number: str,
                 disease: Disease,
                 indication: str,
                 sample_type: SampleType,
                 panel_type: PanelType,
                 specimen: Specimen):
        """
        Initialise the case object
        :param case_accession_number:
        :param disease:
        :param sample_type:
        :param panel_type:
        :param specimen:
        """

        # Need to import
        self.case_accession_number: str = case_accession_number
        self.disease: Disease = disease
        self.indication: str = indication
        self.sample_type: SampleType = sample_type
        self.panel_type: PanelType = panel_type
        self.specimen: Specimen = specimen

        # Added afterwards
        self.case_id = None
        self.informatics_job_id = None
        self.run_objs: Optional[List[PierianDXSequenceRun]] = None

        # Check case exists
        self.check_case_exists()

    def __call__(self, dryrun: bool = False):
        """
        Create the case object on PierianDx
        :return:
        """

        # Get data response
        data = self.get_case_creation_request_data()

        if dryrun:
            logger.debug(f"Would submit case data as "
                         f"'{data}'")
            self.case_id = 0
            return

        self.create_case(data)

    def check_case_exists(self):
        """
        Check the case doesn't already exist
        :return:
        """
        from utils.accession import get_cases_df

        cases_df = get_cases_df()

        if self.case_accession_number in cases_df["accession_number"].tolist():
            case_id = cases_df.query(f"accession_number=='{self.case_accession_number}'")["id"].item()
            logger.error(f"This accession number already exists with case id {case_id}")
            raise CaseExistsError

    def create_case(self, data):
        """
        Get data, and create case
        :param data:
        :return:
        """
        pyriandx_client = get_pieriandx_client()

        # Debug logger
        logger.debug(f"Launching the case creation data endpoint with following data inputs {json.dumps(data)}")

        # Create the case and get the response
        iter_count = 0
        while True:
            # Iter
            iter_count += 1
            if iter_count >= MAX_CASE_CREATION_ATTEMPTS:
                logger.error(f"Tried to create the case {str(MAX_CASE_CREATION_ATTEMPTS)} times and failed!")
                raise CaseCreationError

            # Log this
            logger.debug(f"Creating case {self.case_accession_number} with attempt {str(iter_count)}")

            # Generate response
            response: Response = pyriandx_client._post_api(endpoint="/case", data=data)

            logger.debug("Printing response")
            if not response.status_code == 200:
                print(response)
                logger.warning(f"Received code {response.status_code} and {response.content} trying "
                               f"to create case {self.case_accession_number}")
                logger.warning(f"Trying case creation again - attempt {iter_count}")
                time.sleep(CASE_CREATION_RETRY_TIME)
            else:
                response_json: Dict = response.json()
                break

        # Get the id
        self.case_id = response_json.get("id")

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
        Implemented in subclass
        :return:
        """
        raise NotImplementedError

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

    def launch_informatics_job(self, dryrun: bool = False):
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

        # Create the informatics job and collect the response
        data = self.get_informatics_job_creation_request_data()

        if dryrun:
            logger.debug(f"Would have pushed informatics job with data '{data}'")
            self.informatics_job_id = 0
            return

        self.launch_informatics_job_with_retries(data)

    def launch_informatics_job_with_retries(self, data):
        """
        Continuously try to launch the informatics job
        :param data:
        :return:
        """

        # Launch informatics job
        pyriandx_client = get_pieriandx_client()

        # Set iter count
        iter_count = 0
        while True:
            # Add iter
            iter_count += 1
            if iter_count >= MAX_JOB_CREATION_ATTEMPTS:
                logger.error(f"Tried to upload the case file {str(MAX_JOB_CREATION_ATTEMPTS)} times and failed!")
                raise JobCreationError

            # Call end point
            response: Response = pyriandx_client._post_api(endpoint=f"/case/{self.case_id}/informaticsJobs",
                                                 data=data)

            logger.debug("Printing response")
            if not response.status_code == 200:
                logger.warning(f"Received code {response.status_code} and {response.content} trying "
                               f"create the informatics job")
                logger.warning(f"Trying job creation again - attempt {iter_count}")
                time.sleep(JOB_CREATION_RETRY_TIME)
            else:
                response_json: Dict = response.json()
                break

        # Get the id
        self.informatics_job_id = response_json.get("jobId")

        logger.debug(f"Created informatics job for case {self.case_id} with "
                     f"data {data} and retrieved job id {str(self.informatics_job_id)}")

    def upload_case_files(self, dryrun: bool = False):
        """
        Upload the case files for the case
        :return:
        """
        # Get pieriandx client
        pyriandx_client = get_pieriandx_client()

        for file_name in self.run_objs[0].case_files_dir.rglob("*"):
            if dryrun:
                logger.debug(f"Would have uploaded file {file_name} to case files")
                continue
            # Set iter count
            iter_count = 0
            while True:
                # Add iter
                iter_count += 1
                if iter_count >= MAX_CASE_FILE_UPLOAD_ATTEMPTS:
                    logger.error(f"Tried to upload the case file {str(MAX_CASE_CREATION_ATTEMPTS)} times and failed!")
                    raise UploadCaseFileError
                # Check file is real
                if not file_name.is_file():
                    continue

                # Log this
                logger.debug(f"Uploading {file_name.name} to url endpoint /case/{self.case_id}/caseFiles/{file_name.name}/")

                # Initialise file input
                file_dict = {
                    "file": (file_name.name, open(str(file_name.absolute()), "rb"), "text/plain")
                }

                # Call end point
                response: Response = pyriandx_client._post_api(endpoint=f"/case/{self.case_id}/caseFiles/{file_name.name}/",
                                                               files=file_dict)

                logger.debug("Printing response")
                if not response.status_code == 200:
                    logger.warning(f"Received code {response.status_code} and {response.content} trying "
                                   f"to upload {file_name.name} to end point /case/{self.case_id}/caseFiles/{file_name.name}/")
                    logger.warning(f"Trying upload again - attempt {iter_count}")
                    time.sleep(CASE_FILE_RETRY_TIME)
                else:
                    break

    @classmethod
    def from_dict(cls, case_dict: Dict):
        raise NotImplementedError


class DeIdentifiedCase(Case):
    """
    A DeIdentified Case has identified set to false
    """

    def __init__(self, **kwargs):
        self.identified = False

        # Initialise from the super class
        super(DeIdentifiedCase, self).__init__(**kwargs)

    def get_case_creation_request_data(self):
        """
        Create the case creation request data
        :return:
        """
        data = {
            "dagName": DAG.get("name"),
            "dagDescription": DAG.get("description"),
            "disease": self.disease.to_dict(),
            "identified": self.identified,
            "indication": self.indication,
            "panelName": self.panel_type.value,
            "sampleType": self.sample_type.value,
            "specimens": [
                self.specimen.to_dict()
            ]
        }

        return data

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
                   panel_type=PanelType[case_dict.get("panel_type").upper().replace("_", "")],
                   # Still need to load this
                   specimen=DeIdentifiedSpecimen.from_dict(case_dict),
                   indication=case_dict.get("indication"))


class IdentifiedCase(Case):
    """
    An Identified Case has identified set to True
    """

    def __init__(self, **kwargs):
        self.identified = True
        self.requesting_physicians: Optional[List[Physician]] = kwargs.pop("requesting_physicians")

        # Initialise from the super class
        super(IdentifiedCase, self).__init__(**kwargs)

    def get_case_creation_request_data(self):
        """
        Create the case creation request data
        :return:
        """
        data = {
            "dagName": DAG.get("name"),
            "dagDescription": DAG.get("description"),
            "disease": self.disease.to_dict(),
            "identified": self.identified,
            "indication": self.indication,
            "physicians": [
                physician.to_dict()
                for physician in self.requesting_physicians
            ],
            "panelName": self.panel_type.value,
            "sampleType": self.sample_type.value,
            "specimens": [
                self.specimen.to_dict()
            ]
        }

        return data

    @classmethod
    def from_dict(cls, case_dict: Dict):
        """
        From a redcap dict file, read in the dictionary
        :param case_dict:
        :return:
        """
        return cls(
            case_accession_number=case_dict.get("accession_number"),
            disease=case_dict.get("disease_obj"),
            sample_type=SampleType(case_dict.get("sample_type")),
            panel_type=PanelType[case_dict.get("panel_type").upper().replace("_", "")],
            requesting_physicians=[
                Physician.from_dict(physician)
                for physician in case_dict.get("requesting_physicians")
            ],
            # Still need to load this
            specimen=IdentifiedSpecimen.from_dict(case_dict),
            indication=case_dict.get("indication")
        )
