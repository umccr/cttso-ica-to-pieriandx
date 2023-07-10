#!/usr/bin/env python

"""
Disease and Specimen classes
"""

from utils.globals import DISEASE_CSV, SPECIMEN_TYPE_CSV
import pandas as pd
from pathlib import Path
from typing import Dict, Optional

from utils.logging import get_logger

logger = get_logger()


class SnowMedObject:
    """
    A disease or speciment class
    """
    REFERENCE_DF: pd.DataFrame = [None]

    def __init__(self, code: int = None, label: str = None):
        """
        Given the code or label, create a snowmed object
        """
        self.code = code
        self.label = label

        # If both aren't present raise a value error
        if code is None and label is None:
            logger.error("Tried to define snow med object without code or label")
            raise ValueError

        # If both are present check match
        if code is not None and label is not None:
            self.check_match()
        elif code is not None:
            self.label = self.get_label_from_code()
        else:
            self.code = self.get_code_from_label()

    def check_match(self):
        """
        Check match of code and label
        :return:
        """
        try:
            self.REFERENCE_DF.query(f"Code=={self.code} & Label=='{self.label}'")['Code'].item()
        except ValueError:
            logger.error(f"Code {self.code} doesn't match label {self.label}")
            raise ValueError

    def get_label_from_code(self):
        """
        Get the label from code
        :return:
        """
        try:
            return self.REFERENCE_DF.query(f"Code=={self.code}")['Label'].item()
        except ValueError:
            logger.error(f"Could not find unique code {self.code} in the csv")
            raise ValueError

    def get_code_from_label(self):
        """
        Get the code from the label
        :return:
        """
        try:
            return self.REFERENCE_DF.query(f"Label=={self.label}")['Code'].item()
        except ValueError:
            logger.error(f"Could not find unique label {self.label} in the csv")
            raise ValueError

    @classmethod
    def read_csv(cls, reference_csv: Path = None) -> pd.DataFrame:
        """
        Read the reference csv
        :return:
        """
        return pd.read_csv(reference_csv, header=0)

    def to_dict(self) -> Dict:
        """
        Return as a dictionary
        :return:
        """
        return {
            "code": str(self.code),
            "label": str(self.label)
        }


class Disease(SnowMedObject):
    """
    The disease object
    """
    REFERENCE_DF = SnowMedObject.read_csv(DISEASE_CSV)


class SpecimenType(SnowMedObject):
    """
    The specimen object
    """
    REFERENCE_DF = SnowMedObject.read_csv(SPECIMEN_TYPE_CSV)


class MedicalFacility:
    """
    A Medical facility has two objects, facility and hospital number
    """
    def __init__(self, facility: Optional[str] = None, hospital_number: Optional[str] = None):
        self.facility: str = facility
        self.hospital_number: str = hospital_number

    def to_dict(self):
        """
        Return dictionary
        :return:
        """
        output_dict = {
            "facility": self.facility,
            "hospitalNumber": self.hospital_number
        }

        # Remove unspecified items
        for key, item in output_dict.copy().items():
            if pd.isna(item):
                output_dict.pop(key)

        # Return output dict
        return output_dict

    @classmethod
    def from_dict(cls, medical_facility_dict: Dict):
        """
        Get value from dictionary
        :return:
        """
        return cls(
            facility=medical_facility_dict.get("facility", None),
            hospital_number=medical_facility_dict.get("hospital_number", None)
        )


class MedicalRecordNumber:
    def __init__(self, mrn: int = None, medical_facility: MedicalFacility = None):
        self.mrn: int = mrn
        self.medical_facility: MedicalFacility = medical_facility

    def to_dict(self):
        """
        Return dictionary
        :return:
        """
        return {
            "mrn": self.mrn,
            "medicalFacility": self.medical_facility.to_dict()
        }

    @classmethod
    def from_dict(cls, medical_record_number_dict: Dict):
        """
        Get value from dictionary
        :return:
        """
        return cls(
            mrn=medical_record_number_dict.get("mrn", None),
            medical_facility=MedicalFacility.from_dict(medical_record_number_dict)
        )


class Physician:
    """
    The physician of the patient
    """
    def __init__(self, first_name: str, last_name: str):
        self.first_name: str = first_name
        self.last_name: str = last_name

    def to_dict(self) -> Dict:
        """
        Return dictionary
        :return:
        """
        return {
            "firstName": self.first_name,
            "lastName": self.last_name
        }

    @classmethod
    def from_dict(cls, physician_dict: Dict):
        """
        Get object from dictionary
        :return:
        """
        return cls(
            first_name=physician_dict.get("first_name", None),
            last_name=physician_dict.get("last_name", None)
        )