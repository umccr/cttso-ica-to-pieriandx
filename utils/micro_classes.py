#!/usr/bin/env python

"""
Disease and Specimen classes
"""

from utils.globals import DISEASE_CSV, SPECIMEN_TYPE_CSV
import pandas as pd
from pathlib import Path
from typing import Dict

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

    def get_label_from_code(self):
        """
        Get the label from code
        :return:
        """
        try:
            return self.REFERENCE_DF.query(f"Code=={self.code}")['Label'].item()
        except ValueError:
            logger.error(f"Could not find unique code {self.code} in the csv")

    def get_code_from_label(self):
        """
        Get the code from the label
        :return:
        """
        try:
            return self.REFERENCE_DF.query(f"Label=={self.label}")['Code'].item()
        except ValueError:
            logger.error(f"Could not find unique label {self.label} in the csv")

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
