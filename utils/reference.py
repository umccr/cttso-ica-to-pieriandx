#!/usr/bin/env python3

"""
Load csv files
"""

from globals import DISEASE_CSV, SPECIMEN_TYPE_CSV
import pandas as pd


def read_disease_csv() -> pd.DataFrame:
    """
    Return disease csv with Code and Label 
    :return: 
    """
    return pd.read_csv(DISEASE_CSV, header=0)


def read_specimen_csv() -> pd.DataFrame:
    """
    Return the specimen csv with Code and Label names
    :return: 
    """
    return pd.read_csv(SPECIMEN_TYPE_CSV, header=0)