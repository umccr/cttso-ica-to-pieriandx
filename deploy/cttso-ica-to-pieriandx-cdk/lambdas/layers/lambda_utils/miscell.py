#!/usr/bin/env python3

from datetime import date
from typing import List, Union
import pytz
from dateutil.parser import parse as date_parser

def change_case(column_name: str) -> str:
    """
    Change from Sample Type or SampleType to sample_type
    :param column_name:
    :return:
    """
    return ''.join(['_' + i.lower() if i.isupper()
                    else i for i in column_name]).lstrip('_'). \
        replace("(", "").replace(")", ""). \
        replace("/", "_per_")


def get_alphabet() -> List[str]:
    """
    Used by gspread to get column names
    Credit: https://stackoverflow.com/a/37716814/6946787
    :return:
    """
    return [chr(i) for i in range(ord('A'),ord('Z')+1)]


def handle_date(datetime_str_or_obj: Union[str, datetime]) -> datetime:
    if isinstance(datetime_str_or_obj, str):
        return date_parser(datetime_str_or_obj)
    elif isinstance(datetime_str_or_obj, datetime):
        return datetime_str_or_obj
    else:
        logger.error(f"Couldn't handle date-str-or-obj of type '{type(datetime_str_or_obj)}'")
        raise ValueError