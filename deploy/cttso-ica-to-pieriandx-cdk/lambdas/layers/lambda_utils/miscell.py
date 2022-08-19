#!/usr/bin/env python3

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


def get_alphabet():
    """
    Used by gspread to get column names
    Credit: https://stackoverflow.com/a/37716814/6946787
    :return:
    """
    return [chr(i) for i in range(ord('A'),ord('Z')+1)]
