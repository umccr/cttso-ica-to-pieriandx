#!/usr/bin/env python

"""
Read, handle, write samplesheets
"""

from typing import Dict, List, Optional
import pandas as pd
from pathlib import Path
import re

from utils.logging import get_logger

logger = get_logger()


def read_samplesheet(samplesheet_path: Path) -> Dict:
    """
    Read in a samplesheet, export as dict with section headers as keys
    :param samplesheet_path:
    :return:
    """
    samplesheet_dict: Dict = {}
    section_name: Optional[str] = None

    if not samplesheet_path.is_file():
        logger.error(f"Cannot find samplesheet at {samplesheet_path}")
        raise FileNotFoundError

    with open(samplesheet_path, "r") as samplesheet_h:
        for line in samplesheet_h.readlines():
            line_str = line.rstrip().rstrip(",")

            if line_str == "":
                # Blank line, skip
                continue

            # Check if this is a section header
            re_section_obj = re.match(r"^\[(\S+)](,*)?$", line_str)

            if re_section_obj is not None:
                # This is a section header
                section_name = re_section_obj.group(1)
                # Initialise section in dict
                samplesheet_dict[section_name] = []
            elif section_name is not None:
                # Append this line to the existing section
                samplesheet_dict[section_name].append(line_str)
            else:
                # Don't know how we go here. Just skip
                continue

    samplesheet_dict_tmp = {}
    for section_header, list_items in samplesheet_dict.items():
        if section_header.endswith("Data"):
            # Data should be a pandas df
            samplesheet_dict_tmp[section_header] = pd.DataFrame([list_item.split(",") for list_item in list_items[1:]],
                                                                columns=list_items[0].split(","))
        elif section_header.endswith("Reads"):
            # Data should be in tuple format
            samplesheet_dict_tmp[section_header] = list_items
        else:
            # Everything else has standard 1:1 pairing dict
            samplesheet_dict_tmp[section_header] = {list_item.split(",")[0]: list_item.split(",")[1]
                                                    for list_item in list_items}

    # Reassigning samplesheet dict
    samplesheet_dict = samplesheet_dict_tmp
    del samplesheet_dict_tmp

    return samplesheet_dict


def write_samplesheet(samplesheet_dict: Dict, samplesheet_output_path: Path):
    """
    Write out the samplesheet
    :param samplesheet_dict:
    :param samplesheet_output_path:
    :return:
    """
    # Set newline to '\n' (incase this is run on windowns machines)
    with open(samplesheet_output_path, "w", newline="\n") as samplesheet_h:
        for section_name, section_body in samplesheet_dict.items():
            # Write header
            samplesheet_h.write(f"[{section_name}]\n")

            # Add content
            if isinstance(section_body, dict):
                for key, value in section_body.items():
                    samplesheet_h.write(f"{key},{value}\n")
            elif isinstance(section_body, list):
                for section_line in section_body:
                    samplesheet_h.write(f"{section_line}\n")
            elif isinstance(section_body, pd.DataFrame):
                section_body.to_csv(samplesheet_h, header=True, index=False, line_terminator="\n")

            # Add blank line in between sections (but Data section covers EOF with to_csv)
            if not section_name == "Data":
                samplesheet_h.write("\n")


def update_samplesheet(samplesheet_dict: Dict, sample_id: str, lanes: List) -> Dict:
    """
    Update the dataframe in the samplesheet to be just a single sample
    :param samplesheet_dict:
    :param sample_id:
    :param lanes:
    :return:
    """
    # Get the data section
    sample_df = samplesheet_dict["Data"]

    # Add in the lane information
    sample_df["Lane"] = lanes
    # Rename the columns
    sample_df.rename(columns={"Index": "index", "Index2": "index2"}, inplace=True)
    # Add in the Sample_Name column
    sample_df["Sample_Name"] = sample_id
    # Drop the Sample_Type column
    sample_df.drop(columns="Sample_Type", inplace=True)

    # Reorder
    sample_df = sample_df[["Sample_ID", "Sample_Name", "Lane", "index", "index2"]]

    # Set to just the first row
    sample_df = sample_df.head(n=1)

    # Reassign
    samplesheet_dict["Data"] = sample_df

    # Return
    return samplesheet_dict


