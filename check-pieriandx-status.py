#!/usr/bin/env python

"""
Given a list of case ids or accession numbers, check the status of the PierianDx informatics jobs
"""

from utils.args import get_case_status_args, check_case_status_args
from utils.accession import get_cases_df_from_params, get_informatics_status_by_case_id
from utils.logging import set_basic_logger
from typing import List, Optional
from logging import DEBUG, INFO
import sys
import pandas as pd

logger = set_basic_logger()


def main():
    """
    Get args, check args,
    :return:
    """
    # Check / set log level
    if "--verbose" in sys.argv:
        logger.setLevel(level=DEBUG)
    else:
        logger.setLevel(level=INFO)

    # Get and check args
    args = get_case_status_args()
    args = check_case_status_args(args)

    # Get case accession list
    case_ids_list: Optional[List] = getattr(args, "case_ids_list")
    case_accession_numbers_list: Optional[List] = getattr(args, "case_accession_numbers_list")

    # Get accession numbers
    logger.info("Collecting all cases")
    cases_df = get_cases_df_from_params(case_ids=case_ids_list,
                                        case_accessions=case_accession_numbers_list,
                                        merge_type="outer")

    # Get status args
    informatics_job_list = []
    for index, case_series in cases_df.iterrows():
        case_id = case_series.get("id")
        new_informatics_list = get_informatics_status_by_case_id(case_id=case_id)
        if new_informatics_list is not None:
            informatics_job_list.extend(new_informatics_list)

    # Add the lists of lists to the df
    # Create a dataframe of the informatics jobs
    informatics_df = pd.DataFrame(informatics_job_list)

    # Merge dataframes
    cases_df = pd.merge(cases_df, informatics_df, left_on="id", right_on="case_id", how="outer").\
        dropna(axis="columns", how="all").drop(columns=["case_id"])

    # Print cases df
    print(cases_df.to_csv(index=False, header=True, sep="\t"))


if __name__ == "__main__":
    main()


