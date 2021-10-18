#!/usr/bin/env python3

"""
Given a list of case ids or accession numbers, download a list of PierianDx reports
"""

from utils.args import get_download_reports_args, check_download_reports_args
from utils.accession import get_cases_df_from_params, get_report_ids_by_case_id, download_report
from logging import DEBUG, INFO
from utils.logging import set_basic_logger
from typing import Optional, List
from tempfile import TemporaryDirectory
import sys
import pandas as pd
import re
from pathlib import Path
from zipfile import ZipFile

logger = set_basic_logger()


def main():
    """
    Get the cases df,
    Get the reports list for each of the cases
    Download the reports
    :return:
    """
    # Check / set log level
    if "--verbose" in sys.argv:
        logger.setLevel(level=DEBUG)
    else:
        logger.setLevel(level=INFO)

    # Get / check args
    args = get_download_reports_args()
    args = check_download_reports_args(args)

    # Get cases df
    # Get case accession list
    case_ids_list: Optional[List] = getattr(args, "case_ids_list")
    case_accession_numbers_list: Optional[List] = getattr(args, "case_accession_numbers_list")

    # Get accession numbers
    logger.info("Collecting all cases")
    cases_df = get_cases_df_from_params(case_ids=case_ids_list,
                                        case_accessions=case_accession_numbers_list,
                                        merge_type="outer")

    # Get status args
    reports_list = []
    for index, case_series in cases_df.iterrows():
        case_id = case_series.get("id")
        new_reports_list = get_report_ids_by_case_id(case_id=case_id)
        if new_reports_list is not None:
            reports_list.extend(new_reports_list)

    # Add the lists of lists to the df
    reports_df = pd.DataFrame(reports_list)

    # Merge cases and report dataframes
    cases_df = pd.merge(cases_df, reports_df, left_on="id", right_on="case_id", how="outer")


    # Filter out cases with report status not set to complete
    cases_df = cases_df.query("report_status == 'complete'")

    # Download report ids for each case into tmp dirs
    tmp_dir = Path(TemporaryDirectory().name).absolute().resolve()
    output_dir = tmp_dir / Path(re.sub(r"\.zip$", "", args.output_file_path.name))
    output_dir.mkdir(exist_ok=True, parents=True)
    for index, case_series in cases_df.iterrows():
        download_report(case_series.get("id"),
                        case_series.get("report_id"),
                        output_file_type=args.output_file_type,
                        output_file_path=output_dir / Path(f"{case_series.get('accession_number')}_{case_series.get('report_id')}.{args.output_file_type}"))

    # Zip tmpdir under name cttso-reports
    zip_file_obj = ZipFile(args.output_file_path, "w")

    # Add files to zip
    logger.info("Bundling up data in tmp dir for zip file")
    for file_obj in output_dir.rglob("*"):
        zip_file_obj.write(file_obj, Path(re.sub(r"\.zip$", "", args.output_file_path.name)) / file_obj.absolute().relative_to(output_dir))

    # Close zip file
    zip_file_obj.close()


if __name__ == "__main__":
    main()
