#!/usr/bin/env python

"""
Run sample on ICA to PierianDx
"""

from typing import Dict
from pathlib import Path

from utils.args import get_ica_to_pieriandx_args, check_ica_to_pieriandx_args
from utils.globals import ICA_WES_CTTSO_RUN_NAME_REGEX, ICA_WES_CTTSO_RUN_NAME_REGEX_GROUPS
from utils.samplesheet import read_samplesheet, update_samplesheet, write_samplesheet
from utils.ica_gds import collect_and_download_cttso_samplesheet_from_ica_workflow_run, \
    collect_and_download_cttso_files_from_ica_workflow_run, collect_and_download_case_files
from utils.accession import log_informatics_job_by_case
from libica.openapi.libwes import WorkflowRun

from utils.classes import Case, PierianDXSequenceRun
from utils.logging import set_basic_logger
from logging import DEBUG, INFO

import sys

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

    # Get / set check args
    args = get_ica_to_pieriandx_args()
    args = check_ica_to_pieriandx_args(args)

    # Collect files
    library: str
    case: Case
    run: PierianDXSequenceRun
    ica_workflow_run_obj: WorkflowRun
    for library, case, run, ica_workflow_run_obj in zip(args.sample_libraries, args.cases, args.runs, args.ica_workflow_run_objs):
        if ica_workflow_run_obj is None:
            logger.warning(f"Could not get ica workflow run object for case {case.case_accession_number}."
                           f"Skipping case and run creation for this sample")
            continue

        # Downloading the samplesheet
        logger.info(f"Finding and downloading the samplesheet for case {case.case_accession_number} on ICA")
        collect_and_download_cttso_samplesheet_from_ica_workflow_run(ica_workflow_run_obj,
                                                                     output_dir=run.run_dir)

        # Get samplesheet stats
        samplesheet_path = Path(run.run_dir) / "SampleSheet.csv"

        samplesheet_dict: Dict = read_samplesheet(samplesheet_path)

        # Now we have read in the samplesheet, we can get the sample id
        sample_ids = list(set([sample_id
                               for sample_id in samplesheet_dict["Data"]["Sample_ID"].tolist()
                               if library in sample_id]))

        if len(sample_ids) == 0:
            logger.error(f"Could not get sample id from samplesheet data {samplesheet_dict['Data']} for library {library}")
            raise ValueError

        if len(sample_ids) > 1:
            logger.error(f"Found multiple samples that could match this library {library} in samplesheet data {samplesheet_dict['Data']}")
            raise ValueError

        sample_id = sample_ids[0]
        lanes = 1 if "Lane" not in list(samplesheet_dict["Data"].columns) \
            else list(samplesheet_dict["Data"].query(f"Sample_ID=='{sample_id}'")["Lane"])

        # Edit sample sheet
        samplesheet_dict = update_samplesheet(samplesheet_dict, sample_id=sample_id, lanes=lanes)

        # Write out sample sheet
        write_samplesheet(samplesheet_dict, samplesheet_path)

        # And can subsequently download the cttso files
        collect_and_download_cttso_files_from_ica_workflow_run(sample_id,
                                                               ica_workflow_run_obj,
                                                               run.basecalls_dir,
                                                               run.staging_dir)

        # And download the case files
        collect_and_download_case_files(sample_id, ica_workflow_run_obj, run.case_files_dir)

        # Add samplesheet
        case.add_sample_id_to_specimen(sample_id)

        # Use these add the barcode to the case object
        case.add_samplesheet_attributes_to_specimen(samplesheet_dict["Data"])

        # Create the case in PierianDx
        logger.info(f"Creating case object on PierianDx for case {case.case_accession_number}")
        case()

        # Update the run name to include the runs flowcell id (from the ica workflow object)
        run_flowcell_id = ICA_WES_CTTSO_RUN_NAME_REGEX.match(ica_workflow_run_obj.name).\
            group(ICA_WES_CTTSO_RUN_NAME_REGEX_GROUPS.get("date_stamp"))
        run.rename_run(new_run_name=f"{case.case_accession_number}_{run_flowcell_id}")

        # Create the run in pierian
        logger.info(f"Creating run object on PierianDx for case {case.case_accession_number}")
        run()

        # Add the run id to the case object
        case.add_run_to_case([run])

        # Get the case file
        logger.info("Uploading failed exon coverage case file")
        case.upload_case_files()

        # Upload files to s3
        logger.info(f"Uploading cttso files to PierianDx s3 bucket for case {case.case_accession_number}")
        run.upload_to_s3_bucket()

        # Launch the informatics job:
        logger.info("Launching informatics job for case")
        case.launch_informatics_job()

    logger.info("Writing out cases")
    log_informatics_job_by_case(args.cases)


if __name__ == "__main__":
    main()
