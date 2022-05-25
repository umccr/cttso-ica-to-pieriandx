#!/usr/bin/env python

"""
Globals
"""
from pathlib import Path
import re
from utils.enums import Gender, Ethnicity, Race

#######################
# INPUTS GLOBALS
#######################

MANDATORY_INPUT_COLUMNS = [
    "sample_type",
    "indication",
    # "disease",  Removed since we check for disease_name too
    "accession_number",
    "study_id",
    "participant_id",
    # "specimen_type",  Removed since we check for specimen_type_name too
    "external_specimen_id",
    "date_accessioned",
    "date_collected",
    "date_received"
]

DISEASE_CSV = Path(__file__).parent.parent.absolute() / Path("references") / Path("disease.csv")

SPECIMEN_TYPE_CSV = Path(__file__).parent.parent.absolute() / Path("references") / Path("specimen.csv")

OPTIONAL_DEFAULTS = {
    "gender": Gender.DEFAULT.value,
    "ethnicity": Ethnicity.DEFAULT.value,
    "race": Race.DEFAULT.value,
    "specimen_label": "primarySpecimen"
}

ACCESSION_FORMAT_REGEX = re.compile(r"(SBJ\d{5})_(L\d{7})")

#####################
# ICA GLOBALS
#####################

ICA_WES_CTTSO_RUN_NAME_REGEX = re.compile(r"umccr__automated__tso_ctdna_tumor_only__"
                                          r"(\w+)__(\w+)__(\w+)")
ICA_WES_CTTSO_RUN_NAME_REGEX_GROUPS = {
    "subject": 1,
    "library": 2,
    "date_stamp": 3,
}

ICA_WES_MAX_PAGE_SIZE = 1000
ICA_GDS_MAX_PAGE_SIZE = 1000

#########################
# PIERIANDX GLOBALS
#########################


# Used when creting a case
DAG = {
    "name": "cromwell_tso500_ctdna_workflow_1.0.1",
    "description": "tso500_ctdna_workflow"
}

# All data is de-identified
IDENTIFIED = False

# The panel name is also set
PANEL_NAME = "tso500_ctDNA_vcf_workflow_university_of_melbourne"

# The name of the cttso files
CTTSO_FILE_SUFFIXES = [
    "_Fusions.csv",
    "_MergedSmallVariants.genome.vcf.gz",
    "_CopyNumberVariants.vcf.gz",
    ".tmb.json.gz",
    ".msi.json.gz"
]

CTTSO_SAMPLESHEET_NAME = "SampleSheet_Intermediate.csv"

CTTSO_COVERAGE_FILE_SUFFIX = "_Failed_Exon_coverage_QC.txt"

MAX_CASE_FILE_UPLOAD_ATTEMPTS = 50
MAX_CASE_CREATION_ATTEMPTS = 50
MAX_RUN_CREATION_ATTEMPTS = 50
MAX_JOB_CREATION_ATTEMPTS = 50
MAX_ATTEMPTS_GET_CASES = 50
CASE_FILE_RETRY_TIME = 20
CASE_CREATION_RETRY_TIME = 20
RUN_CREATION_RETRY_TIME = 20
JOB_CREATION_RETRY_TIME = 20
LIST_CASES_RETRY_TIME = 20


#################
# OUTPUT GLOBALS
#################
OUTPUT_STATS_FILE = "ids_by_case.csv"
