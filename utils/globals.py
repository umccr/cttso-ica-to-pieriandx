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

ICA_WES_CTTSO_RUN_NAME_REGEX = re.compile(r"umccr__automated__dragen_tso_ctdna__"
                                          r"(\d{6})_(\w{6})_(\d{4})_(\w+)__\S+__(L\d{7})__\d+")
ICA_WES_CTTSO_RUN_NAME_REGEX_GROUPS = {
    "date": 1,
    "machine_id": 2,
    "run_number": 3,
    "flowcell": 4,
    "library": 5
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

#################
# OUTPUT GLOBALS
#################
OUTPUT_STATS_FILE = "ids_by_case.csv"
