#!/usr/bin/env python3

"""
Enumerate globals
"""
from enum import Enum


class SampleType(Enum):
    PATIENTCARE = "patientcare"
    CLINICAL_TRIAL = "clinical_trial"
    VALIDATION = "validation"
    PROFICIENCY_TESTING = "proficiency_testing"


class PanelType(Enum):
    MAIN = "tso500_ctDNA_vcf_workflow_university_of_melbourne"
    SUBPANEL = "tso500_ctDNA_vcf_subpanel_workflow_university_of_melbourne"


class Ethnicity(Enum):
    HISPANIC_OR_LATINO = "hispanic_or_latino"
    NOT_HISPANIC_OR_LATINO = "not_hispanic_or_latino"
    NOT_REPORTED = "not_reported"
    UNKNOWN = "unknown"
    # Set default as unknown
    DEFAULT = "unknown"


class Race(Enum):
    AMERICAN_INDIAN_OR_ALASKA_NATIVE = "american_indian_or_alaska_native"
    ASIAN = "asian"
    BLACK_OR_AFRICAN_AMERICAN = "black_or_african_american"
    NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = "native_hawaiian_or_other_pacific_islander"
    NOT_REPORTED = "not_reported"
    UNKNOWN = "unknown"
    WHITE = "white"
    # Set default as unknown
    DEFAULT = "unknown"


class Gender(Enum):
    UNKNOWN = "unknown"
    MALE = "male"
    FEMALE = "female"
    UNSPECIFIED = "unspecified"
    OTHER = "other"
    AMBIGUOUS = "ambiguous"
    NOT_APPLICABLE = "not_applicable"
    # Set default as unknown
    DEFAULT = "unknown"

