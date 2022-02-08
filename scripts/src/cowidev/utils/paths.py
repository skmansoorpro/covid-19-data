import os
from dotenv import load_dotenv
from dataclasses import make_dataclass


def _get_s3_dir():
    _s3_dir = {"INTERNAL": "internal", "INTERNAL_VAX": "internal/vax", "VAX_ICE": "s3://covid-19/internal/vax/ice"}
    B = make_dataclass("Bucket", _s3_dir.keys(), frozen=True)
    return B(**_s3_dir)


def _get_scripts_dir(project_dir: str):
    # SCRIPTS DIRECTORY
    _SCRIPTS_DIR = os.path.join(project_dir, "scripts")
    _SCRIPTS_OLD_DIR = os.path.join(_SCRIPTS_DIR, "scripts")
    _SCRIPTS_GRAPHER_DIR = os.path.join(_SCRIPTS_DIR, "grapher")
    _SCRIPTS_INPUT_DIR = os.path.join(_SCRIPTS_DIR, "input")
    _SCRIPTS_OUTPUT_DIR = os.path.join(_SCRIPTS_DIR, "output")
    _SCRIPTS_OUTPUT_VAX_DIR = os.path.join(_SCRIPTS_OUTPUT_DIR, "vaccinations")
    _SCRIPTS_OUTPUT_HOSP_DIR = os.path.join(_SCRIPTS_OUTPUT_DIR, "hospitalizations")
    _SCRIPTS_OUTPUT_TEST_DIR = os.path.join(_SCRIPTS_OUTPUT_DIR, "testing")
    _SCRIPTS_DOCS_DIR = os.path.join(_SCRIPTS_DIR, "docs")
    _INPUT_FOLDER_LS = [
        "bsg",
        "gbd",
        "jh",
        "reproduction",
        "wb",
        "cdc",
        "gmobility",
        "jhu",
        "sweden",
        "who",
        "ecdc",
        "iso",
        "owid",
        "un",
        "yougov",
    ]

    _scripts_dirs = {
        "OLD": _SCRIPTS_OLD_DIR,
        "GRAPHER": _SCRIPTS_GRAPHER_DIR,
        "INPUT": _SCRIPTS_INPUT_DIR,
        **{f"INPUT_{e.upper()}": os.path.join(_SCRIPTS_INPUT_DIR, e) for e in _INPUT_FOLDER_LS},
        "OUTPUT": _SCRIPTS_OUTPUT_DIR,
        "OUTPUT_HOSP": _SCRIPTS_OUTPUT_HOSP_DIR,
        "OUTPUT_HOSP_MAIN": os.path.join(_SCRIPTS_OUTPUT_HOSP_DIR, "main_data"),
        "OUTPUT_HOSP_META": os.path.join(_SCRIPTS_OUTPUT_HOSP_DIR, "metadata"),
        "OUTPUT_VAX": _SCRIPTS_OUTPUT_VAX_DIR,
        "OUTPUT_VAX_AGE": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "by_age_group"),
        "OUTPUT_VAX_MANUFACT": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "by_manufacturer"),
        "OUTPUT_VAX_MAIN": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "main_data"),
        "OUTPUT_VAX_META": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "metadata"),
        "OUTPUT_VAX_META_AGE": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "metadata", "locations-age.csv"),
        "OUTPUT_VAX_META_MANUFACT": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "metadata", "locations-manufacturer.csv"),
        "OUTPUT_VAX_PROPOSALS": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "proposals"),
        "OUTPUT_VAX_LOG": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "log"),
        "OUTPUT_TEST": _SCRIPTS_OUTPUT_TEST_DIR,
        "OUTPUT_TEST_MAIN": os.path.join(_SCRIPTS_OUTPUT_TEST_DIR, "main_data"),
        # "OUTPUT_TEST_MAIN": os.path.join(_SCRIPTS_OLD_DIR, "testing", "automated_sheets"),
        "DOCS": _SCRIPTS_DOCS_DIR,
        "DOCS_VAX": os.path.join(_SCRIPTS_DOCS_DIR, "vaccination"),
        "TMP": os.path.join(_SCRIPTS_DIR, "tmp"),
        "TMP_VAX": os.path.join(_SCRIPTS_DIR, "vaccinations.preliminary.csv"),
        "TMP_VAX_META": os.path.join(_SCRIPTS_DIR, "metadata.preliminary.csv"),
    }
    _scripts_dirs = {**_scripts_dirs, "INPUT_CDC_VAX": os.path.join(_scripts_dirs["INPUT_CDC"], "vaccinations")}
    B = make_dataclass("Bucket", _scripts_dirs.keys(), frozen=True)
    B.__repr__ = lambda _: _SCRIPTS_DIR
    return B(**_scripts_dirs)


def _get_data_dir(project_dir: str):
    _PUBLIC_DIR = os.path.join(project_dir, "public")
    _DATA_DIR = os.path.join(_PUBLIC_DIR, "data")
    _DATA_FOLDER_LS = [
        "ecdc",
        "excess_mortality",
        "internal",
        "jhu",
        "latest",
        "testing",
        "vaccinations",
        "variants",
        "who",
        "hospitalizations",
    ]

    _data_dirs = {
        "PUBLIC": _PUBLIC_DIR,
        **{f"{e.upper()}": os.path.join(_DATA_DIR, e) for e in _DATA_FOLDER_LS},
    }

    _data_dirs = {
        **_data_dirs,
        "TIMESTAMP": os.path.join(_data_dirs["INTERNAL"], "timestamp"),
        "VAX_COUNTRY": os.path.join(_data_dirs["VACCINATIONS"], "country_data"),
        "VAX_META_MANUFACT": os.path.join(_data_dirs["VACCINATIONS"], "locations-manufacturer.csv"),
        "VAX_META_AGE": os.path.join(_data_dirs["VACCINATIONS"], "locations-age.csv"),
        "VAX_META": os.path.join(_data_dirs["VACCINATIONS"], "locations.csv"),
    }

    B = make_dataclass(
        "Bucket",
        _data_dirs.keys(),
        frozen=True,
    )
    B.__repr__ = lambda _: _DATA_DIR
    return B(**_data_dirs)


def _get_project_dir_from_env(err: bool = False):
    project_dir = os.environ.get("OWID_COVID_PROJECT_DIR")
    if project_dir is None:  # err and
        raise ValueError("Please set environment variable ${OWID_COVID_PROJECT_DIR}.")
    return project_dir


PROJECT_DIR = _get_project_dir_from_env()
SCRIPTS = _get_scripts_dir(PROJECT_DIR)
DATA = _get_data_dir(PROJECT_DIR)
S3 = _get_s3_dir()


def out_vax(country: str, public=False, age=False, manufacturer=False, proposal=False):
    if not public:
        if age:
            return os.path.join(SCRIPTS.OUTPUT_VAX_AGE, f"{country}.csv")
        elif manufacturer:
            return os.path.join(SCRIPTS.OUTPUT_VAX_MANUFACT, f"{country}.csv")
        elif proposal:
            return os.path.join(SCRIPTS.OUTPUT_VAX_PROPOSALS, f"{country}.csv")
        return os.path.join(SCRIPTS.OUTPUT_VAX_MAIN, f"{country}.csv")
    else:
        return os.path.join(DATA.VAX_COUNTRY, f"{country}.csv")


CONFIG_FILE_NEW = os.path.join(str(SCRIPTS), "config_new.yaml")
CONFIG_FILE = os.path.join(str(SCRIPTS), "config.yaml")
CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "owid")

SECRETS_FILE = os.environ.get("OWID_COVID_SECRETS", None)


### New ################################################################################################################


# Public ########
PUBLIC_DIR = os.path.join(PROJECT_DIR, "public")
DATA_DIR = os.path.join(PUBLIC_DIR, "data")
DATA_READ_FILE = os.path.join(DATA_DIR, "README.md")
DATA_CODEBOOK_FILE = os.path.join(DATA_DIR, "owid-covid-codebook.csv")
DATA_MAIN_FILE = os.path.join(DATA_DIR, "owid-covid-data.csv")

## Data excess mortality
DATA_XM_DIR = os.path.join(DATA_DIR, "excess_mortality")
DATA_XM_MAIN_FILE = os.path.join(DATA_XM_DIR, "excess_mortality.csv")
DATA_XM_ECON_FILE = os.path.join(DATA_XM_DIR, "excess_mortality_economist_estimates.csv")
DATA_XM_READ_FILE = os.path.join(DATA_XM_DIR, "README.md")
## Data hospitalizations
DATA_HOSP_DIR = os.path.join(DATA_DIR, "hospitalizations")
DATA_HOSP_MAIN_FILE = os.path.join(DATA_HOSP_DIR, "covid-hospitalizations.csv")
DATA_HOSP_META_FILE = os.path.join(DATA_HOSP_DIR, "locations.csv")
DATA_HOSP_READ_FILE = os.path.join(DATA_HOSP_DIR, "README.md")
## Data testing
DATA_TEST_DIR = os.path.join(DATA_DIR, "testing")
DATA_TEST_MAIN_FILE = os.path.join(DATA_TEST_DIR, "covid-testing-all-observations.csv")
## Data vaccinations
DATA_VAX_DIR = os.path.join(DATA_DIR, "vaccinations")
DATA_VAX_COUNTRY_DIR = os.path.join(DATA_VAX_DIR, "country_data")
DATA_VAX_MAIN_FILE = os.path.join(DATA_VAX_DIR, "vaccinations.csv")
DATA_VAX_META_FILE = os.path.join(DATA_VAX_DIR, "locations.csv")
DATA_VAX_US_FILE = os.path.join(DATA_VAX_DIR, "us_state_vaccinations.csv")
DATA_VAX_MAIN_JSON_FILE = os.path.join(DATA_VAX_DIR, "vaccinations.json")
DATA_VAX_MANUFACT_FILE = os.path.join(DATA_VAX_DIR, "vaccinations-by-manufacturer.csv")
DATA_VAX_META_MANUFACT_FILE = os.path.join(DATA_VAX_DIR, "locations-manufacturer.csv")
DATA_VAX_AGE_FILE = os.path.join(DATA_VAX_DIR, "vaccinations-by-age-group.csv")
DATA_VAX_META_AGE_FILE = os.path.join(DATA_VAX_DIR, "locations-age.csv")
DATA_VAX_READ_FILE = os.path.join(DATA_VAX_DIR, "README.md")
## Data jhu
DATA_JHU_DIR = os.path.join(DATA_DIR, "jhu")
DATA_JHU_CASES_FILE = os.path.join(DATA_JHU_DIR, "total_cases.csv")
DATA_JHU_DEATHS_FILE = os.path.join(DATA_JHU_DIR, "total_deaths.csv")
## Data others
DATA_INTERNAL_DIR = os.path.join(DATA_DIR, "internal")
DATA_LATEST_DIR = os.path.join(DATA_DIR, "latest")
## Timestamps
DATA_TIMESTAMP_DIR = os.path.join(DATA_INTERNAL_DIR, "timestamp")
DATA_TIMESTAMP_HOSP_FILE = os.path.join(DATA_TIMESTAMP_DIR, "owid-covid-data-last-updated-timestamp-hosp.txt")
DATA_TIMESTAMP_ROOT_FILE = os.path.join(DATA_TIMESTAMP_DIR, "owid-covid-data-last-updated-timestamp-root.txt")
DATA_TIMESTAMP_TEST_FILE = os.path.join(DATA_TIMESTAMP_DIR, "owid-covid-data-last-updated-timestamp-test.txt")
DATA_TIMESTAMP_VAX_FILE = os.path.join(DATA_TIMESTAMP_DIR, "owid-covid-data-last-updated-timestamp-vaccinations.txt")
DATA_TIMESTAMP_XM_FILE = os.path.join(DATA_TIMESTAMP_DIR, "owid-covid-data-last-updated-timestamp-xm.txt")
DATA_TIMESTAMP_OLD_FILE = os.path.join(DATA_TIMESTAMP_DIR, "owid-covid-data-last-updated-timestamp.txt")

# Internal ########
INTERNAL_DIR = os.path.join(PROJECT_DIR, "scripts")

## Output
INTERNAL_TMP_DIR = os.path.join(INTERNAL_DIR, "tmp")
## Output
INTERNAL_OUTPUT_DIR = os.path.join(INTERNAL_DIR, "output")
### Output vax
INTERNAL_OUTPUT_VAX_DIR = os.path.join(INTERNAL_OUTPUT_DIR, "vaccinations")
INTERNAL_OUTPUT_VAX_TABLE_FILE = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "source_table.html")
INTERNAL_OUTPUT_VAX_AUTOM_FILE = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "automation_state.csv")
INTERNAL_OUTPUT_VAX_MAIN_DIR = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "main_data")
INTERNAL_OUTPUT_VAX_MANUFACT_DIR = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "by_manufacturer")
INTERNAL_OUTPUT_VAX_AGE_DIR = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "by_age_group")
INTERNAL_OUTPUT_VAX_META_DIR = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "metadata")
INTERNAL_OUTPUT_VAX_META_AGE_FILE = os.path.join(INTERNAL_OUTPUT_VAX_META_DIR, "locations-age.csv")
INTERNAL_OUTPUT_VAX_META_MANUFACT_FILE = os.path.join(INTERNAL_OUTPUT_VAX_META_DIR, "locations-manufacturer.csv")
INTERNAL_OUTPUT_VAX_LOG_DIR = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "log")
INTERNAL_OUTPUT_VAX_PROPOSALS_DIR = os.path.join(INTERNAL_OUTPUT_VAX_DIR, "proposals")
### Output test
INTERNAL_OUTPUT_TEST_DIR = os.path.join(INTERNAL_OUTPUT_DIR, "testing")
INTERNAL_OUTPUT_TEST_MAIN_DIR = os.path.join(INTERNAL_OUTPUT_TEST_DIR, "main_data")
### Output hospitalizations
INTERNAL_OUTPUT_HOSP_DIR = os.path.join(INTERNAL_OUTPUT_DIR, "hospitalizations")
INTERNAL_OUTPUT_HOSP_MAIN_DIR = os.path.join(INTERNAL_OUTPUT_HOSP_DIR, "main_data")
INTERNAL_OUTPUT_HOSP_META_DIR = os.path.join(INTERNAL_OUTPUT_HOSP_DIR, "metadata")

## Input
INTERNAL_INPUT_DIR = os.path.join(INTERNAL_DIR, "input")
### Input UN
INTERNAL_INPUT_BSG_DIR = os.path.join(INTERNAL_INPUT_DIR, "bsg")
INTERNAL_INPUT_BSG_FILE = os.path.join(INTERNAL_INPUT_BSG_DIR, "latest.csv")
INTERNAL_INPUT_CDC_VAX_DIR = os.path.join(INTERNAL_INPUT_DIR, "cdc", "vaccinations")
INTERNAL_INPUT_GMOB_DIR = os.path.join(INTERNAL_INPUT_DIR, "gmobility")
INTERNAL_INPUT_GMOB_STD_FILE = os.path.join(INTERNAL_INPUT_GMOB_DIR, "gmobility_country_standardized.csv")
INTERNAL_INPUT_ISO_DIR = os.path.join(INTERNAL_INPUT_DIR, "iso")
INTERNAL_INPUT_ISO_FILE = os.path.join(INTERNAL_INPUT_ISO_DIR, "iso3166_1_alpha_3_codes.csv")
INTERNAL_INPUT_JHU_DIR = os.path.join(INTERNAL_INPUT_DIR, "jhu")
INTERNAL_INPUT_JHU_STD_FILE = os.path.join(INTERNAL_INPUT_JHU_DIR, "jhu_country_standardized.csv")
INTERNAL_INPUT_OWID_DIR = os.path.join(INTERNAL_INPUT_DIR, "owid")
INTERNAL_INPUT_OWID_POPULATION_SUB_FILE = os.path.join(INTERNAL_INPUT_OWID_DIR, "subnational_population_2020.csv")
INTERNAL_INPUT_OWID_CONT_FILE = os.path.join(INTERNAL_INPUT_OWID_DIR, "continents.csv")
INTERNAL_INPUT_OWID_INCOME_FILE = os.path.join(INTERNAL_INPUT_OWID_DIR, "income_groups_complement.csv")
INTERNAL_INPUT_OWID_EU_FILE = os.path.join(INTERNAL_INPUT_OWID_DIR, "eu_countries.csv")
INTERNAL_INPUT_OWID_ANNOTATIONS_FILE = os.path.join(INTERNAL_DIR, "scripts", "annotations_internal.yaml")
INTERNAL_INPUT_OWID_READ_FILE = os.path.join(INTERNAL_DIR, "scripts", "README.md.template")
INTERNAL_INPUT_UN_DIR = os.path.join(INTERNAL_INPUT_DIR, "un")
INTERNAL_INPUT_UN_POPULATION_FILE = os.path.join(INTERNAL_INPUT_UN_DIR, "population_latest.csv")
INTERNAL_INPUT_WB_DIR = os.path.join(INTERNAL_INPUT_DIR, "wb")
INTERNAL_INPUT_WB_INCOME_FILE = os.path.join(INTERNAL_INPUT_WB_DIR, "income_groups.csv")

## Grapher
INTERNAL_GRAPHER_DIR = os.path.join(INTERNAL_DIR, "grapher")

## Temporary
INTERNAL_TMP_VAX_MAIN_FILE = os.path.join(INTERNAL_DIR, "vaccinations.preliminary.csv")
INTERNAL_TMP_VAX_META_FILE = os.path.join(INTERNAL_DIR, "metadata.preliminary.csv")
