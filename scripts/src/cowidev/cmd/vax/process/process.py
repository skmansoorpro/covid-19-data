import os

import pandas as pd

import click
from pandas.core.base import DataError
from pandas.errors import ParserError

from cowidev import PATHS
from cowidev.utils.log import get_logger, print_eoe
from cowidev.cmd.vax.process.utils import process_location, VaccinationGSheet
from cowidev.utils.params import CONFIG, SECRETS


logger = get_logger()


@click.command(name="process", short_help="Step 2: Process scraped vaccination data from primary sources.")
def click_vax_process():
    """Process data in folder scripts/output/vaccinations/.

    By default, the default values for OPTIONS are those specified in the configuration file. The configuration file is
    a YAML file with the pipeline settings. Note that the environment variable `OWID_COVID_CONFIG` must be pointing to
    this file. We provide a default config file in the project folder scripts/config.yaml.

    OPTIONS passed via command line will overwrite those from configuration file.

    Example:
    Process all country data.

        cowid vax process
    """
    main_process_data(
        skip_complete=CONFIG.pipeline.vaccinations.process.skip_complete,
        skip_monotonic=CONFIG.pipeline.vaccinations.process.skip_monotonic_check,
        skip_anomaly=CONFIG.pipeline.vaccinations.process.skip_anomaly_check,
    )


def main_process_data(
    skip_complete: list = None,
    skip_monotonic: dict = {},
    skip_anomaly: dict = {},
):
    # TODO: Generalize
    print("-- Processing data... --")
    # Get data from sheets
    logger.info("Getting data from Google Spreadsheet...")
    gsheet = VaccinationGSheet()
    df_manual_list = gsheet.df_list()

    # Get automated-country data
    logger.info("Getting data from output...")
    automated = gsheet.automated_countries
    filepaths_auto = [PATHS.out_vax(country) for country in automated]
    df_auto_list = [read_csv(filepath) for filepath in filepaths_auto]

    # Concatenate
    vax = df_manual_list + df_auto_list

    # Check that no location is present in both manual and automated data
    manual_locations = set([df.location[0] for df in df_manual_list])
    auto_locations = os.listdir(PATHS.INTERNAL_OUTPUT_VAX_MAIN_DIR)
    auto_locations = set([loc.replace(".csv", "") for loc in auto_locations])
    common_locations = auto_locations.intersection(manual_locations)
    if len(common_locations) > 0:
        raise DataError(f"The following locations have data in both output/main_data and GSheet: {common_locations}")

    # vax = [v for v in vax if v.location.iloc[0] == "Pakistan"]  # DEBUG
    # Process locations
    def _process_location(df):
        monotonic_check_skip = skip_monotonic.get(df.loc[0, "location"], [])
        anomaly_check_skip = skip_anomaly.get(df.loc[0, "location"], [])
        return process_location(df, monotonic_check_skip, anomaly_check_skip)

    logger.info("Processing and exporting data...")
    vax_valid = []
    for df in vax:
        if "location" not in df:
            raise ValueError(f"Column `location` missing. df: {df.tail(5)}")
        country = df.loc[0, "location"]
        if country.lower() not in skip_complete:
            df = _process_location(df)
            vax_valid.append(df)
            # Export
            df.to_csv(PATHS.out_vax(country, public=True), index=False)
            logger.info(f"{country}: SUCCESS ✅")
        else:
            logger.info(f"{country}: SKIPPED 🚧")
    df = pd.concat(vax_valid).sort_values(by=["location", "date"])
    df.to_csv(PATHS.INTERNAL_TMP_VAX_MAIN_FILE, index=False)
    gsheet.metadata.to_csv(PATHS.INTERNAL_TMP_VAX_META_FILE, index=False)
    logger.info("Exported ✅")
    print_eoe()


def read_csv(filepath):
    try:
        return pd.read_csv(filepath)
    except:
        raise ParserError(f"Error tokenizing data from file {filepath}")
