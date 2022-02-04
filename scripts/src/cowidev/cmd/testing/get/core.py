import click

from cowidev.cmd.testing.get.get_data import main_get_data
from cowidev.cmd.config import CONFIG
from cowidev.cmd.utils import PythonLiteralOption, normalize_country_name
from cowidev.testing.countries import MODULES_NAME, MODULES_NAME_BATCH, MODULES_NAME_INCREMENTAL, country_to_module


@click.command(name="get", short_help="Scrape testing data from primary sources.")
@click.option(
    "--parallel/--no-parallel",
    default=CONFIG.pipeline.testing.get.parallel,
    help="Parallelize process.",
    show_default=True,
)
@click.option(
    "--n-jobs",
    default=CONFIG.pipeline.testing.get.njobs,
    type=int,
    help="Number of threads to use.",
    show_default=True,
)
@click.option(
    "--countries",
    "-c",
    default=CONFIG.pipeline.testing.get.countries,
    # default=[],
    help="List of countries to skip (comma-separated)",
    cls=PythonLiteralOption,
)
@click.option(
    "--skip-countries",
    "-s",
    default=CONFIG.pipeline.testing.get.skip_countries,
    help="List of countries to skip (comma-separated)",
    cls=PythonLiteralOption,
)
def click_test_get(parallel, n_jobs, countries, skip_countries):
    """Runs scraping scripts to collect the data from the primary sources. Data is exported to project folder
    scripts/output/testing/.

    By default, the default values for OPTIONS are those specified in the configuration file. The configuration file is
    a YAML file with the pipeline settings. Note that the environment variable `OWID_COVID_CONFIG` must be pointing to
    this file. We provide a default config file in the project folder scripts/config.yaml.

    OPTIONS passed via command line will overwrite those from configuration file.

    Example:
    Run the step using default values, from config.yaml file.

        cowid test get

    Example:
    Run the step only for Australia.

        cowid test get -c australia

    Example:
    Run the step for all countries except Australia.

        cowid test get -c all -s australia

    Example:
    Run the step for all incremental processes (can be also done using 'batch').

        cowid test get -c incremental
    """
    modules = _countries_to_modules(countries)
    modules_skip = _countries_to_modules(skip_countries)
    main_get_data(
        parallel=parallel,
        n_jobs=n_jobs,
        modules=modules,
        modules_skip=modules_skip,
    )


def _comma_separated_to_list(x):
    return [c for c in x.split(",")]


def _countries_to_modules(countries):
    if isinstance(countries, str):
        countries = _comma_separated_to_list(countries)
    countries = [normalize_country_name(c) for c in countries]
    if len(countries) == 1:
        if countries[0].lower() == "all":
            return MODULES_NAME
        elif countries[0] == "incremental":
            return MODULES_NAME_INCREMENTAL
        elif countries[0] == "batch":
            return MODULES_NAME_BATCH
    if len(countries) >= 1:
        # Verify validity of countries
        _check_countries(countries)
        # Get module equivalent names
        modules = [country_to_module[country] for country in countries]
        return modules
    return []


def _check_countries(countries):
    countries_wrong = [c for c in countries if c not in country_to_module]
    countries_valid = sorted(list(country_to_module.keys()))
    if countries_wrong:
        raise ValueError(f"Invalid countries: {countries_wrong}. Valid countries are: {countries_valid}")
        # raise ValueError("Invalid country")
