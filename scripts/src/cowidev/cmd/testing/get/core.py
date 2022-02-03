import click

from cowidev.cmd.testing.get.get_data import main_get_data
from cowidev.testing.countries import MODULES_NAME, MODULES_NAME_BATCH, MODULES_NAME_INCREMENTAL, country_to_module


@click.command(name="get")
@click.option("--parallel/--no-parallel", default=True, help="Parallelize process.", show_default=True)
@click.option("--n-jobs", default=-2, type=int, help="Number of threads to use.", show_default=True)
@click.option("--countries", "-c", default="all", help="List of countries to skip (comma-separated)")
@click.option("--skip-countries", "-s", default=None, help="List of countries to skip (comma-separated)")
def click_test_get(parallel, n_jobs, countries, skip_countries):
    modules = _countries_to_modules(countries)
    if skip_countries:
        skip_countries = countries = [c.strip() for c in skip_countries.split(",")]
        _check_countries(skip_countries)
    main_get_data(
        parallel=parallel,
        n_jobs=n_jobs,
        modules_name=modules,
        skip_countries=skip_countries,
    )


def _countries_to_modules(countries):
    countries = [c.strip() for c in countries.split(",")]
    if len(countries) == 1:
        if countries[0] == "all":
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


def _check_countries(countries):
    countries_wrong = [c for c in countries if c not in country_to_module]
    countries_valid = sorted(list(country_to_module.keys()))
    if countries_wrong:
        raise ValueError(f"Invalid countries: {countries_wrong}. Valid countries are: {countries_valid}")
        # raise ValueError("Invalid country")
