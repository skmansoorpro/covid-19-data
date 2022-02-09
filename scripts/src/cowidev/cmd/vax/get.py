import click

from cowidev.cmd.commons.get import main_get_data
from cowidev.cmd.commons.utils import Country2Module, PythonLiteralOption
from cowidev.utils.params import CONFIG
from cowidev.vax.countries import MODULES_NAME, MODULES_NAME_BATCH, MODULES_NAME_INCREMENTAL, country_to_module


@click.command(name="get", short_help="Step 1: Scrape vaccination data from primary sources.")
@click.option(
    "--countries",
    "-c",
    default=CONFIG.pipeline.vaccinations.get.countries,
    # default=[],
    help="List of countries to skip (comma-separated)",
    cls=PythonLiteralOption,
)
@click.option(
    "--skip-countries",
    "-s",
    default=CONFIG.pipeline.vaccinations.get.skip_countries,
    help="List of countries to skip (comma-separated)",
    cls=PythonLiteralOption,
)
@click.option(
    "--optimize/--no-optimize",
    "-O",
    default=False,
    help="Optimize processes based on older logging times.",
    show_default=True,
)
@click.option(
    "--parallel/--no-parallel",
    default=CONFIG.pipeline.vaccinations.get.parallel,
    help="Parallelize process.",
    show_default=True,
)
@click.option(
    "--n-jobs",
    default=CONFIG.pipeline.vaccinations.get.njobs,
    type=int,
    help="Number of threads to use.",
    show_default=True,
)
def click_vax_get(parallel, n_jobs, countries, skip_countries, optimize):
    """Runs scraping scripts to collect the data from the primary sources. Data is exported to project folder
    scripts/output/vaccinations/.

    By default, the default values for OPTIONS are those specified in the configuration file. The configuration file is
    a YAML file with the pipeline settings. Note that the environment variable `OWID_COVID_CONFIG` must be pointing to
    this file. We provide a default config file in the project folder scripts/config.yaml.

    OPTIONS passed via command line will overwrite those from configuration file.

    Example:
    Run the step using default values, from config.yaml file.

        cowid vax get

    Example:
    Run the step only for Australia.

        cowid vax get -c australia

    Example:
    Run the step for all countries except Australia.

        cowid vax get -c all -s australia

    Example:
    Run the step for all incremental processes (can be also done using 'batch').

        cowid vax get -c incremental
    """
    c2m = Country2Module(
        modules_name=MODULES_NAME,
        modules_name_incremental=MODULES_NAME_INCREMENTAL,
        modules_name_batch=MODULES_NAME_BATCH,
        country_to_module=country_to_module,
    )
    print(skip_countries)
    modules = c2m.parse(countries)
    modules_skip = c2m.parse(skip_countries)
    main_get_data(
        parallel=parallel,
        n_jobs=n_jobs,
        modules=modules,
        modules_skip=modules_skip,
        log_header="VAX",
        log_s3_path="s3://covid-19/log/vax-get-data-countries.csv" if optimize else None,
    )
