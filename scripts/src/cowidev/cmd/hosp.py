import click

from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.hosp.etl import run_etl
from cowidev.hosp.grapher import run_db_updater, run_grapheriser


@click.group(name="hosp", chain=True, cls=OrderedGroup)
@click.pass_context
def click_hosp(ctx):
    """COVID-19 Hospitalization data pipeline."""
    pass


@click.command(name="generate", short_help="Step 1: Get and generate hospitalization dataset.")
@click.pass_context
def click_hosp_generate(ctx):
    """Runs scraping scripts to collect the data from the primary sources, transforms it and exports the result to
    public/data/hospitalizations/.

    By default, the default values for OPTIONS are those specified in the configuration file. The configuration file is
    a YAML file with the pipeline settings. Note that the environment variable `OWID_COVID_CONFIG` must be pointing to
    this file. We provide a default config file in the project folder scripts/config.yaml.

    OPTIONS passed via command line will overwrite those from configuration file.
    """
    run_etl(ctx.obj["parallel"], ctx.obj["n_jobs"])


@click.command(name="grapher-io", short_help="Step 2: Generate grapher-ready files.")
def click_hosp_grapherio():
    run_grapheriser()


@click.command(name="grapher-db", short_help="Step 3: Update Grapher database with generated files.")
def click_hosp_grapherdb():
    run_db_updater()


click_hosp.add_command(click_hosp_generate)
click_hosp.add_command(click_hosp_grapherio)
click_hosp.add_command(click_hosp_grapherdb)
