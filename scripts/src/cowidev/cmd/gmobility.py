import click

from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.gmobility.etl import run_etl
from cowidev.gmobility.grapher import run_grapheriser, run_db_updater


@click.group(name="gmobility", chain=True, cls=OrderedGroup)
@click.pass_context
def click_gm(ctx):
    """Google Mobility data pipeline."""
    pass


@click.command(name="generate", short_help="Step 1: Get and generate hospitalization dataset.")
@click.pass_context
def click_gm_generate(ctx):
    """Download and generate our COVID-19 Hospitalization dataset."""
    run_etl()


@click.command(name="grapher-io", short_help="Step 2: Generate grapher-ready files.")
def click_gm_grapherio():
    run_grapheriser()


@click.command(name="grapher-db", short_help="Step 3: Update Grapher database with generated files.")
def click_gm_grapherdb():
    run_db_updater()


click_gm.add_command(click_gm_generate)
click_gm.add_command(click_gm_grapherio)
click_gm.add_command(click_gm_grapherdb)
