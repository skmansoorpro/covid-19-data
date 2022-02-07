import click

from cowidev.cmd.vax.get import click_vax_get
from cowidev.cmd.vax.process import click_vax_process


@click.group(name="vax", chain=True)
def click_vax():
    """COVID-19 Vaccination data pipeline."""
    pass


click_vax.add_command(click_vax_get)
click_vax.add_command(click_vax_process)
