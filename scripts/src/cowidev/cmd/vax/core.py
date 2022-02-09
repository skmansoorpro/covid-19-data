import click

from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.cmd.vax.get import click_vax_get
from cowidev.cmd.vax.process import click_vax_process
from cowidev.cmd.vax.generate import click_vax_generate
from cowidev.cmd.vax.export import click_vax_export


@click.group(name="vax", chain=True, cls=OrderedGroup)
def click_vax():
    """COVID-19 Vaccination data pipeline."""
    pass


click_vax.add_command(click_vax_get)
click_vax.add_command(click_vax_process)
click_vax.add_command(click_vax_generate)
click_vax.add_command(click_vax_export)
