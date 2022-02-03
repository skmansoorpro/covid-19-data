import click
from cowidev.testing.cmd import click_test


@click.group()
def cli():
    pass


cli.add_command(click_test)
