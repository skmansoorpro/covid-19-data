import click
from cowidev.cmd.testing import click_test
from cowidev.cmd.vax import click_vax


@click.group()
def cli():
    """COVID-19 Data pipeline tool by Our World in Data."""
    pass


cli.add_command(click_test)
cli.add_command(click_vax)


if __name__ == "__main__":
    cli()
