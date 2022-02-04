import click
from cowidev.cmd.testing import click_test


@click.group()
def cli():
    """COVID-19 Data pipeline tool by Our World in Data."""
    pass


cli.add_command(click_test)


if __name__ == "__main__":
    cli()
