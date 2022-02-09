import click
from cowidev.cmd.testing import click_test
from cowidev.cmd.vax import click_vax


@click.group(name="cowid")
def cli():
    """COVID-19 Data pipeline tool by Our World in Data."""
    pass


# def recursive_help(cmd, parent=None):
#     """From https://stackoverflow.com/a/58018765/5056599"""
#     ctx = click.core.Context(cmd, info_name=cmd.name, parent=parent)
#     print(cmd.get_help(ctx))
#     print()
#     commands = getattr(cmd, "commands", {})
#     for sub in commands.values():
#         recursive_help(sub, ctx)


# @cli.command()
# def dumphelp():
#     recursive_help(cli)


cli.add_command(click_test)
cli.add_command(click_vax)


if __name__ == "__main__":
    cli()
