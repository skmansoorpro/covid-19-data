import click

from cowidev.utils.params import CONFIG
from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.cmd.testing import click_test
from cowidev.cmd.vax import click_vax
from cowidev.cmd.hosp import click_hosp
from cowidev.cmd.jhu import click_jhu


@click.group(name="cowid", cls=OrderedGroup)
@click.option(
    "--parallel/--no-parallel",
    default=CONFIG.execution.parallel,
    help="Parallelize process.",
    show_default=True,
)
@click.option(
    "--n-jobs",
    default=CONFIG.execution.njobs,
    type=int,
    help="Number of threads to use.",
    show_default=True,
)
@click.pass_context
def cli(ctx, parallel, n_jobs):
    """COVID-19 Data pipeline tool by Our World in Data."""
    ctx.ensure_object(dict)
    ctx.obj["parallel"] = parallel
    ctx.obj["n_jobs"] = n_jobs


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
cli.add_command(click_hosp)
cli.add_command(click_jhu)


if __name__ == "__main__":
    cli()
