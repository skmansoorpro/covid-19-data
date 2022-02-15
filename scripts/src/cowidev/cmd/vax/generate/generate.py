import click

from cowidev.cmd.vax.generate.utils import DatasetGenerator


@click.command(name="generate", short_help="Step 3: Generate vaccination dataset.")
def click_vax_generate():
    # Select columns
    generator = DatasetGenerator()
    generator.run()
