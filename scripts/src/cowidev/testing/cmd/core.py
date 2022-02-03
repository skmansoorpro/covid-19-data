import click
from cowidev.testing.cmd.get.core import test_get


@click.group(name="test")
def click_test():
    pass


click_test.add_command(test_get)
