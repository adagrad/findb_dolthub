import inspect
import logging
import sys

import click

from modules.dolt_api import dolt_status

logging.basicConfig(level=logging.INFO)

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
def cli():
    print(dolt_status())


if __name__ == '__main__':
    cli()
