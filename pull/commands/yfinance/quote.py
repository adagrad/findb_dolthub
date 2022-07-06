import inspect
import sys

import click


if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
def cli():
    pass


if __name__ == '__main__':
    cli()
