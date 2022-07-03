import inspect
import sys

import click

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-i', '--input', type=str, default="hallo", help='Number of greetings.')
def cli(*args, **kwargs):
    print(kwargs)


if __name__ == '__main__':
    cli()
