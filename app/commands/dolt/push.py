import inspect
import sys

import click

from modules.dolt_api import dolt_push

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-a', '--stage-all', default=False, is_flag=True, help='Stage all changes')
@click.option('-m', '--commit-message', type=str, default="merge", help='Commit message')
def cli(stage_all, commit_message):
    dolt_push(stage_all, commit_message)


if __name__ == '__main__':
    cli()
