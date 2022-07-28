import inspect
import sys

import click

from modules.dolt_api import dolt_push

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-a', '--add-changes', type=str, default=None, help='Stage only specified changes seperated by blanks')
@click.option('--stage-all', default=False, is_flag=True, help='Stage all changes')
@click.option('-m', '--commit-message', type=str, default="merge", help='Commit message')
def cli(add_changes, stage_all, commit_message):
    dolt_push(add_changes.split(" ") if add_changes is not None else stage_all, commit_message)


if __name__ == '__main__':
    cli()
