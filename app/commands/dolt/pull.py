import inspect
import sys

import click

from modules.dolt_api import dolt_checkout_remote_branch

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-b', '--branch', type=str, default="main", help='Remote branch name to be pulled and checked out')
def cli(repo_database, force_clone, force_init, branch):
    dolt_checkout_remote_branch(repo_database, force_clone, force_init, branch)


if __name__ == '__main__':
    cli()
