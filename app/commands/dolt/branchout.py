import inspect
import sys

import click

from modules.dolt_api import dolt_checkout_remote_branch, dolt_current_branch, dolt_checkout

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-s', '--source-branch', type=str, default="main", help='Branch name to be crated and checked out')
@click.option('-b', '--branch', type=str, required=True, help='Branch name to be crated and checked out')
def cli(repo_database, force_clone, force_init, source_branch, branch):
    dolt_checkout_remote_branch(repo_database, force_clone, force_init, source_branch)
    dolt_checkout(branch, new=True)

    assert dolt_current_branch() == branch, f"Failed to create the new branch {branch}, still on {dolt_current_branch()}"


if __name__ == '__main__':
    cli()
