import inspect
import logging
import sys

import click

from modules.dolt_api import dolt_merge

logging.basicConfig(level=logging.INFO)

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-s', '--source-branch', type=str, required=True, help='Branch name to be merged into target')
@click.option('-t', '--target-branch', type=str, default="main", help='Branch name where source branch gets merged into')
@click.option('-m', '--commit-message', type=str, default="merge", help='Commit message')
@click.option('-p', '--push', default=False, is_flag=True, help='Push the updated branch')
@click.option('--delete-source', default=False, is_flag=True, help='Delete source branch after merge/push')
@click.option('--theirs', default=False, is_flag=True, help='Resole conflicts using theirs')
@click.option('--ours', default=False, is_flag=True, help='Resole conflicts using ours')
def cli(repo_database, force_clone, force_init, source_branch, target_branch, commit_message, push, delete_source, theirs, ours):
    dolt_merge(repo_database, force_clone, force_init, source_branch, target_branch, commit_message, push, delete_source, theirs, ours)


if __name__ == '__main__':
    cli()
