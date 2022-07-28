import inspect
import logging
import sys

import click

from modules.dolt_api import dolt_checkout_remote_branch, dolt_checkout, dolt_status, dolt_execute_query, dolt_push

logging.basicConfig(level=logging.INFO)

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-b', '--branch', type=str, default="main", help='Branch name to be pulled and checked out')
@click.option('-f', '--feature-branch', type=str, default=None, help='Branch off a feature branch from the specified branch')
@click.option('-q', '--query', type=str, default=None, help='SQL query to execute')
@click.option('-p', '--push', type=str, default=None, help='Push the updated branch')
def cli(repo_database, force_clone, force_init, branch, feature_branch, query, push):
    # checkout repo
    dolt_checkout_remote_branch(repo_database, force_clone, force_init, branch)

    # eventually checkout feature branch
    if feature_branch is not None:
        print(f"create new feature branch {feature_branch} from {branch}")
        dolt_checkout(feature_branch, True)
        print(dolt_status())

    # execute the query
    dolt_execute_query(query)

    # eventually push updated branch
    if push:
        dolt_push(True, commit_message=query)


if __name__ == '__main__':
    cli()
