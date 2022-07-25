import inspect
import sys

import click

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-b', '--branch', type=str, default="main", help='Branch name to be pulled and checked out')
@click.option('--and', type=str, default=None, help='Command to be executed after the server has started (stops server afterwards)')
def cli(time, output, repo_database, known_symbols, dolt_load):
    pass


if __name__ == '__main__':
    cli()
