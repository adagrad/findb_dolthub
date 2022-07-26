import inspect
import subprocess
import sys
import shlex
from time import sleep

import click

from modules.dolt_api import dolt_checkout_remote_branch

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-b', '--branch', type=str, default="main", help='Branch name to be pulled and checked out')
@click.option('--and-exec', type=str, default=None, help='Command to be executed after the server has started (stops server afterwards)')
def cli(repo_database, force_clone, force_init, branch, and_exec):
    # checkout repo
    dolt_checkout_remote_branch(repo_database, force_clone, force_init, branch)

    # start dlt sql server with defaults
    dolt_sql_server_command = ["dolt", "sql-server"]
    sql_server = None

    try:
        if and_exec is not None:
            sql_server = subprocess.Popen(dolt_sql_server_command)
            sleep(1)

            print("server started")
            sub_command_splitter = shlex.shlex(and_exec, posix=True)
            sub_command_splitter.whitespace_split = True
            res = subprocess.run(list(sub_command_splitter))
            sql_server.kill()
            exit(res.returncode)
        else:
            res = subprocess.run(dolt_sql_server_command)
            exit(res.returncode)
    except KeyboardInterrupt as interrupt:
        if sql_server is not None:
            sql_server.kill()
        raise interrupt


if __name__ == '__main__':
    cli()