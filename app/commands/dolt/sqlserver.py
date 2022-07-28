import inspect
import os
import shlex
import signal
import subprocess
import sys
from time import sleep

import click

from modules.dolt_api import dolt_checkout_remote_branch, dolt_checkout, dolt_status, dolt_push, dolt_current_branch

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


@click.command()
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-c', '--force-clone', default=False, is_flag=True, help='Force a clone if the working directory is not a dolt repository')
@click.option('-i', '--force-init', default=False, is_flag=True, help='Force a repo init and add origin if the working directory is not a dolt repository')
@click.option('-b', '--branch', type=str, default="main", help='Branch name to be pulled and checked out')
@click.option('-f', '--feature-branch', type=str, default=None, help='Branch off a feature branch from the specified branch')
@click.option('-a', '--add-changes', type=str, default=".", help='Add eventually added changes, defaults to "." meaning all changes')
@click.option('-p', '--push', default=False, is_flag=True, help='Push an eventually updated branch')
@click.option('--and-exec', type=str, default=None, help='Command to be executed after the server has started (stops server afterwards)')
def cli(repo_database, force_clone, force_init, branch, feature_branch, add_changes, push, and_exec):
    # checkout repo
    dolt_checkout_remote_branch(repo_database, force_clone, force_init, branch)

    # eventually checkout feature branch
    if feature_branch is not None:
        print(f"create new feature branch {feature_branch} from {branch}")
        dolt_checkout(feature_branch, True)
        print(dolt_status())

    # start dolt sql server with defaults
    dolt_sql_server_command = ["dolt", "sql-server"]
    if os.environ.get("DOLT_SQL_SERVER_CONFIG", None) is not None:
        dolt_sql_server_command += ["--config", os.environ.get("DOLT_SQL_SERVER_CONFIG")]

    sql_server = None
    rc = -1

    try:
        print("start server:", dolt_sql_server_command)
        if and_exec is not None:
            sql_server = subprocess.Popen(dolt_sql_server_command)
            sleep(1)

            print("server started")
            sub_command_splitter = shlex.shlex(and_exec, posix=True)
            sub_command_splitter.whitespace_split = True
            res = subprocess.run(list(sub_command_splitter))
            rc = res.returncode
            sql_server.kill()
        else:
            res = subprocess.run(dolt_sql_server_command)
            rc = res.returncode
    finally:
        if sql_server is not None:
            print("STOPPING Server ... ")
            sql_server.send_signal(signal.SIGINT)
            sleep(0.5)
            try:
                os.unlink(".dolt/sql-server.lock")
            except Exception:
                pass

    if push:
        print(f"add and push changes made to the branch {dolt_current_branch()}")

        if rc != 0:
            print(f"ERROR The last command exited with rc: {rc}")
            exit(rc)

        dolt_push(add_changes.split(" "), and_exec if and_exec is not None else "add changes from server run")


if __name__ == '__main__':
    cli()
