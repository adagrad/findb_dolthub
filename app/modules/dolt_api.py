import logging
import os
import random
import subprocess
from threading import Lock
from typing import Tuple

import pandas as pd

threadlock = Lock()
symbol_table_name = 'yfinance_symbol'
tz_info_table_name = 'yfinance_exchange_info'
log = logging.getLogger(__name__)


def fetch_rows(database, query, first_or_none=False):
    if database is None:
        return None

    df = execute_query(database, query)
    return (df.iloc[0] if len(df) > 0 else None) if first_or_none else df


def execute_query(database, query, max_batches=999999, nr_jobs=5, page_size=200, max_retries=4, **kwargs):
    log.info(f"execute query: {query}")
    return pd.read_sql(query, database).reset_index()


#
# DOLT Bash API
#

def dolt_load_file(table_name, csv_file) -> Tuple[int, str, str]:
    if os.path.exists(csv_file):
        # rc = os.system(f"bash -c 'dolt table import -u {table_name} {csv_file}'")
        rc, out, err = execute_shell("dolt", "table", "import", "-u", table_name, csv_file)
        return rc, out, err
    else:
        return 1, "", "File Not Found"


def dolt_init(alternative_main: str = None):
    rc, std, err = execute_shell("dolt", "init")
    if rc != 0: raise IOError(std + '\n' + err)

    if alternative_main is not None:
        rc, std, err = execute_shell("dolt", "checkout", "-b", alternative_main)
        if rc != 0: raise IOError(std + '\n' + err)

        rc, std, err = execute_shell("dolt", "branch", "-f", "-d", "main")
        if rc != 0: raise IOError(std + '\n' + err)


def dolt_merge(repo_database, force_clone, force_init, source_branch, target_branch, commit_message, push, delete_source, theirs, ours):
    assert not (theirs is True and ours is True), "Nice try, but you can specify theirs and ours at the same time!"

    # make sure the target source is there, checked out and up to date
    dolt_checkout_remote_branch(repo_database, force_clone, force_init, source_branch)

    # checkout an up-to-date target branch
    dolt_checkout(target_branch)

    # merge source into target
    log.info(f"merge {source_branch} into {target_branch}")
    rc, std, err = execute_shell("dolt", "merge", source_branch)
    if "CONFLICT" in std:
        if theirs or ours:
            log.info("resolve conflicts using " + "--theirs" if theirs else "--ours")
            rc, std, err = execute_shell("dolt", "conflicts", "resolve", "--theirs" if theirs else "--ours", ".")
            if rc != 0:raise IOError(std + '\n' + err)
        else:
            raise IOError(std + '\n' + err)

    # add changes
    rc, std, err = execute_shell("dolt", "add", ".")
    if rc != 0: raise IOError(std + '\n' + err)

    # commit changes
    rc, std, err = execute_shell("dolt", "commit", "-m", commit_message)
    if rc != 0 and "no changes added to commit" not in err: raise IOError(std + '\n' + err)

    if push:
        log.info("push changes")
        dolt_push()

    if delete_source:
        log.warning(f"delete branch {source_branch}")
        rc, std, err = execute_shell("dolt", "branch", "-d", source_branch)
        if rc != 0: raise IOError(std + '\n' + err)

        if push:
            log.warning(f"delete remote branch {source_branch}")
            # dolt push --set-upstream origin schema
            rc, std, err = execute_shell("dolt", "push", "origin", f":{source_branch}")
            if rc != 0: raise IOError(std + '\n' + err)


def dolt_push(stage_all=False, commit_message=None):
    if stage_all:
        stage_tables = stage_all if isinstance(stage_all, (tuple, list, set)) else ["."]
        rc, std, err = execute_shell("dolt", "add", *stage_tables)
        if rc != 0 and "Unknown tables" not in err: raise IOError(std + '\n' + err)

        rc, std, err = execute_shell("dolt", "commit", "-m", commit_message if commit_message is not None else 'push local changes')
        if rc != 0 and "no changes added to commit" not in err: raise IOError(std + '\n' + err)

    branch = dolt_current_branch()
    rc, std, err = execute_shell("dolt", "push", "--set-upstream", "origin", branch)
    if rc != 0: raise IOError(std + '\n' + err)


def dolt_status():
    rc, std, err = execute_shell("dolt", "status")
    if rc != 0:
        raise IOError(std + '\n' + err)

    return std + '\n' + err


def dolt_current_branch():
    rc, std, err = execute_shell("dolt", "branch")
    if rc != 0:
        raise IOError(std + '\n' + err)

    current_branch = [l[2:].strip() for l in  std.splitlines() if l.startswith("*")]
    return current_branch[0]


def dolt_checkout(branch, new=False):
    log.info(f"checkout branch {branch}")

    rc, std, err = execute_shell("dolt", "checkout", "-b", branch) if new else execute_shell("dolt", "checkout", branch)
    if rc != 0: raise IOError(std + '\n' + err)

    assert dolt_current_branch() == branch, f"failed to checkout branch {branch} are on {dolt_current_branch()}"

    if not new:
        log.info("make sure we are up to date")
        rc, std, err = execute_shell("dolt", "pull")
        if "no tracking information for the current branch" in err:
            log.info("this is a local only branch")
        elif 'no common ancestor' in err:
            rc, std, err = execute_shell("dolt", "fetch", "origin", branch)
            if rc != 0: raise IOError(std + '\n' + err)

            rc, std, err = execute_shell("dolt", "checkout", f"origin/{branch}", "-b", f"{branch}/_{random.randint(0, 99999999)}")
            if rc != 0: raise IOError(std + '\n' + err)
        else:
            if rc != 0: raise IOError(std + '\n' + err)


def dolt_checkout_remote_branch(repo_database, force_clone, force_init, branch):
    rc, std, err = execute_shell("dolt", "checkout", branch)
    if rc == 2 and "current directory is not a valid dolt repository" in err:
        log.warning(
            "current directory is not a dolt repository " +
            (f"-> clone {repo_database}" if force_clone else f"-> init {repo_database}" if force_init else "") +
            f" to {os.getcwd()}"
        )

        if force_init and force_clone:
            raise ValueError("Only one of force clone or force init can be provided")
        elif force_init:
            log.info("dolt init + add origin")
            dolt_init("main/alternative")

            rc, std, err = execute_shell("dolt", "remote", "add", "origin", f"https://doltremoteapi.dolthub.com/{repo_database}")
            if rc != 0: raise IOError(std + '\n' + err)

            rc, std, err = execute_shell("dolt", "fetch", "origin", branch)
            if rc != 0: raise IOError(std + '\n' + err)
        elif force_clone:
            log.info("dolt clone")
            rc, std, err = execute_shell("dolt", "clone", repo_database, ".")
            if rc != 0: raise IOError(std + '\n' + err)
        else:
            raise IOError(std + '\n' + err)

    # directory is a dolt repo
    dolt_checkout(branch)

    assert dolt_current_branch() == branch or dolt_current_branch().startswith(branch + "/_"), \
        f"failed to checkout branch {branch} are on {dolt_current_branch()}"


def dolt_execute_query(query):
    rc, std, err = execute_shell("dolt", "sql", "-q", query)
    if rc != 0:
        raise IOError(std + '\n' + err)


def execute_shell(command, *args, **kwargs):
    res = None, None, None
    threadlock.acquire()
    try:
        result = subprocess.run([command, *args], capture_output=True, text=True, **kwargs)
        res = result.returncode, result.stdout, result.stderr
        log.info(f"{command} {args} \n\t {res}")
        return res
    except Exception as e:
        log.error(f"{command} {args} \n\t {res}", exc_info=1)
        raise e
    finally:
        threadlock.release()