import logging
import os
import subprocess
import urllib.parse
from threading import Lock
from typing import Tuple

import pandas as pd
from request_boost import boosted_requests

threadlock = Lock()
symbol_table_name = 'yfinance_symbol'
tz_info_table_name = 'tzinfo_exchange'
log = logging.getLogger(__name__)


def fetch_symbols(database, where=None, max_retries=4, nr_jobs=5, page_size=200, max_batches=999999, with_timezone=False):
    join_tz = (', tz.timezone', f'left outer join {tz_info_table_name} tz on tz.symbol = s.exchange') if with_timezone else ('', '')
    if where is None: where = "1=1"

    query = f"""
        select s.symbol{join_tz[0]}
          from {symbol_table_name} s
          {join_tz[1]}
          where {where} 
          order by s.symbol 
          limit {{offset}}, {{limit}}
    """

    existing_symbols = []
    results = execute_query(database, query, max_batches, nr_jobs, page_size, max_retries)

    for _, r in results.iterrows():
        existing_symbols.append((r['symbol'], r['timezone']) if with_timezone else r['symbol'])

    return existing_symbols


def fetch_rows(database, query, first_or_none=False):
    if database is None:
        return None

    query += '\nlimit {offset}, {limit}'

    df = execute_query(database, query)
    return (df.iloc[0] if len(df) > 0 else None) if first_or_none else df


def execute_query(database, query, max_batches=999999, nr_jobs=5, page_size=200, max_retries=4, **kwargs):
    if database.startswith('mysql+pymysql://'):
        log.info(f"use local server {database} instead of http requests")
        return pd.read_sql(query, database)
    else:
        url = 'https://dolthub.com/api/v1alpha1/' + database + '/main?q='
        log.info(f"use the dolthub hosted database over the rest api: {url}")
        offset = page_size * nr_jobs
        pages = [(x * page_size, x * page_size + page_size) for x in range(nr_jobs)]
        results = pd.DataFrame({})

        for i in range(max_batches):
            urls = [url + urllib.parse.quote(query.format(**kwargs, offset=p[0] + i * offset, limit=p[1] + i * offset)) for p in pages]
            log.info(f"submit batch {i} of batch size {page_size} {urls}")
            results = boosted_requests(urls=urls, no_workers=nr_jobs, max_tries=max_retries, timeout=60, parse_json=True, verbose=False)

            assert all([r['query_execution_status'] != 'Error' for r in results]), "\n".join([r['query_execution_message'] for r in results])
            results = [row for r in results if 'rows' in r and len(r['rows']) > 0 for row in r['rows']]

            # check if we have a full last batch
            if len(results[-1]) < page_size:
                break

        return pd.DataFrame(results)


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