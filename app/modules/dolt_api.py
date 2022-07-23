import contextlib
import io
import os
import subprocess
import urllib.parse
from threading import Lock
from typing import Tuple
import logging
import requests
from request_boost import boosted_requests

threadlock = Lock()
symbol_table_name = 'yfinance_symbol'
tz_info_table_name = 'tzinfo_exchange'
log = logging.getLogger(__name__)


def fetch_symbols(database, where=None, max_retries=4, nr_jobs=5, page_size=200, max_batches=999999, with_timezone=False):
    url = 'https://dolthub.com/api/v1alpha1/' + database +'/main?q='
    join_tz = (', tz.timezone', f'left outer join {tz_info_table_name} tz on tz.symbol = s.exchange') if with_timezone else ('', '')
    query = f"""
        select s.symbol{join_tz[0]}
          from {symbol_table_name} s
          {join_tz[1]}
          where {{where}} 
          order by s.symbol 
          limit {{offset}}, {{limit}}
    """
    if where is None: where = "1=1"
    offset = page_size * nr_jobs

    pages = [(x * page_size, x * page_size + page_size) for x in range(nr_jobs)]
    existing_symbols = []

    for i in range(max_batches):
        urls = [url + urllib.parse.quote(query.format(where=where, offset=p[0] + i * offset, limit=p[1] + i * offset)) for p in pages]
        log.info(f"submit batch {i} of batch size {page_size} {urls}")
        results = boosted_requests(urls=urls, no_workers=nr_jobs, max_tries=max_retries, timeout=60, parse_json=True, verbose=False)
        results = [r['rows'] for r in results]

        for r in results:
            for s in r:
                existing_symbols.append((s['symbol'], s['timezone']) if with_timezone else s['symbol'])

        # check if we have a full last batch
        if len(results[-1]) < page_size:
            break

    return existing_symbols


def fetch_rows(database, query, offset=0, limit=200, first_or_none=False):
    if database is None:
        return None

    query += f'limit {offset}, {limit}'
    url = f'https://dolthub.com/api/v1alpha1/{database}/main?q={urllib.parse.quote(query)}'

    response = requests.get(url).json()
    assert response['query_execution_status'] != 'Error', response['query_execution_message']
    rows = response['rows']
    return (rows[0] if len(rows) > 0 else None) if first_or_none else rows


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