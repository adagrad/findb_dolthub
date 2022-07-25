import inspect
import math
import os.path
import random
import re
import sys
from datetime import datetime, timedelta
from time import sleep
from urllib.parse import quote

import click
import pandas as pd
import requests

from modules.dolt_api import fetch_symbols, dolt_load_file
from modules.requests_session import RequestsSession

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

first_search_characters = 'abcdefghijklmnopqrstuvwxyz^'.upper()
# a representation of 'abcdefghijklmnopqrstuvwxyz0123456789.=' but in statistical order
general_search_characters = '012.ap5csnb63v47t8xem9flidgurqhokzwyj=+'.upper()
options_search_characters = '0123456789'.upper()
exchanges = ('.BA', '.AX', '.VI', '.BR', '.SA', '.CN', '.NE', '.TO', '.V', '.SN', '.SS', '.SZ', '.PR', '.CO', '.CA', '.TL', '.HE', '.NX', '.PA', '.BE', '.BM', '.DU', '.F', '.HM', '.HA', '.MU', '.SG', '.DE', '=X', '.AT', '.HK', '.BD', '.IC', '.BO', '.NS', '.JK', '.IR', '.TA', '.TI', '.MI', '.T', '.RG', '.VS', '.KL', '.MX', '.AS', '.NZ', '.OL', '.LS', '.QA', '.ME', '.SI', '.Jo', '.KS', '.KQ', '.MC', '.SAU', '.ST', '.SW', '.TWO', '.TW', '.BK', '.IS', '.L', '.IL', '.CBT', '.CME', '.NYB', '.CMX', '.NYM', '.CR')
table_name = 'yfinance_symbol'

query_string = {'device': 'console', 'returnMeta': 'true'}
illegal_tokens = ['null']


class YFSession(RequestsSession):

    def __init__(self, tor_socks_port=None, tor_control_port=None, tor_control_password="password") -> None:
        #super().__init__("http://finance.yahoo.com/", tor_socks_port, tor_control_port, tor_control_password)
        super().__init__(None, tor_socks_port, tor_control_port, tor_control_password)


@click.command()
@click.option('-t', '--time', type=int, default=None, help='Maximum runtime in minutes')
@click.option('-r', '--resume', type=str, default=None, help='Symbols file to resume from a left session')
@click.option('-o', '--output', type=str, default="yfsymbols.csv", help='Filename holding the results, appends if exists (default=yfsymbols.csv)')
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-s', '--known-symbols', type=str, default=None, help='Provide known symbols file instead of fetching them (one sybol per line)')
@click.option('--fetch-known-symbols-only', default=False, is_flag=True, help='Only saves the known symbols')
@click.option('--dolt-load', default=False, is_flag=True, help='Load file into local dolt database branch')
@click.option('--no-ease', default=False, is_flag=True, help='Don\'t sleep between http search calls' )
@click.option('--tor-socks-port', default=None, type=int, help='Tor scks port to access yfinance via TOR')
@click.option('--tor-control-port', default=None, type=int, help='Tor control port to reset exit IP')
@click.option('--tor-control-password', default="password", type=str, help='Tor control password to reset exit IP')
@click.option('--retries', type=int, default=4, help='Maximum number of retries (default=4)')
def cli(time, resume, output, repo_database, known_symbols, fetch_known_symbols_only, dolt_load, no_ease, tor_socks_port, tor_control_port, tor_control_password, retries):
    started = datetime.now()
    max_runtime = datetime.now() + timedelta(minutes=time) if time is not None else None
    print(f"started at: {started}, write results to {os.path.abspath(output)} run until: {max_runtime}")

    # get maximum lengh of symbols
    max_symbol_length = _get_max_symbol_length(repo_database)

    # get starting sets of symbols
    existing_symbols, possible_symbols = _get_symbol_sets(known_symbols, repo_database, resume, retries)
    print(f"fetched last state {len(existing_symbols)} symbols in {(datetime.now() - started).seconds / 60} minutes")
    _save_symbols(existing_symbols, output + ".existing.symbols")

    # eventually we are already done
    if fetch_known_symbols_only:
        exit(0)

    yf_session = YFSession(tor_socks_port, tor_control_port, tor_control_password)
    _look_for_new_symbols(possible_symbols, existing_symbols, max_symbol_length, retries, not no_ease, max_runtime, yf_session, output)

    # finalize the program
    csv_file = os.path.abspath(output)
    print(f"dolt table import -u {table_name} {csv_file}")
    if dolt_load:
        rc, out, err = dolt_load_file(table_name, csv_file)
        exit(rc)
    else:
        exit(0)


def _look_for_new_symbols(possible_symbols, existing_symbols, max_symbol_length, retries, ease, max_runtime, yf_session, output):
    counter = 0
    while len(possible_symbols) > 0:
        try:
            query = possible_symbols.pop()
            df, count = _download_new_symbols(query, existing_symbols, retries, ease, yf_session)

            if (query in existing_symbols or count > 10) and (len(query) < max_symbol_length + 1 and not query.endswith(exchanges)):
                if query.endswith("."):
                    # there can only come an exchange code from here
                    for x in exchanges:
                        possible_symbols.add(query[:-1] + x)
                elif re.search(r"\d{4}[PC]$", query):
                    # it is an options code there can only be numbers from here
                    for n in options_search_characters:
                        possible_symbols.add(query + n)
                else:
                    # else continue brute force searching
                    for c in general_search_characters:
                        possible_symbols.add(query + c)

            if count > 0:
                # remove symbols we already have in the database and update the known symbols accordingly
                df = df[~df["symbol"].isin(existing_symbols)]
                existing_symbols.update(df["symbol"].to_list())

                # save remainder to output file
                if os.path.exists(output):
                    with open(output, 'a') as f:
                        df.to_csv(f, header=False, index=False)
                else:
                    df.to_csv(output, header=True, index=False)

                # save existing symbols for retry purposes
                _save_symbols(existing_symbols, output + ".existing.symbols")

            # loop counter
            counter += 1
        finally:
            # check if we still have some time left to run another search
            if max_runtime is not None and datetime.now() > max_runtime:
                print(f"maximum allowed minutes reached")
                _save_symbols(possible_symbols, output + ".possible.symbols")
                break

    return counter


def _get_symbol_sets(known_symbols_file, repo_database, resume_file, max_dolt_fetch_retries, **kwargs):
    # get known symbols
    known_symbols = None if known_symbols_file is None else _load_symbols(known_symbols_file)
    has_repo = len(repo_database) > 0 and "None" != repo_database

    possible_symbols = [c for c in first_search_characters] if resume_file is None else _load_symbols(resume_file)  # TODO eventually read from database table?
    existing_symbols = known_symbols if known_symbols is not None else _fetch_existing_symbols(repo_database, max_dolt_fetch_retries, **kwargs) if has_repo > 0 else []
    existing_symbols = set([es.strip().upper() for es in existing_symbols])
    known_symbols = None

    # make random starting order
    random.shuffle(possible_symbols)
    possible_symbols = set(possible_symbols)

    return existing_symbols, possible_symbols


def _fetch_existing_symbols(database, max_retries=4, nr_jobs=4, page_size=200, max_batches=999999):
    return fetch_symbols(database, max_retries=max_retries, nr_jobs=nr_jobs, page_size=page_size, max_batches=max_batches)


def _get_max_symbol_length(repo_database, default_value=21):
    if len(repo_database) > 0 and "None" != repo_database:
        url = f'https://dolthub.com/api/v1alpha1/{repo_database}/main?q=select max(length(symbol)) as length from {table_name}'
        resp = requests.get(url)

        try:
            resp.raise_for_status()
        except Exception as e:
            raise ValueError(f"{url}, {resp.text}", e)

        max_symbol_length = int(resp.json()["rows"][0]['length'])
        return max_symbol_length

    return default_value


def _download_new_symbols(query, existing_symbols, retries, ease, yf_session, _fetch_json_as_dataframe=lambda r, q: _next_request(r, q)):
    i, count, df = -1, -1, pd.DataFrame({})

    if query not in existing_symbols:
        for i in range(retries):
            try:
                if ease: sleep(random.random() + 0.2)  # just ease a bit on the server
                df = _fetch_json_as_dataframe(yf_session.rsession, query)
                count = len(df)
                break
            except (requests.HTTPError, requests.exceptions.ChunkedEncodingError, requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as e:
                sleep_amt = int(math.pow(5, i + 1))
                print(f"Retry attempt: {i + 1} of {retries}. Sleep period: {sleep_amt} seconds.", e)
                sleep(sleep_amt)

                # reset session for a new random user agent
                yf_session.reset_session()

        if i >= retries:
            raise ValueError(f"Stop loop after {retries} failed retries")

    return df, count


def _next_request(rsession, query_str):
    def decode_symbols_container(json):
        df = pd.DataFrame(json['data']['items'])\
            .rename(columns={"exch": "exchange", "exchDisp": "exchange_description", "typeDisp": "type_description"})

        # add active as default value for the symbols we just found
        df['active'] = 1

        return df

    return decode_symbols_container(
        _fetch(
            rsession,
            query_str,
            headers={
                "Host": "finance.yahoo.com",
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:101.0) Gecko/20100101 Firefox/101.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "TE": "trailers",
                # "Set-Cookie": "EuConsent=CPcqqIAPcqqIAAOACBESCYCoAP_AAH_AACiQIlNd_X__bX9n-_7_6ft0cY1f9_r3ruQzDhfFs-8F3L_W_LwX32E7NF36pq4KmR4ku1bBIQFtHMnUDUmxaolVrzHsak2cpyNKI7JkknsZe2dYGF9Pn9lD-YKZ7_5_9_f52T_9_9_-39z3_9f___dt_-__-vjfV599n_v9fV_789Kf9____-_-___4IQQ_AJMNW4gC7EscCbQMIoQQIwrCQqAUAEFAMLRBYAODgp2VgEusIWACAVARgRAgxBRgwCAAACAJCIAJACwQCIAiAQAAgARAIQAETAILACwMAgABANCxACgAECQgyICI5TAgIgSCglsrEEoK9DTCAOssAKBRGxUACJAABSAgJCwcAwBICXCyQJMULwAw0AGAAIIlCIAMAAQRKFQAYAAgiUA;Version=1;Comment=;Domain=yahoo.com;Path=/;Max-Age=86400"
            }
        )
    )


def _fetch(rsession, query_str, headers={}):
    resp = None

    try:
        url = f"https://finance.yahoo.com/_finance_doubledown/api/resource/searchassist;searchTerm={query_str}?device=console&returnMeta=true&_guc_consent_skip=1658766797"
        print("req", url)
        resp = rsession.get(url)
        resp.raise_for_status()

        return resp.json()
    except Exception as e:
        print("ERROR", url, headers, resp.text if resp is not None else "")
        raise e


def _save_symbols(symbols, output_file):
    with open(output_file, 'w') as f:
        for es in symbols:
            f.write(es.strip() + ' \n')


def _load_symbols(file):
    return [s.strip().upper() for s in open(file).readlines() if len(s.strip()) > 0]


if __name__ == '__main__':
    cli()
