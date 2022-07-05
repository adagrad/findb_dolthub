import inspect
import math
import os.path
import random
import sys
import urllib.parse
from datetime import datetime
from time import sleep
from urllib.parse import quote

import click
import pandas as pd
import requests
import requests_random_user_agent
from request_boost import boosted_requests

from modules.requests_session import RequestsSession

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

first_search_characters = 'abcdefghijklmnopqrstuvwxyz^'
# a representation of 'abcdefghijklmnopqrstuvwxyz0123456789.=' but in statistical order
general_search_characters = '012.ap5csnb63v47t8xem9flidgurqhokzwyj=+'
options_search_characters = '0123456789PCa5snbvxemflidgurqhokzwyj=+.'
table_name = 'yfinance_symbol'

query_string = {'device': 'console', 'returnMeta': 'true'}
illegal_tokens = ['null']


class YFSession(RequestsSession):

    def __init__(self, tor_socks_port=None, tor_control_port=None, tor_control_password="password") -> None:
        #super().__init__("http://finance.yahoo.com/", tor_socks_port, tor_control_port, tor_control_password)
        super().__init__(None, tor_socks_port, tor_control_port, tor_control_password)


@click.command()
@click.option('-t', '--time', type=int, default=None, help='Maximum runtime in minutes')
@click.option('-r', '--retries', type=int, default=4, help='Maximum number of retries')
@click.option('-o', '--output', type=str, default="yfsymbols.csv", help='Filename holding the results, appends if exists')
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name')
@click.option('-s', '--known-symbols', type=str, default=None, help='Provide known symbols file instead of fetching them (one sybol per line)')
@click.option('--fetch-known-symbols-only', default=False, is_flag=True, help='Only saves the known symbols')
@click.option('--dolt-load', default=False, is_flag=True, help='Load file into local dolt database branch')
@click.option('--no-ease', default=False, is_flag=True, help='Don\'t sleep between http search calls' )
@click.option('--tor-socks-port', default=None, type=int, help='Tor scks port to access yfinance via TOR')
@click.option('--tor-control-port', default=None, type=int, help='Tor control port to reset exit IP')
@click.option('--tor-control-password', default="password", type=str, help='Tor control passeord to reset exit IP')
def cli(time, retries, output, repo_database, known_symbols, fetch_known_symbols_only, dolt_load, no_ease, tor_socks_port, tor_control_port, tor_control_password):
    started = datetime.now()
    print(f"started at: {started}, write results to {os.path.abspath(output)}", requests_random_user_agent.__version__)

    if known_symbols is not None:
        known_symbols = open(known_symbols).readlines()

    has_repo = len(repo_database) > 0 and "None" != repo_database
    max_symbol_length = _get_max_symbol_length(repo_database) if has_repo > 0 else 21

    possible_symbols = [c for c in first_search_characters]
    existing_symbols = known_symbols if known_symbols is not None else _get_existing_symbols(repo_database, retries) if has_repo > 0 else []

    # make random starting order
    random.shuffle(possible_symbols)

    print(f"fetched last state {len(existing_symbols)} symbols in {(datetime.now() - started).seconds / 60} minutes")
    _save_exisiting_symbls(existing_symbols, output)

    if fetch_known_symbols_only:
        exit(0)

    yf_session = YFSession(tor_socks_port, tor_control_port, tor_control_password)
    possible_symbols = set(possible_symbols)
    while len(possible_symbols) > 0:
        query = possible_symbols.pop()
        i, count = -1, -1

        for i in range(retries):
            if query in existing_symbols: break

            try:
                if not no_ease: sleep(random.random() + 0.2)  # just ease a bit on the server
                df, count = _next_request(yf_session.rsession, query)
                break
            except (requests.HTTPError, requests.exceptions.ChunkedEncodingError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                sleep_amt = int(math.pow(5, i + 1))
                print(f"Retry attempt: {i+1} of {retries}. Sleep period: {sleep_amt} seconds.", e)
                sleep(sleep_amt)

                # reset session for a new random user agent
                yf_session.reset_session()

        if i >= retries:
            raise ValueError(f"Stop loop after {retries} failed retries")

        if (count > 10 and len(query) < max_symbol_length + 1) or query in existing_symbols:
            letters = options_search_characters if query[-1].isnumeric() else general_search_characters
            for c in letters:
                possible_symbols.add(query + c)

        if count > 0:
            # remove symbols we already have in the database and update the known symbols accordingly
            df = df[~df["symbol"].isin(existing_symbols)]
            existing_symbols.extend(df["symbol"].to_list())

            # save remainder to output file
            if os.path.exists(output):
                with open(output, 'a') as f:
                    df.to_csv(f, header=False, index=False)
            else:
                df.to_csv(output, header=True, index=False)

            # save existing symbols for retry purposes
            _save_exisiting_symbls(existing_symbols, output)

        # check if we still have some time left to run another search
        if time is not None and (datetime.now() - started).seconds / 60 > time:
            print(f"maximum allowed {time} minutes reached")
            break

    # finalize the program
    if dolt_load:
        exit(os.system(f"bash -c 'dolt table import -u {table_name} {os.path.abspath(output)}'"))
    else:
        print(f"dolt table import -u {table_name} {os.path.abspath(output)}")
        exit(0)


def _get_existing_symbols(database, max_retries=4, nr_jobs=4, page_size=200, max_batches=999999):
    url = 'https://dolthub.com/api/v1alpha1/' + database +'/main?q='
    query = 'select symbol from ' + table_name + ' order by symbol limit {offset}, {limit}'
    offset = page_size * nr_jobs

    pages = [(x * page_size, x * page_size + page_size) for x in range(nr_jobs)]
    existing_symbols = []

    for i in range(max_batches):
        urls = [url + urllib.parse.quote(query.format(offset=p[0] + i * offset, limit=p[1] + i * offset)) for p in pages]
        print("submit batch", i, "of batch size", urls)
        results = boosted_requests(urls=urls, no_workers=4, max_tries=max_retries, timeout=5, parse_json=True, verbose=False)
        results = [r['rows'] for r in results]

        for r in results:
            for s in r:
                existing_symbols.append(s['symbol'])

        # check if we have a full last batch
        if len(results[-1]) < page_size:
            break

    return existing_symbols


def _get_max_symbol_length(database):
    resp = requests.get(
        f'https://dolthub.com/api/v1alpha1/{database}/main?q=select max(length(symbol)) as length from {table_name}'
    )

    max_symbol_length = resp.json()["rows"][0]['length']
    return max_symbol_length


def _next_request(rsession, query_str, max_retries=4):
    def decode_symbols_container(json):
        df = pd.DataFrame(json['data']['items'])\
            .rename(columns={"exch": "exchange", "exchDisp": "exchange_description", "typeDisp": "type_description"})

        count = len(df)
        return df, count

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
            }
        )
    )


def _fetch(rsession, query_str, headers={}):
    # curl 'https://finance.yahoo.com/_finance_doubledown/api/resource/searchassist;searchTerm=zt9c?device=console&returnMeta=true' -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:101.0) Gecko/20100101 Firefox/101.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' -H 'Accept-Encoding: gzip, deflate, br' -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Cookie: A1=d=AQABBCbOwmICEJWysq1Qp7ywjF4irDRfshwFEgEBAQEfxGLMYgAAAAAA_eMAAA&S=AQAAAr9Nn1qU_11hiHcgRn6AAQg; A3=d=AQABBCbOwmICEJWysq1Qp7ywjF4irDRfshwFEgEBAQEfxGLMYgAAAAAA_eMAAA&S=AQAAAr9Nn1qU_11hiHcgRn6AAQg; A1S=d=AQABBCbOwmICEJWysq1Qp7ywjF4irDRfshwFEgEBAQEfxGLMYgAAAAAA_eMAAA&S=AQAAAr9Nn1qU_11hiHcgRn6AAQg&j=WORLD' -H 'Upgrade-Insecure-Requests: 1' -H 'Sec-Fetch-Dest: document' -H 'Sec-Fetch-Mode: navigate' -H 'Sec-Fetch-Site: cross-site' -H 'If-None-Match: W/"98-a6M7dMFne4FxCrSkEA+UT5YgLSU"' -H 'TE: trailers'
    def _encodeParams(params):
        encoded = ''
        for key, value in params.items():
            encoded += ';' + quote(key) + '=' + quote(str(value))
        return encoded

    params = {
        'searchTerm': query_str,
    }

    protocol = 'https'
    req = requests.Request(
        'GET',
        protocol +'://finance.yahoo.com/_finance_doubledown/api/resource/searchassist' + _encodeParams(params),
        headers=headers,
        params=query_string
    )

    req = req.prepare()
    resp = None
    print("req " + req.url)

    try:
        resp = rsession.send(req, timeout=(12, 12))
        resp.raise_for_status()

        return resp.json()
    except Exception as e:
        print("ERROR", req, headers, resp.text if resp is not None else "")
        raise e


def _save_exisiting_symbls(existing_symbols, output):
    with open(output + ".existing.symbols", 'w') as f:
        for es in existing_symbols:
            f.write(es + ' \n')


if __name__ == '__main__':
    cli()
