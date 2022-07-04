import inspect
import math
import os.path
import random
import sys
from datetime import datetime
from time import sleep
from urllib.parse import quote

import click
import pandas as pd
import requests
import requests_random_user_agent

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

first_search_characters = 'abcdefghijklmnopqrstuvwxyz^'
# a representation of 'abcdefghijklmnopqrstuvwxyz0123456789.=' but in statistical order
general_search_characters = '012.ap5csnb63v47t8xem9flidgurqhokzwyj=+'
options_search_characters = '0123456789PCa5snbvxemflidgurqhokzwyj=+.'


headers = {
    #'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:101.0) Gecko/20100101 Firefox/101.0',  # 'yahoo-ticker-symbol-downloader'
    #'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    #'Accept-Language': 'en-US,en;q=0.5',
    #'Accept-Encoding': 'gzip, deflate, br',
    #'DNT': '1',
    #'Connection': 'keep-alive',
    #'Cookie: XXXX'
    #'Upgrade-Insecure-Requests': '1',
    #'Sec-Fetch-Dest': 'document',
    #'Sec-Fetch-Mode': 'navigate',
    #'Sec-Fetch-Site': 'cross-site',
    #'If-None-Match': 'W/"98-a6M7dMFne4FxCrSkEA+UT5YgLSU"',
    #'TE': 'trailers'
}
query_string = {'device': 'console', 'returnMeta': 'true'}
illegal_tokens = ['null']
max_brute_force_len = 3
rsession = requests.Session()


@click.command()
@click.option('-t', '--time', type=int, default=None, help='Maximum runtime in minutes')
@click.option('-r', '--retries', type=int, default=4, help='Maximum number of retries')
@click.option('-o', '--output', type=str, default="yfsymbols.csv", help='Filename holding the results, appends if exists')
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name')
@click.option('-s', '--known-symbols', type=str, default=None, help='Provide known symbols file instead of fetching them (one sybol per line)')
@click.option('--fetch-known-symbols-only', default=False, help='Only saves the known symbols')
def cli(time, retries, output, repo_database, known_symbols, fetch_known_symbols_only):
    started = datetime.now()
    print(f"started at: {started}, write results to {os.path.abspath(output)}", requests_random_user_agent.default_user_agent)

    if known_symbols is not None:
        known_symbols = set(open(known_symbols).readlines())

    has_repo = len(repo_database) > 0 and "None" != repo_database
    max_symbol_length = _get_max_symbol_length(repo_database) if has_repo > 0 else 21

    possible_symbols = _brute_force_symbols(max_brute_force_len)
    existing_symbols = known_symbols if known_symbols is not None else _get_existing_symbols(repo_database, retries) if has_repo > 0 else {}
    possible_symbols -= existing_symbols

    # make random starting order
    possible_symbols = _shuffle_set(possible_symbols)

    print(f"fetched last state {len(existing_symbols)} symbols in {(datetime.now() - started).seconds / 60} minutes")
    with open(output + ".existing.symbols", 'w') as f:
        for es in existing_symbols:
            f.write(es + ' \n')

    if fetch_known_symbols_only:
        exit(0)

    while len(possible_symbols) > 0:
        # now we randomize the query, this way we can run this program parallel
        query = possible_symbols.pop()
        i, count = -1, -1

        for i in range(retries):
            try:
                df, count = _next_request(query)
                break
            except (requests.HTTPError, requests.exceptions.ChunkedEncodingError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                sleep_amt = int(math.pow(5, i + 1))
                print(f"Retry attempt: {i+1} of {retries}. Sleep period: {sleep_amt} seconds.", e)
                sleep(sleep_amt)

                # reset session for a new random user agent
                global rsession
                rsession = requests.Session()

        if i >= retries:
            raise ValueError(f"Stop loop after {retries} failed retries")

        if count > 10 and len(query) >= max_brute_force_len and len(query) < max_symbol_length + 1:
            letters = options_search_characters if query[-1].isnumeric() else general_search_characters
            for c in letters:
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
            with open(output + ".existing.symbols", 'w') as f:
                for es in existing_symbols:
                    f.write(es + ' \n')

        # check if we still have some time left to run another search
        if time is not None and (datetime.now() - started).seconds / 60 > time:
            print(f"maximum allowed {time} minutes reached")
            return


def _shuffle_set(s):
    l = list(s)
    random.shuffle(l)
    return set(l)


def _brute_force_symbols(max_len=4):
    symbols = []

    def add_character(s):
        for c in general_search_characters:
            _next = s + c
            yield _next
            if (len(_next)) < max_len:
                for _s in add_character(_next):
                    yield _s

    for fc in first_search_characters:
        symbols.append(fc)
        for s in add_character(fc):
            symbols.append(s)

    return set(symbols)


def _get_existing_symbols(database, max_retries=4, max_pages=999999):
    def get_page(offset):
        print("fetch existing symbols page offset", offset)
        return requests.get(
            f'https://dolthub.com/api/v1alpha1/{database}/main?q=select symbol from yahoo_symbol order by symbol limit {offset}, {offset + 200}'
        ).json()['rows']

    existing_symbols = []
    page_count = 0
    offset = 0
    retry = 0

    while retry < max_retries:
        try:
            page = get_page(offset)
            existing_symbols += [p['symbol'] for p in page]
            page_count += 1
            offset += 200
            retry = 0

            if len(page) < 200 or page_count >= max_pages:
                break

        except Exception:
            retry += 1
            print("retry after error", retry + 1)
            sleep(1 * retry)

    return set(existing_symbols)


def _get_max_symbol_length(database):
    resp = requests.get(
        f'https://dolthub.com/api/v1alpha1/{database}/main?q=select max(length(symbol)) as length from yahoo_symbol'
    )

    max_symbol_length = resp.json()["rows"][0]['length']
    return max_symbol_length


def _next_request(query_str, max_retries=4):
    def decode_symbols_container(json):
        df = pd.DataFrame(json['data']['items'])\
            .rename(columns={"exch": "exchange", "exchDisp": "exchange_description", "typeDisp": "type_description"})

        count = len(df)
        return df, count

    return decode_symbols_container(_fetch(query_str))


def _fetch(query_str):
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


if __name__ == '__main__':
    cli()
