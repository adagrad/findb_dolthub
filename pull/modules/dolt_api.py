import urllib.parse

import requests
from request_boost import boosted_requests


def fetch_symbols(database, table_name, where=None, max_retries=4, nr_jobs=4, page_size=200, max_batches=999999):
    url = 'https://dolthub.com/api/v1alpha1/' + database +'/main?q='
    query = 'select symbol from ' + table_name + ' where {where} order by symbol limit {offset}, {limit}'
    if where is None: where = "1=1"
    offset = page_size * nr_jobs

    pages = [(x * page_size, x * page_size + page_size) for x in range(nr_jobs)]
    existing_symbols = []

    for i in range(max_batches):
        urls = [url + urllib.parse.quote(query.format(where=where, offset=p[0] + i * offset, limit=p[1] + i * offset)) for p in pages]
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


def fetch_rows(database, query, offset=0, limit=200, first_or_none=False):
    query += f'limit {offset}, {limit}'
    url = f'https://dolthub.com/api/v1alpha1/{database}/main?q={urllib.parse.quote(query)}'

    response = requests.get(url).json()
    rows = response['rows']
    return (rows[0] if len(rows) > 0 else None) if first_or_none else rows

