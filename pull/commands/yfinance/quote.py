import datetime
import inspect
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import partial

import click
import pytz
import yfinance as yf

from modules.dolt_api import fetch_symbols, fetch_rows

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


symbol_table_name = 'yfinance_symbol'
quote_table_name = 'yfinance_quote'


@click.command()
@click.option('-t', '--time', type=int, default=None, help='Maximum runtime in minutes')
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('-w', '--where', type=str, default=None, help='A "where" constraint provided for the selection of symbols from the database')
@click.option('-s', '--symbols', type=str, default=None, help='A file of symbols (one per line) to fetch prices')
@click.option('-o', '--output-dir', type=str, default='.', help='Path to store downloaded csv files')
@click.option('-p', '--parallel-threads', type=int, default=10, help='Number of parallel threads')
@click.option('--dolt-load', default=False, is_flag=True, help='Load file into local dolt database branch')
def cli(time, repo_database, where, symbols, output_dir, parallel_threads, dolt_load):
    max_runtime = datetime.datetime.now() + timedelta(minutes=time)
    if repo_database == "" or repo_database == "None":
        repo_database = None

    # select tickers from dolt using a where query or allow a list of symbols to be passed as a file
    assert (where is not None and repo_database is not None) or symbols is not None, \
        "Either a query (and database repository) or a symbols file has to be provied"

    symbols = _select_tickers(repo_database, where, symbols)

    # create a thread pool and wait until all jobs completed
    with ThreadPoolExecutor(max_workers=parallel_threads) as executor:
        for symbol in symbols:
            executor.submit(partial(_fetch_data, database=repo_database, symbol=symbol, dolt_load=dolt_load, path=output_dir, max_runtime=max_runtime))


def _select_tickers(database, where, symbols_file):
    if symbols_file is not None:
        return [s.strip().upper() for s in open(symbols_file).readlines() if len(s.strip()) > 0]

    fetched_symbols = fetch_symbols(database, symbol_table_name, where)
    print(f"fetched {len(fetched_symbols)} symbols from database")
    return fetched_symbols


def _fetch_data(database, symbol, path='.', dolt_load=False, max_runtime=None):
    if max_runtime is not None and datetime.datetime.now() >= max_runtime:
        print("max time reachedm exit before fetching", symbol)
        return

    try:
        tz_info = pytz.timezone('US/Eastern')  # TODO derive from exchange
        ticker = yf.Ticker(symbol)

        # check if price data is already available in database and what the latest date is
        query = f"select max(epoch) as epoch from {quote_table_name} where symbol={symbol}"
        res = fetch_rows(database, query, first_or_none=True)
        last_price_date = datetime.datetime.fromtimestamp(res["epoch"], tz=tz_info) if res is not None else None

        # fetch data, and overwrite the last couple of days in case of error corrections
        if last_price_date is None:
            print("no last price date available, fetch max history")
            df = ticker.history(period='max')
        else:
            print(f"fetch for new prices from {last_price_date}")
            df = ticker.history(start=last_price_date - timedelta(days=5))

        # insert timezone from exchange
        df.insert(0, "tzinfo", str(tz_info))

        # convert date to float
        df.insert(0, "epoch", df.index.to_series().apply(lambda x:x.tz_localize(tz_info).to_pydatetime().timestamp()))

        # add symbol to dataframe
        df.insert(0, "symbol", symbol)

        # rename columns
        df = df.rename(
            columns={"Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v", "Dividends": "dividend", "Stock Splits": "split"}
        )

        # save as csv
        csv_file = os.path.abspath(os.path.join(path, f"{str(symbol)}.csv"))
        print(f"save csv for {symbol} containing {len(df)} rows to {csv_file}")
        df.to_csv(csv_file, index=False)

        # load csv into dolt branch
        if dolt_load:
            if os.path.exists(csv_file):
                exit(os.system(f"bash -c 'dolt table import -u {quote_table_name} {csv_file}'"))
        else:
            print(f"dolt table import -u {quote_table_name} {csv_file}")
            exit(0)
    except Exception as e:
        traceback.print_exc(e)
        raise e


if __name__ == '__main__':
    cli()
