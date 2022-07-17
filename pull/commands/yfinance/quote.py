import contextlib
import datetime
import inspect
import io
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import partial
from time import sleep

import click
import pandas as pd
import pytz
import yfinance as yf

from modules.dolt_api import fetch_symbols, fetch_rows, dolt_load_file

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())



quote_meta_table_name = 'yfinance_quote_meta'
quote_tz_table_name = 'tzinfo_exchange'
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
    max_runtime = datetime.datetime.now() + timedelta(minutes=time) if time is not None else None
    if repo_database == "" or repo_database == "None":
        repo_database = None

    # select tickers from dolt using a where query or allow a list of symbols to be passed as a file
    assert (where is not None and repo_database is not None) or symbols is not None, \
        "Either a query (and database repository) or a symbols file has to be provided"

    print(f"select symbols where {where}")
    symbols = _select_tickers(repo_database, where, symbols)

    # create a thread pool and wait until all jobs completed
    with ThreadPoolExecutor(max_workers=parallel_threads) as executor:
        futures = \
            [executor.submit(
                partial(
                    _fetch_data,
                    database=repo_database,
                    symbol=symbol,
                    dolt_load=dolt_load,
                    path=output_dir,
                    max_runtime=max_runtime
                )
            ) for symbol in symbols]

        try:
            while not all([future.done() for future in futures]):
                sleep(0.2)
        except KeyboardInterrupt:
            print("SIGTERM stopping thread pool!")
            executor.shutdown(wait=False, cancel_futures=True)


def _select_tickers(database, where, symbols_file, nr_jobs=5):
    if symbols_file is not None:
        return [s.strip().upper() for s in open(symbols_file).readlines() if len(s.strip()) > 0]

    fetched_symbols = fetch_symbols(database, where, with_timezone=True, nr_jobs=nr_jobs)
    print(f"fetched {len(fetched_symbols)} symbols from database")
    return fetched_symbols


def _fetch_data(database, symbol, path='.', dolt_load=False, max_runtime=None):
    if max_runtime is not None and datetime.datetime.now() >= max_runtime:
        print("max time reached exit before fetching", symbol)
        return

    try:
        if isinstance(symbol, tuple):
            symbol, tz_info = symbol[0], pytz.timezone(symbol[1])
        else:
            tz_info = pytz.timezone('US/Eastern')

        ticker = yf.Ticker(symbol)
        first_price_date, last_price_date = _fetch_last_date(database, symbol, tz_info)
        delisted = False

        # fetch data, and overwrite the last couple of days in case of error corrections
        with io.StringIO() as output:
            with contextlib.redirect_stdout(output):
                if last_price_date is None:
                    print("no last price date available, fetch max history")
                    df = ticker.history(period='max')
                else:
                    print(f"fetch for new prices from {last_price_date}")
                    df = ticker.history(start=(last_price_date - timedelta(days=5)).date())

            if "No data found, symbol may be delisted" in output.getvalue():
                delisted = True

        # insert timezone from exchange
        df.insert(0, "tzinfo", str(tz_info))

        # convert date to float
        df.insert(0, "epoch", df.index.to_series().apply(lambda x: x.tz_localize(tz_info).to_pydatetime().timestamp()))

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

        # add another csv file to load containing symbol, min_epoch, max_epoch, delisted
        pd.DataFrame(
            [{
                "symbol": symbol,
                "min_epoch": float(first_price_date.timestamp()) if first_price_date is not None else df["epoch"].min(),
                "max_epoch": df["epoch"].max(),
                "delisted": int(delisted),
                "tz_info": str(tz_info)
            }]
        ).to_csv(csv_file + ".meta.csv", index=False)

        # load csv into dolt branch
        if dolt_load:
            print(f"dolt table import -u {quote_table_name} {csv_file}")
            rc, out, err = dolt_load_file(quote_table_name, csv_file)
            if "There are fewer columns in the import file's schema than the table's schema" in err:
                # TODO fix cases where the columns don't match
                print(symbol, out)

            print(f"dolt table import -u {quote_meta_table_name} {csv_file}.meta.csv")
            dolt_load_file(quote_meta_table_name, csv_file + ".meta.csv")
    except Exception as e:
        print("ERROR for", symbol, e)
        traceback.print_exc()
        raise e


def _fetch_last_date(database, symbol, tz_info):
    # check if price data is already available in database and what the latest date is
    query = f""" 
        select min(q.epoch) as min_epoch, max(q.epoch) as max_epoch
          from {quote_table_name} q
          left outer join {quote_meta_table_name} qm on qm.symbol = q.symbol 
         where q.symbol='{symbol}'
           and (qm.delisted = 0 or qm.delisted is null)
    """
    res = fetch_rows(database, query, first_or_none=True)
    has_valid_epoch = res is not None and "max_epoch" in res and res["max_epoch"] is not None
    first_price_date = datetime.datetime.fromtimestamp(float(res["min_epoch"]), tz=tz_info) if has_valid_epoch else None
    last_price_date = datetime.datetime.fromtimestamp(float(res["max_epoch"]), tz=tz_info) if has_valid_epoch else None
    return first_price_date, last_price_date


if __name__ == '__main__':
    cli()
