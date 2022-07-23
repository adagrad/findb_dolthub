import datetime
import inspect
import os
import sys
import threading
from datetime import timedelta
from functools import partial

import click
import pandas as pd
import pytz
from yfinance import download, Ticker
from yfinance.utils import auto_adjust

from modules.dolt_api import fetch_symbols, fetch_rows, dolt_load_file
from modules.log import get_logger
from modules.threaded import execute_parallel

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

log = get_logger(f'{threading.current_thread().name}.quotes.log')

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
@click.option('--clean', default=False, is_flag=True, help='Deletes intermediary files directly after load (only works together with --dolt-load)')
def cli(time, repo_database, where, symbols, output_dir, parallel_threads, dolt_load, clean):
    max_runtime = datetime.datetime.now() + timedelta(minutes=time) if time is not None else None
    if repo_database == "" or repo_database == "None":
        repo_database = None

    # select tickers from dolt using a where query or allow a list of symbols to be passed as a file
    assert (where is not None and repo_database is not None) or symbols is not None, \
        "Either a query (and database repository) or a symbols file has to be provided"

    print(f"select symbols where {where}")
    symbols = _select_tickers(repo_database, where, symbols)

    execute_parallel(
        partial(
            _fetch_data,
            database=repo_database,
            dolt_load=dolt_load,
            path=output_dir,
            max_runtime=max_runtime,
            clean=clean,
        ),
        symbols,
        "symbol",
        parallel_threads
    )


def _select_tickers(database, where, symbols_file, nr_jobs=5):
    if symbols_file is not None:
        return [s.strip().upper() for s in open(symbols_file).readlines() if len(s.strip()) > 0]

    fetched_symbols = fetch_symbols(database, where, with_timezone=True, nr_jobs=nr_jobs)
    print(f"fetched {len(fetched_symbols)} symbols from database")
    return fetched_symbols


def _fetch_data(database, symbol, path='.', dolt_load=False, max_runtime=None, clean=False):
    if max_runtime is not None and datetime.datetime.now() >= max_runtime:
        log.info("max time reached exit before fetching", symbol)
        return

    # split symbol and time zone
    if isinstance(symbol, tuple):
        symbol, tz_info = symbol[0], pytz.timezone(symbol[1])
    else:
        tz_info = pytz.timezone('US/Eastern')

    # define result file name
    csv_file = os.path.abspath(os.path.join(path, f"{str(symbol)}.csv"))

    # try to fetch quotes
    try:
        first_price_date, last_price_date, delisted = _fetch_last_date(database, symbol, tz_info)
        if delisted:
            log.warn(f"{symbol} is marked as delisted, skipping!")
            return  # TODO later allow action on delisted items

        try:
            if last_price_date is None:
                log.info(f"{symbol}: no last price date available, fetch max history")
                #df = download([symbol], progress=False, show_errors=False)
                df = Ticker(symbol).history(period='max')
            else:
                # fetch data, and overwrite the last couple of days in case of error corrections
                log.info(f"{symbol}: fetch for new prices from {last_price_date}")
                # df = download([symbol], start=(last_price_date - timedelta(days=5)).date(), progress=False, show_errors=False)
                df = Ticker(symbol).history(start=(last_price_date - timedelta(days=5)).date())
        except KeyError as ke:
            log.warn(f"dataframe does not have key {ke} {symbol}")
            df = pd.DataFrame({})

        if len(df) > 0:
            # fix hick ups
            if "Adj. Close" in df.columns:
                log.warn("Auto Adjustment failed for some reason")
                df = auto_adjust(df)

            if "Dividends" not in df.columns:
                log.warn("Add missing column Dividends")
                df["Dividends"] = None

            if "Stock Splits" not in df.columns:
                log.warn("Add missing column Stock Splits")
                df["Stock Splits"] = None

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
            log.info(f"save csv for {symbol} containing {len(df)} rows to {csv_file}")
            df.to_csv(csv_file, index=False)

            min_epoch = float(first_price_date.timestamp()) if first_price_date is not None else df["epoch"].min()
            max_epoch = df["epoch"].max()
        else:
            log.info(f"{symbol} no data found, might be delisted")
            delisted = True
            min_epoch = None
            max_epoch = None

        # add another csv file to load containing symbol, min_epoch, max_epoch, delisted
        pd.DataFrame(
            [{
                "symbol": symbol,
                "min_epoch": min_epoch,
                "max_epoch": max_epoch,
                "delisted": int(delisted),
                "tz_info": str(tz_info)
            }]
        ).to_csv(csv_file + ".meta.csv", index=False)

        # load csv into dolt branch
        if dolt_load and os.path.exists(csv_file + ".meta.csv"):
            if os.path.exists(csv_file):
                log.info(f"dolt table import -u {quote_table_name} {csv_file}")
                rc, out, err = dolt_load_file(quote_table_name, csv_file)
                if "There are fewer columns in the import file's schema than the table's schema" in err:
                    log.warn(f"{symbol}, {out}, {err}")

                if rc != 0:
                    log.error(f"ERROR: load {csv_file} failed\n{out}\n{err}", exc_info=False)
                    raise ValueError(f"ERROR: load {csv_file} failed\n{out}\n{err}")

            log.info(f"dolt table import -u {quote_meta_table_name} {csv_file}.meta.csv")
            rc, _, _ = dolt_load_file(quote_meta_table_name, csv_file + ".meta.csv")

            if rc != 0:
                log.error(f"ERROR: load {csv_file}.meta.csv failed\n{out}\n{err}", exc_info=False)
                raise ValueError(f"ERROR: load {csv_file}.meta.csv failed\n{out}\n{err}")

            if clean:
                try:
                    if os.path.exists(csv_file): os.unlink(csv_file)
                    os.unlink(f"{csv_file}.meta.csv")
                except Exception as ignore:
                    log.info("error unlinking file: " + csv_file, ignore)
    except Exception as e:
        log.error(f"ERROR for symbol: {symbol}, {e}", exc_info=1)
        raise e


def _fetch_last_date(database, symbol, tz_info, verbose=False):
    # check if price data is already available in database and what the latest date is
    query = f""" 
        select min(q.epoch) as min_epoch, max(q.epoch) as max_epoch, coalesce(qm.delisted, 0) as delisted
          from {quote_table_name} q
          left outer join {quote_meta_table_name} qm on qm.symbol = q.symbol 
         where q.symbol='{symbol}'
    """

    if verbose: log.info(query)

    try:
        res = fetch_rows(database, query, first_or_none=True)
        if verbose: log.info(res)

        has_valid_epoch = res is not None and "max_epoch" in res and res["max_epoch"] is not None
        first_price_date = datetime.datetime.fromtimestamp(float(res["min_epoch"]), tz=tz_info) if has_valid_epoch else None
        last_price_date = datetime.datetime.fromtimestamp(float(res["max_epoch"]), tz=tz_info) if has_valid_epoch else None
        delisted = res['delisted'] if has_valid_epoch else 0
        return first_price_date, last_price_date, int(delisted)
    except Exception as e:
        log.warn(f"{symbol}: last min/max epoch query failed, load full history: {e}")
        return None, None, 0


if __name__ == '__main__':
    cli()
