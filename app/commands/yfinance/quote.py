import datetime
import inspect
import logging
import os
import sys
import threading
from datetime import timedelta
from functools import partial

import click
import pandas as pd
import pytz
from yfinance import Ticker
from yfinance.utils import auto_adjust

from modules.df_utils import df_to_csv, save_results
from modules.disk_utils import check_disk_full
from modules.dolt_api import fetch_rows
from modules.log import get_logger
from modules.threaded import execute_parallel

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

logging.basicConfig(level=logging.INFO)
log = get_logger(f'{threading.current_thread().name}.quotes.log')

quote_meta_table_name = 'yfinance_quote_meta'
quote_tz_table_name = 'tzinfo_exchange'
quote_table_name = 'yfinance_quote'


@click.command()
@click.option('-t', '--time', type=int, default=None, help='Maximum runtime in minutes')
@click.option('-d', '--database', type=str, default="sqlite:///fin.db.sqlite", help='database connection string containing schema and data')
@click.option('-i', '--inactive', type=int, default=None, help='Whether only active (0)/inactive(1) or both (default) symbols should be fetched')
@click.option('-o', '--output-dir', type=str, default='.', help='Path to store downloaded csv files')
@click.option('-p', '--parallel-threads', type=int, default=10, help='Number of parallel threads')
@click.option('-n', '--include-new', default=False, is_flag=True, help='Also look for symbols without any quote (need main branch to be present)')
@click.option('--clean', default=False, is_flag=True, help='Deletes intermediary files directly after load (only works together with --dolt-load)')
def cli(time, database, inactive, output_dir, parallel_threads, include_new, clean):
    max_runtime = datetime.datetime.now() + timedelta(minutes=time) if time is not None else None
    log.info(f"jobs ends at: {max_runtime}")

    if database == "" or database == "None":
        database = None

    log.info(f"select symbols where inactive = {inactive}")
    last_state = _select_last_state(database, include_new)

    if inactive is not None:
        last_state = last_state[last_state["delisted"] == inactive]

    log.info(f"found {len(last_state)} quotes to fetch prices for")

    def early_exit():
        return (max_runtime is not None and datetime.datetime.now() >= max_runtime) or check_disk_full()

    download_parallel(database, last_state, max_runtime, output_dir, True, clean, parallel_threads, early_exit)


def download_parallel(repo_database, last_state, max_runtime, output_dir, dolt_load=False, clean=False, num_threads=4, early_exit=None):
    execute_parallel(
        partial(
            _fetch_data,
            database=repo_database,
            dolt_load=dolt_load,
            path=output_dir,
            early_exit=early_exit,
            clean=clean,
        ),
        last_state.iterrows(),
        "last_state",
        num_threads,
        early_exit=early_exit
    )

    print("done!")


def _select_last_state(database, include_new_symbols=False):
    if include_new_symbols:
        query = """
            select s.symbol, m.min_epoch as first_quote_epoch, m.max_epoch as last_quote_epoch, e.timezone as tz_info, coalesce(m.delisted, 0) as delisted, null as volume
              from yfinance_symbol s
              join yfinance_exchange_info e on e.symbol = s.exchange
              left outer join yfinance_quote_meta m on m.symbol = s.exchange
             where m.max_epoch is null or m.max_epoch < strftime('%s', date(current_date, '-1 days'))
             order by m.max_epoch is null desc, m.max_epoch asc
        """
    else:
        query = """
            select symbol, min_epoch as first_quote_epoch, max_epoch as last_quote_epoch, tz_info, delisted, null as volume
              from yfinance_quote_meta
             where (last_quote_epoch is null or last_quote_epoch < strftime('%s', date(current_date, '-1 days')))
             order by last_quote_epoch is null desc, last_quote_epoch asc
        """

    return fetch_rows(database, query)


def _fetch_data(database, last_state, path='.', dolt_load=False, early_exit=None, clean=False):
    last_state = last_state[1] if isinstance(last_state, tuple) else last_state
    symbol = last_state["symbol"]
    tz_info = pytz.timezone(last_state["tz_info"]) if last_state["tz_info"] is not None else pytz.timezone('US/Eastern')
    first_price_date, last_price_date = last_state["first_quote_epoch"], last_state["last_quote_epoch"]
    delisted = last_state["delisted"] if last_state["delisted"] is not None else 0

    # check early exit
    if early_exit is not None and early_exit():
        log.warning(f"max time reached or disk almost full, exit before fetching {symbol}")
        return f"skipped {symbol}"

    # parse dates
    first_price_date = datetime.datetime.fromtimestamp(first_price_date, tz=tz_info) if not pd.isna(first_price_date) else None
    last_price_date = datetime.datetime.fromtimestamp(last_price_date, tz=tz_info) if not pd.isna(last_price_date) else None

    # define result file name
    csv_file = os.path.abspath(os.path.join(path, f"{str(symbol)}.csv"))

    # try to fetch quotes
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
        df_to_csv(df, csv_file)

        min_epoch = float(first_price_date.timestamp()) if first_price_date is not None else df["epoch"].min()
        max_epoch = df["epoch"].max()
    else:
        log.info(f"{symbol} no data found, might be delisted")
        delisted = True
        min_epoch = None
        max_epoch = None

    # save all results
    _save_results(database, df, dolt_load, csv_file, clean, symbol, min_epoch, max_epoch, delisted, tz_info)

    # just to return something to the executor
    return symbol


def _save_results(database, df, dolt_load,  csv_file, clean, symbol, min_epoch, max_epoch, delisted, tz_info):
    if len(df) > 0:
        # safe df
        if pd.Series(['symbol', 'epoch']).isin(df.columns).all():
            save_results(database, df, dolt_load, quote_table_name, csv_file, clean, index_columns=["symbol", "epoch"])
        else:
            log.error(f"some strange dataframe for {symbol}\n{df.head()}")
            delisted = True

    # save metadata
    save_results(
        database,
        pd.DataFrame(
            [{
                "symbol": symbol,
                "min_epoch": min_epoch,
                "max_epoch": max_epoch,
                "delisted": int(delisted),
                "tz_info": str(tz_info)
            }]
        ),
        dolt_load,
        quote_meta_table_name,
        csv_file + ".meta.csv",
        clean,
        index_columns="symbol"
    )


if __name__ == '__main__':
    cli()
