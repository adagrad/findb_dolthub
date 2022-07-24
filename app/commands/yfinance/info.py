import inspect
import os.path
import sys
from datetime import datetime, timedelta

import click
import pandas as pd
from yfinance import Ticker

from modules.dolt_api import fetch_rows, dolt_load_file
from modules.log import get_logger

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

log = get_logger(__name__ + ".log")

headers = "symbol exchange shortName exchangeTimezoneName exchangeTimezoneShortName isEsgPopulated gmtOffSetMilliseconds messageBoardId market longName" \
          "companyOfficers twitter name startDate description maxAge zip sector fullTimeEmployees longBusinessSummary city phone state country website address1 address2 address3 industry" \
          "initInvestment family categoryName initAipInvestment subseqIraInvestment brokerages managementInfo subseqInvestment legalType styleBoxUrl feesExpensesInvestment feesExpensesInvestmentCat initIraInvestment subseqAipInvestment"\
          .split(" ")

symbol_table_name = 'yfinance_symbol'
info_table_name = 'yfinance_symbol_info'
max_errors = 50

@click.command()
@click.option('-t', '--time', type=int, default=None, help='Maximum runtime in minutes')
@click.option('-o', '--output', type=str, default="yfinfo.csv", help='Filename holding the results, appends if exists (default=yfinfo.csv)')
@click.option('-s', '--known-symbols', type=str, default=None, help='Provide known symbols file instead of fetching them (one sybol per line)')
@click.option('-d', '--repo-database', type=str, default="adagrad/findb", help='Dolthub repository and database name (default=adagrad/findb)')
@click.option('--dolt-load', default=False, is_flag=True, help='Load file into local dolt database branch')
def cli(time, output, repo_database, known_symbols, dolt_load):
    started = datetime.now()
    max_runtime = datetime.now() + timedelta(minutes=time) if time is not None else None
    output = os.path.abspath(output)

    log.info(f"started at: {started}, write results to {os.path.abspath(output)} run until: {max_runtime}")

    # get starting sets of symbols
    symbols_for_info = _get_symbol_sets(known_symbols, repo_database)

    # fetch info
    log.info(f"fetch info for {len(symbols_for_info)} symbols")
    df = _fetch_info(symbols_for_info, max_runtime)

    # save result
    _save_result(df, output)

    # finalize the program
    log.info(f"dolt table import -u {info_table_name} {output}")
    if dolt_load:
        rc, out, err = dolt_load_file(info_table_name, output)
        exit(rc)
    else:
        exit(0)


def _get_symbol_sets(known_symbols_file, repo_database, **kwargs):
    # get known symbols
    if known_symbols_file is not None:
        return _load_symbols(known_symbols_file)

    query = f"select symbol from {symbol_table_name} s where not exists (select 1 from {info_table_name} i where i.symbol = s.symbol)"
    df = fetch_rows(repo_database, query)

    return df["symbol"].tolist()


def _load_symbols(file):
    return [s.strip().upper() for s in open(file).readlines() if len(s.strip()) > 0]


def _fetch_info(symbols, max_until=None):
    infos = []
    error_count = 0
    for s in symbols:
        log.info(f"get info for {s}")
        try:
            info = Ticker(s).info
            infos.append(
                {col: info[col] if col in info else None for col in headers}
            )

            error_count = 0
        except Exception as e:
            log.info(f"ERROR for symbol {s}, {e}")
            error_count += 1
        except KeyboardInterrupt:
            break

        if max_until is not None and datetime.now() >= max_until or error_count >= max_errors:
            break

    return pd.DataFrame(infos)


def _save_result(df, file):
    log.info(f"save dataframe to {file}")
    if os.path.exists(file):
        with open(file, 'a') as f:
            df.to_csv(f, header=False, index=False)
    else:
        df.to_csv(file, header=True, index=False)


if __name__ == '__main__':
    cli()
