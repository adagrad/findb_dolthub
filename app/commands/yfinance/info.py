import inspect
import logging
import os.path
import sys
from datetime import datetime, timedelta

import click
import pandas as pd
from yfinance import Ticker

from modules.df_utils import save_results
from modules.disk_utils import check_disk_full
from modules.dolt_api import fetch_rows
from modules.log import get_logger

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

logging.basicConfig(level=logging.INFO)
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
@click.option('-d', '--database', type=str, default="sqlite:///fin.meta.db.sqlite", help='database connection string containing schema and data')
def cli(time, output, database, known_symbols):
    started = datetime.now()
    max_runtime = datetime.now() + timedelta(minutes=time) if time is not None else None
    output = os.path.abspath(output)

    log.info(f"started at: {started}, write results to {os.path.abspath(output)} run until: {max_runtime}")

    # get starting sets of symbols
    symbols_for_info = _get_symbol_sets(known_symbols, database)

    def early_exit():
        return (time is not None and datetime.now() >= max_runtime) or check_disk_full(output)

    # fetch info
    log.info(f"fetch info for {len(symbols_for_info)} symbols")
    _fetch_info(symbols_for_info, database, output, early_exit)


def _get_symbol_sets(known_symbols_file, repo_database, **kwargs):
    # get known symbols
    if known_symbols_file is not None:
        return _load_symbols(known_symbols_file)

    query = f"select distinct symbol from {symbol_table_name} s where not exists (select 1 from {info_table_name} i where i.symbol = s.symbol)"
    df = fetch_rows(repo_database, query)

    return df["symbol"].tolist()


def _load_symbols(file):
    return [s.strip().upper() for s in open(file).readlines() if len(s.strip()) > 0]


def _fetch_info(symbols, repo_database=None, csv_file=None, eary_exit=None):
    error_count = 0
    for s in symbols:
        log.info(f"get info for {s}")
        try:
            info = Ticker(s).info
            info_dict = {col: info[col] if col in info else None for col in headers}

            # make sure we have symbol primary key for emtpy information
            info_dict["symbol"] = s
            if any([isinstance(v, (tuple, list, dict))for v in info_dict.values()]):
                log.warning(f"{s} has illegal columns! {info_dict}")
            else:
                save_results(
                    repo_database,
                    _rename_columns(pd.DataFrame([info_dict])),
                    True,
                    info_table_name,
                    csv_file,
                    False,
                    index_columns=["symbol", "exchange"]
                )

            error_count = 0
        except Exception as e:
            log.info(f"ERROR for symbol {s}, {e}")
            error_count += 1
        except KeyboardInterrupt:
            break

        if eary_exit is not None and eary_exit():
            break


def _rename_columns(df):
    return df.rename(columns={
        "symbol": "symbol",
        "exchange": "exchange",
        "shortName": "short_name",
        "exchangeTimezoneName": "exchange_timezone",
        "exchangeTimezoneShortName": "exchange_timezone_short",
        "isEsgPopulated": "is_esg_populated",
        "gmtOffSetMilliseconds": "gmt_offset_ms",
        "messageBoardId": "message_board",
        "market": "market",
        "longNamecompanyOfficers": "company_officers",
        "twitter": "twitter",
        "name": "name",
        "startDate": "start_date",
        "description": "description",
        "maxAge": "max_age",
        "zip": "zip",
        "sector": "sector",
        "fullTimeEmployees": "full_time_employees",
        "longBusinessSummary": "long_summary",
        "city": "city",
        "phone": "phone",
        "state": "state",
        "country": "country",
        "website": "website",
        "address1": "address1",
        "address2": "address2",
        "address3": "address3",
        "industryinitInvestment": "industry_init_investment",
        "family": "family",
        "categoryName": "categoryName",
        "initAipInvestment": "init_aip_investment",
        "subseqIraInvestment": "subseq_ira_investment",
        "brokerages": "brokerages",
        "managementInfo": "management_info",
        "subseqInvestment": "subseq_investment",
        "legalType": "legal_type",
        "styleBoxUrl": "style_box_url",
        "feesExpensesInvestment": "fees_expenses_investment",
        "feesExpensesInvestmentCat": "fees_expenses_investment_cat",
        "initIraInvestment": "init_ira_investment",
        "subseqAipInvestment": "subseq_aip_investment",
    })


if __name__ == '__main__':
    cli()
