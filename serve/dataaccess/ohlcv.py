import logging
from datetime import datetime
from typing import Dict

import asyncio
import pandas as pd
import pytz

log = logging.getLogger(__name__)


async def fetch_ohlcv(symbols: Dict[str, list], where=None, axis=1) -> pd.DataFrame:
    log.info(f"fetch data for symbols: {symbols} where: {where}")
    tasks, index_symbols = zip(*[(_fetch(source, symbol, where), symbol) for source, src_symbols in symbols.items() for symbol in src_symbols])
    frames = await asyncio.gather(*tasks)
    if len(frames) > 1:
        return pd.concat([frame for frame in frames], join='outer', axis=axis, keys=index_symbols)
    else:
        return frames[0]


async def _fetch(source, symbol, where):
    df = pd.read_sql(
        f"select * from {source}_quote where symbol='{symbol.upper()}' and {where} order by epoch",
        'mysql+pymysql://root:@localhost/findb'
    )

    def get_tz(tzinfo_str):
        try:
            return pytz.timezone(tzinfo_str)
        except Exception:
            return None

    if len(df) > 0:
        df.index = pd.DatetimeIndex(
            df[["epoch", "tzinfo"]]\
                .apply(lambda x: datetime.fromtimestamp(x["epoch"], get_tz(x["tzinfo"])), axis=1).rename("time")
        )
    else:
        df.index = pd.DatetimeIndex([], name="time", tz=pytz.timezone('UTC'))

    return df

