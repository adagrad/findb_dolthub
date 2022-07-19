import subprocess
from time import sleep

import pandas as pd
import pytest


def test_csv(server_started):
    df = pd.read_csv("http://127.0.0.1:9876/api/ohlcv/yfinance/MSFT,AAPL?__axis=0")
    if len(df) <= 0:
        print("empty frame!")
    else:
        print(df)


def test_csv_joined(server_started):
    df = pd.read_csv("http://127.0.0.1:9876/api/ohlcv/yfinance/MSFT,AAPL?__axis=1")
    if len(df) <= 0:
        print("empty frame!")
    else:
        print(df)


def test_pickle(server_started):
    df = pd.read_pickle("http://127.0.0.1:9876/api/ohlcv/yfinance/MSFT,AAPL?__axis=0&__as=pickle")
    if len(df) <= 0:
        print("empty frame!")
    else:
        print(df)

    assert isinstance(df.index, pd.MultiIndex), f"Index is not MultiIndex {df.index}"
    assert isinstance(df.index.get_level_values(1), pd.DatetimeIndex), f"Index is not DateTime Index on level 1 {df.index}"


def test_parquet(server_started):
    df = pd.read_parquet("http://127.0.0.1:9876/api/ohlcv/yfinance/MSFT,AAPL?__axis=0&__as=parquet")
    if len(df) <= 0:
        print("empty frame!")
    else:
        print(df)

    assert isinstance(df.index, pd.MultiIndex), f"Index is not MultiIndex {df.index}"
    assert isinstance(df.index.get_level_values(1), pd.DatetimeIndex), f"Index is not DateTime Index on level 1 {df.index}"


@pytest.fixture()
def server_started():
    import serve.api
    sp = subprocess.Popen(["python", serve.api.__file__])
    sleep(1)

    yield True

    sp.kill()