import pandas as pd
import pytest


@pytest.mark.asyncio
async def test_ping_request(client):
    response = await client.get("/api/ping")
    data = await response.get_data()
    assert b"pong" in data


@pytest.mark.asyncio
async def test_ohlcv_columns(client):
    response = await client.get("/api/ohlcv?yahoo=MSFT,AAPL&fred=GDP")
    data = await response.get_data()
    print(data)
    assert b"MSFT,MSFT,AAPL,AAPL,GDP,GDP" in data


@pytest.mark.asyncio
async def test_ohlcv_rows(client):
    response = await client.get("/api/ohlcv?yahoo=MSFT,AAPL&fred=GDP&__axis=0")
    data = await response.get_data()
    print(data)
    assert b",,open,close" in data


@pytest.mark.asyncio
async def test_ohlcv_same_source(client):
    response = await client.get("/api/ohlcv/yahoo/MSFT,AAPL?__as=json")
    data = await response.get_data()
    print(data)
    assert b'{"(\'MSFT\', \'open\')"' in data


@pytest.fixture()
def app():
    from serve.dataaccess import ohlcv

    # setup mock implementations
    async def _mock_fetch( *args, **kwargs):
        return pd.DataFrame({"open": [1, 2, 3], "close": [1, 2, 3]})

    ohlcv._fetch = _mock_fetch

    # setup app
    from serve.api import app
    app.config.update({
        "TESTING": True,
        "ENV": "UnitTest"
    })

    yield app

    # clean up / reset resources here


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def runner(app):
    return app.test_cli_runner()

