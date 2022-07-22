import datetime
import json
import mimetypes
import os.path
import subprocess
import sys
from time import sleep

import click
from quart import Quart, request

from serve.config import Config
from serve.dataaccess.ohlcv import fetch_ohlcv
from serve.datastream.pandas_response import make_pandas_response
from serve.utils.request_util import get_data_args

# create app
app = Quart(__name__)

# the end of day data path is mapped to the EOD table.
#
# eod_ohlcv:
#  source --like yahoofinance or investing.com etc.
#  symbol  -- like aapl, etc
#  date  -- as long or float with ms!!
#  o
#  h
#  l
#  v
#  pk (source, symbol, date)
#
# then we have different endpoints after the path /eod_ohlcv/
# while all support a generic __where__= query parameter allowing SQL injection (but read SQLs only) and
# a__ separator__=, parameter to split multiple symbols and finally one to determine the return type
# __as__=JSON,pickle,hdf,...


# use query parameters to provide source and symbols
# ?Yahoo=aapl,msft&investing=10y,...
@app.route("/api/ohlcv")
async def ohlcv():
    separator, as_type, where, axis, pandas_kwargs = get_data_args(request.args)

    symbols = {source: symbols.split(separator) for source, symbols in request.args.items() if not source.startswith("__")}
    df = await fetch_ohlcv(symbols, where, axis)
    return make_pandas_response(df, as_type, **pandas_kwargs)


# get joined symbols
# get from the same source one or more symbols
# /Yahoo/aapl,msft,...
@app.route("/api/ohlcv/<string:source>/<string:symbols>")
async def ohlcv_source(source, symbols):
    separator, as_type, where, axis, pandas_kwargs = get_data_args(request.args)
    symbols = symbols.split(separator)

    df = await fetch_ohlcv({source: symbols}, where, axis)
    return make_pandas_response(df, as_type, **pandas_kwargs)


# a route to just the test that the server is up and responses
@app.route("/api")
async def base():
    return request.base_url


@app.route("/api/ping")
async def ping():
    # @stream_with_context
    async def async_generator():
        for i in range(5):
            yield json.dumps({"pong": datetime.datetime.utcnow().timestamp()}).encode("UTF-8")

    return async_generator(), 200, {'Content-Type': mimetypes.guess_type(f'test.json')[0]}


@click.command()
@click.option('-e', '--env-file', type=str, default=None, help='Config file')
@click.option('-c', '--config-file', type=str, default=None, help='Config file')
def cli(env_file, config_file):
    # overwrite configuration
    if env_file is not None: Config.env_path = env_file
    if config_file is not None: Config.conf_path = config_file

    config = Config.get()
    print("starting api using", config)
    dolt_server_process = None

    try:
        dolt_server_command = config["dolt"]["repository"].get("start_server", None)
        if dolt_server_command is not None and len(dolt_server_command) > 0:
            print("start dolt server", dolt_server_command)
            dolt_server_process = subprocess.Popen(dolt_server_command, cwd=os.path.join(PATH, config["dolt"]["repository"].get("path", ".")))
            sleep(1)

        print("starting http server")
        app.run(
            config["server"].get("host", None),
            config["server"].get("port", None),
        )
    finally:
        if dolt_server_process is not None: dolt_server_process.kill()
        app.shutdown()


# start server
if __name__ == "__main__":
    cli()