import datetime
import json
import mimetypes
import os.path
import subprocess
import urllib.parse
from os import path
from time import sleep

from dotenv import load_dotenv
from pyaml_env import parse_config
from quart import Quart, request, make_response, stream_with_context

# load environment
from serve.utuls.request_util import get_data_args
from serve.dataaccess.ohlcv import fetch_ohlcv
from serve.datastream.pandas_response import make_pandas_response

PATH = path.abspath(path.dirname(__file__))
load_dotenv() # local
load_dotenv('/etc/app/config/.env') # when deployed to kubernetes
config = parse_config(path.join(PATH, 'conf.yml'))

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


# start server
if __name__ == "__main__":
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
