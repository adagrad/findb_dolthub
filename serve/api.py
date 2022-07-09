from quart import Quart, render_template, websocket
from dotenv import load_dotenv
from pyaml_env import parse_config
from os import path

# load environment
PATH = path.abspath(path.dirname(__file__))
load_dotenv() # local
load_dotenv('/etc/app/config/.env') # when deployed to kubernetes
config = parse_config(path.join(PATH, 'config.yml'))

# create app
app = Quart(__name__)


@app.route("/api")
async def json():
    return {"hello": "world"}


# start server
if __name__ == "__main__":
    app.run()