import os.path
from os import path

from dotenv import load_dotenv
from pyaml_env import parse_config


class Config():

    app_path = path.abspath(path.dirname(__file__))
    conf_path = os.path.join(app_path, "conf.yml")
    env_path = os.path.join(app_path, ".env")

    @staticmethod
    def get():
        if os.path.exists(Config.env_path):
            load_dotenv(Config.env_path)

        return parse_config(Config.conf_path)
