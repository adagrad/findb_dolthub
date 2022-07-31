import inspect
import logging
import sys
from urllib.parse import unquote, urlparse
from pathlib import PurePosixPath
import click

from modules.dolt_api import execute_shell

logging.basicConfig(level=logging.INFO)

if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())

@click.command()
@click.option('-d', '--database', type=str, default="sqlite:///fin.meta.db.sqlite", help='database connection string containing schema and data')
def cli(database):
    path = PurePosixPath(unquote(urlparse(database).path))
    db_file = str(path)[1:]
    print("dump database", db_file, "from uri", database)
    print(execute_shell("cp", db_file, path.parts[-1]))


if __name__ == '__main__':
    cli()
