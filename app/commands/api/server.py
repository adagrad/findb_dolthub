import inspect
import sys

from serve.api import cli


if not hasattr(sys.modules[__name__], '__file__'):
    __file__ = inspect.getfile(inspect.currentframe())


if __name__ == '__main__':
    cli()
