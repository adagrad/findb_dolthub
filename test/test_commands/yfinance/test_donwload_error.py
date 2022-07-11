import io
import os
from unittest import TestCase
import yfinance as yf
import contextlib


class TestYFDownloadErrors(TestCase):

    def test_symbol_delisted(self):
        "No data found, symbol may be delisted"

        with io.StringIO() as output:
            with contextlib.redirect_stdout(output):
                yf.Ticker("VTA").history()

            self.assertTrue("No data found, symbol may be delisted" in output.getvalue())

    def test_os_out(self):
        with io.StringIO() as output:
            with contextlib.redirect_stdout(output):
                os.system("ls -l")

            print(output.getvalue())