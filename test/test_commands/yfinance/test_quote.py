import io
import os
import tempfile
from unittest import TestCase

import pandas as pd
import yfinance as yf
import contextlib
from commands.yfinance.quote import _fetch_data, _fetch_last_date


class TestQuote(TestCase):

    def test__fetch_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            _fetch_data("adagrad/findb", 'AAPL', path=tmp, dolt_load=False, max_runtime=None)
            df = pd.read_csv(os.path.join(tmp, 'AAPL.csv'))
            self.assertGreater(len(df), 0)

        with tempfile.TemporaryDirectory() as tmp:
            _fetch_data("adagrad/findb", ('AAPL', 'America/New_York'), path=tmp, dolt_load=False, max_runtime=None)
            df=pd.read_csv(os.path.join(tmp, 'AAPL.csv'))
            self.assertGreater(len(df), 0)
            self.assertEqual(df["tzinfo"].iloc[0], 'America/New_York')

    def test__fetch_last_date(self):
        self.assertIsNotNone(_fetch_last_date("adagrad/findb", "AAPL", None)[1])

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

    def test_debugging(self):
        print(_fetch_last_date("adagrad/findb", "AAAWX", None, verbose=True))
        """
        ./AAMBFX.csv
            Warning: There are fewer columns in the import file's schema than the table's schema.
                If unintentional, check for any typos in the import file's header.
                Rows Processed: 0, Additions: 0, Modifications: 0, Had No Effect: 0
        """