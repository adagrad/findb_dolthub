import os
import os
import tempfile
from unittest import TestCase

import pandas as pd

from commands.yfinance.quote import _fetch_data, download_parallel


class TestQuote(TestCase):

    def test__fetch_data(self):
        last_state = pd.DataFrame([
            {"symbol": "AAPL", "first_quote_epoch": None, "last_quote_epoch": None, "tz_info": 'US/Eastern', "delisted": 0},
            {"symbol": "MSFT", "first_quote_epoch": None, "last_quote_epoch": None, "tz_info": None, "delisted": 0},
            {"symbol": "XXXXXXXXX", "first_quote_epoch": None, "last_quote_epoch": None, "tz_info": None, "delisted": 0},
        ])

        with tempfile.TemporaryDirectory() as tmp:
            download_parallel(None, last_state, None, tmp, False, False, 2)
            self.assertTrue(os.path.exists(os.path.join(tmp, "AAPL.csv")), "AAPL.csv exists")
            self.assertTrue(os.path.exists(os.path.join(tmp, "AAPL.csv.meta.csv")), "AAPL.csv meta exists")
            self.assertTrue(os.path.exists(os.path.join(tmp, "MSFT.csv")), "MSFT.csv exists")
            self.assertTrue(os.path.exists(os.path.join(tmp, "MSFT.csv.meta.csv")), "MSFT.csv meta exists")
            self.assertTrue(os.path.exists(os.path.join(tmp, "XXXXXXXXX.csv")), "XXXXXXXXX.csv exists")
            self.assertTrue(os.path.exists(os.path.join(tmp, "XXXXXXXXX.csv.meta.csv")), "XXXXXXXXX.csv meta exists")

            df = pd.read_csv(os.path.join(tmp, 'AAPL.csv'))
            self.assertGreater(len(df), 0)

            df = pd.read_csv(os.path.join(tmp, 'AAPL.csv.meta.csv'))
            self.assertEqual(len(df), 1)

            df = pd.read_csv(os.path.join(tmp, 'XXXXXXXXX.csv.meta.csv'))
            self.assertEqual(len(df), 1)
