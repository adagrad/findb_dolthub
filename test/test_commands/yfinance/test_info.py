import tempfile
from unittest import TestCase

import pandas as pd

from commands.yfinance.info import _fetch_info, headers


class TestInfo(TestCase):

    def test__fetch_info(self):
        with tempfile.TemporaryDirectory() as dir:
            _fetch_info(["AAPL"], csv_file=f"{dir}/out.csv")
            df = pd.read_csv(f"{dir}/out.csv")

            self.assertEqual(
                (1, len(headers)),
                df.shape
            )