from unittest import TestCase

from commands.yfinance.info import _fetch_info, headers


class TestInfo(TestCase):

    def test__fetch_info(self):
        df = _fetch_info(["AAPL"])
        self.assertEqual(
            (1, len(headers)),
            df.shape
        )