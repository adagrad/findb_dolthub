from unittest import TestCase

from modules.dolt_api import fetch_symbols, fetch_rows


class TestDoltApi(TestCase):

    def test__fetch_existing_symbols(self):
        existing_symbols = fetch_symbols("adagrad/findb", 'yfinance_symbol', "symbol = 'AAPL'", 1, 4, 200, 1)
        self.assertIsInstance(existing_symbols, list)
        if len(existing_symbols) > 0:
            print("Testing on non empty database")
            self.assertEqual(existing_symbols[0], "AAPL")

    def test_fetch_row(self):
        res = fetch_rows("adagrad/findb", "select * from yfinance_symbol where symbol = 'AAPL'")
        self.assertIsInstance(res, list)
        if len(res) > 0:
            print("Testing on non empty database")
            self.assertEqual(
                res[0],
                {
                    'exchange': 'NMS',
                     'exchange_description': 'NASDAQ',
                     'name': 'Apple Inc.',
                     'symbol': 'AAPL',
                     'type': 'S',
                     'type_description': 'Equity'
                }
            )

            self.assertIsNotNone(fetch_rows("adagrad/findb", "select * from yfinance_symbol where symbol = 'AAPL'", first_or_none=True))