from unittest import TestCase

from modules.dolt_api import fetch_symbols, fetch_rows, execute_shell


class TestDoltApi(TestCase):

    def test__fetch_existing_symbols(self):
        existing_symbols = fetch_symbols("adagrad/findb", "symbol = 'AAPL'", 1, 4, 200, 1)
        self.assertIsInstance(existing_symbols, list)
        self.assertGreater(len(existing_symbols), 0)
        if len(existing_symbols) > 0:
            print("Testing on non empty database")
            self.assertEqual(existing_symbols[0], "AAPL")

    def test__fetch_existing_symbols_with_tz(self):
        existing_symbols = fetch_symbols("adagrad/findb", "s.symbol = 'AAPL'", 1, 4, 200, 1, with_timezone=True)
        self.assertIsInstance(existing_symbols, list)
        self.assertGreater(len(existing_symbols), 0)
        if len(existing_symbols) > 0:
            print("Testing on non empty database")
            self.assertEqual(existing_symbols[0], ('AAPL', 'America/New_York'))

    def test_fetch_row(self):
        res = fetch_rows("adagrad/findb", "select * from yfinance_symbol where symbol = 'AAPL'")
        self.assertIsInstance(res, list)
        if len(res) > 0:
            print("Testing on non empty database")
            self.assertEqual(
                res[0],
                {
                    'active': '1',
                    'exchange': 'NMS',
                    'exchange_description': 'NASDAQ',
                    'name': 'Apple Inc.',
                    'symbol': 'AAPL',
                    'type': 'S',
                    'type_description': 'Equity'
                }
            )

            self.assertIsNotNone(fetch_rows("adagrad/findb", "select * from yfinance_symbol where symbol = 'AAPL'", first_or_none=True))

    def test_run_shell_command(self):
        rc, out, err = execute_shell("dolt", "table", "import", "-u", "lala", "foo.csv")
        self.assertTrue('The current directory is not a valid dolt repository' in err)