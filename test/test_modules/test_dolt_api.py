import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import sleep
from unittest import TestCase

import pandas as pd

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
        self.assertIsInstance(res, pd.DataFrame)
        if len(res) > 0:
            print("Testing on non empty database")
            self.assertEqual(
                {0: {
                    'active': '1',
                    'exchange': 'NMS',
                    'exchange_description': 'NASDAQ',
                    'name': 'Apple Inc.',
                    'symbol': 'AAPL',
                    'type': 'S',
                    'type_description': 'Equity'
                }},
                res.iloc[0].to_dict(),
            )

            self.assertIsNotNone(fetch_rows("adagrad/findb", "select * from yfinance_symbol where symbol = 'AAPL'", first_or_none=True))

    def test_run_shell_command(self):
        rc, out, err = execute_shell("dolt", "table", "import", "-u", "lala", "foo.csv")
        self.assertTrue('The current directory is not a valid dolt repository' in err)
        self.assertNotEqual(rc, 0)

    def test_shell_threadlock(self):
        file = "test_shell_threadlock.txt"
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = \
                [executor.submit(
                    partial(
                        execute_shell,
                        "bash", "-c", f'echo {i} >> {file}'
                    )
                ) for i in range(10)]

        executor.shutdown(wait=True)
        rc = sum([f.result()[0] for f in futures])
        lines = open(file).readlines()
        try:
            self.assertEqual(rc, 0)
            self.assertListEqual(lines, ['0\n', '1\n', '2\n', '3\n', '4\n', '5\n', '6\n', '7\n', '8\n', '9\n'])
        finally:
            os.unlink(file)