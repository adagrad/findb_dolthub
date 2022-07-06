import pandas as pd

from commands.yfinance.symbol import YFSession, _save_symbols, _load_symbols, _fetch_existing_symbols, _get_symbol_sets, _get_max_symbol_length, _download_new_symbols
from unittest import TestCase
import os


class TestSymbol(TestCase):

    test_path = os.path.abspath(os.path.dirname(__file__))

    def test__get_max_symbol_length(self):
        self.assertEqual(_get_max_symbol_length("adagrad/findb"), 21)
        self.assertEqual(_get_max_symbol_length("", 12), 12)

    def test__get_symbol_sets(self):
        symbols_file = os.path.join(TestSymbol.test_path, "symbols.txt")

        # get existing symbol from file
        existing_symbols, _ = _get_symbol_sets(symbols_file, "adagrad/findb", None, 1, max_batches=1)
        self.assertEqual(existing_symbols, set(_load_symbols(symbols_file)))

        # get existing symbol from database
        existing_symbols, _ = _get_symbol_sets(None, "adagrad/findb", None, 1, max_batches=1, nr_jobs=1)
        self.assertEqual(len(existing_symbols), 200)

        # resume possible symbol from file
        _, possible_symbols = _get_symbol_sets(symbols_file, "adagrad/findb", symbols_file, 1, max_batches=1)
        self.assertEqual(possible_symbols, set(_load_symbols(symbols_file)))

        # TODO resume possible symbol from database
        #_get_symbol_sets()
        pass

    def test__download_new_symbols(self):
        yfs = YFSession()

        def _mock(r, q):
            return pd.DataFrame({"symbol": [q]})

        self.assertEqual(
            _download_new_symbols("A", set(["A", "B"]), 1, False, yfs, _mock)[1],
            -1
        )
        self.assertEqual(
            _download_new_symbols("F", set(["A", "B"]), 1, False, yfs, _mock)[1],
            1
        )

    def test__fetch_existing_symbols(self):
        existing_symbols = _fetch_existing_symbols("adagrad/findb", 1, 4, 200, 1)
        self.assertIsInstance(existing_symbols, list)

    def test___save_symbols__load_symbols(self):
        file = f"/tmp/{os.path.basename(__file__)}.test"

        src = ["AAA ", "bBB", "ABC", "ABD\n"]
        target = ["AAA", "BBB", "ABC", "ABD"]

        _save_symbols(src, file)
        self.assertListEqual(_load_symbols(file), target)


