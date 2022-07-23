from functools import partial
from unittest import TestCase

from modules.threaded import execute_parallel


class TestThreaded(TestCase):

    def test_execute_parallel(self):
        def func(a, b, c):
            return f'{a}-{b}-{c}'

        results = execute_parallel(partial(func, a="A", b="B"), range(10), "c")
        self.assertListEqual(
            results,
            ['A-B-0', 'A-B-1', 'A-B-2', 'A-B-3', 'A-B-4', 'A-B-5', 'A-B-6', 'A-B-7', 'A-B-8', 'A-B-9']
        )
