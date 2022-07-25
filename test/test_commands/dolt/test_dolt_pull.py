import tempfile
from unittest import TestCase

import pandas as pd
import numpy as np

class TestDoltPull(TestCase):

    def test_init_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            pass