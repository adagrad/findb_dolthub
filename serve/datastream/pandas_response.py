import logging
import mimetypes
from tempfile import TemporaryFile

import pandas as pd

log = logging.getLogger(__name__)


def make_pandas_response(df: pd.DataFrame, result_type, **kwargs):
    async def generator():
        with TemporaryFile() as tmp:
            result = getattr(df, f"to_{result_type}")(tmp, **kwargs)
            tmp.seek(0)
            yield tmp.read()

    return (
        generator(),
        200,
        {'Content-Type': mimetypes.guess_type(f'test.{result_type}')[0], 'Pandas-Version': pd.__version__}
    )
