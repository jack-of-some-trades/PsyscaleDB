import pandas as pd

import pytest
import pytest_asyncio


@pytest.fixture(scope="module")
def spy_data():
    df = pd.read_csv("example_data/spy_data.csv")
    yield df


@pytest.fixture(scope="module")
def sync_db(psyscaledb): ...


@pytest_asyncio.fixture(scope="module")
async def async_db(psyscale_async): ...
