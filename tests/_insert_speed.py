"Optional Tests to easily measure data insert speeds. Add a 'test_' prefix to functions to run."

from time import time
import pandas as pd

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from psyscale import PsyscaleAsync, PsyscaleDB
from psyscale.core import TIMESCALE_IMAGE
from psyscale.psql.orm import TimeseriesConfig
from psyscale.series_df import Series_DF


SYMBOLS = pd.DataFrame(
    [
        {
            "symbol": "SPY",
            "name": "S&P 500 ET",
            "exchange": "NYSE",
            "asset_class": "equity",
            "sector": "Technology",
        }
    ]
)


MINUTE_CONFIG = TimeseriesConfig(
    ["equity"],  # type:ignore
    rth_origins={
        "equity": pd.Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "equity": pd.Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"equity": True},
    calculated_periods={"default": [pd.Timedelta("5m"), pd.Timedelta("30m"), pd.Timedelta("1h")]},
    stored_periods={"default": [pd.Timedelta("1m")]},
)


@pytest.fixture(scope="module")
def spy_data():
    t_start = time()
    # Replace the *s with the name of a large csv file. I did ~1.75M Rows
    df = pd.read_csv("example_data/***.csv")
    # pre-mark the 'rth' column, remove dt duplicate column that's generated
    print(f"Spy Data Fetch Time : {time() - t_start}s")
    yield Series_DF(df, "NYSE").df.drop(columns="dt")


def _setup_db(db: PsyscaleDB | PsyscaleAsync):
    db.configure_timeseries_schema(minute_tables=MINUTE_CONFIG)
    db.upsert_securities(SYMBOLS, source="unit_test")
    db.update_symbol("spy", {"store_minute": True})


@pytest.fixture(scope="module")
def sync_db():
    "Create a Test Container that will one be used for a single module"
    with PostgresContainer(TIMESCALE_IMAGE, driver=None) as pg_container:
        db = PsyscaleDB(pg_container.get_connection_url())
        _setup_db(db)

        yield db


@pytest.fixture(scope="module")
def async_db_url():
    "Create a Test Container that will one be used for a single module"
    with PostgresContainer(TIMESCALE_IMAGE, driver=None) as pg_container:
        yield pg_container.get_connection_url()


ASYNC_INIT = False


@pytest_asyncio.fixture
async def async_db(async_db_url):
    # Cannot use 'module' scoped fixture with asyncio test for some stupid reason
    global ASYNC_INIT
    if ASYNC_INIT:
        db = PsyscaleAsync(async_db_url)
        yield db
    else:
        ASYNC_INIT = True
        db = PsyscaleAsync(async_db_url)
        _setup_db(db)
        yield db
    await db.close()


def test_sync_insert_speed(sync_db: PsyscaleDB, spy_data):
    spy = sync_db.search_symbols({"symbol": "spy"})[0]
    mdata = sync_db.stored_metadata("spy", _all=True)
    minute_metadata = [m for m in mdata if m.timeframe == pd.Timedelta("1min")][0]

    t_start = time()
    sync_db.upsert_series(spy["pkey"], minute_metadata, spy_data)
    assert "" == f"Sync insert : {time() - t_start:.3f}s"


def test_sync_withdraw_speed(sync_db: PsyscaleDB):
    t_start = time()
    sync_db.get_series("spy", pd.Timedelta("1m"))
    assert "" == f"Sync Withdraw : {time() - t_start:.3f}s"


async def test_async_insert_speed(async_db: PsyscaleAsync, spy_data):
    spy = async_db.search_symbols({"symbol": "spy"})[0]
    mdata = async_db.stored_metadata("spy", _all=True)
    minute_metadata = [m for m in mdata if m.timeframe == pd.Timedelta("1min")][0]

    t_start = time()
    await async_db.upsert_series_async(spy["pkey"], minute_metadata, spy_data)
    assert "" == f"Async insert : {time() - t_start:.3f}s"


async def test_async_withdraw_speed(async_db: PsyscaleAsync):
    t_start = time()
    print(await async_db.get_series_async("spy", pd.Timedelta("1m")))
    assert "" == f"Async Withdraw : {time() - t_start:.3f}s"
