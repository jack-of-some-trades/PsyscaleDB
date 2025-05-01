"Tests the Asyncronous method versions for Symbol and Data insertion."

import pandas as pd
from pandas.testing import assert_series_equal

import pytest
import pytest_asyncio

from psyscale import PsyscaleAsync
from psyscale.dev import TimeseriesConfig
from psyscale.psql.enum import Schema
from psyscale.psql.timeseries import AGGREGATE_ARGS
from psyscale.series_df import Series_DF


@pytest.fixture(scope="module")
def AAPL_MIN_DATA():
    df = pd.read_csv("example_data/AAPL_1min.csv")
    df.rename({"dt": "date", "open": "o", "high": "max"}, inplace=True)
    yield df


@pytest.fixture(scope="module")
def symbols():
    yield pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "sector": "Technology",
            },
            {
                "symbol": "GOOG",
                "name": "Alphabet Inc.",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "sector": "Technology",
            },
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
    aggregate_periods={
        "default": [pd.Timedelta("5m"), pd.Timedelta("30m"), pd.Timedelta("1h")]
    },
    inserted_aggregate_periods={"default": [pd.Timedelta("1m")]},
)


def test_00_configure(psyscale_async: PsyscaleAsync):
    psyscale_async.configure_timeseries_schema(minute_tables=MINUTE_CONFIG)


@pytest.mark.asyncio
async def test_01_insert_symbols(psyscale_async: PsyscaleAsync, symbols):
    await psyscale_async.upsert_securities_async(symbols, source="unit_test")

    rsp = await psyscale_async.search_symbols_async({"asset_class": "equity"})
    assert len(rsp) == 2

    rsp = await psyscale_async.search_symbols_async(
        {"symbol": "AAPL"}, strict_symbol_search=True
    )
    aapl = rsp[0]
    assert aapl["pkey"]
    assert aapl["symbol"] == "AAPL"

    # No Async Version of this function
    psyscale_async.update_symbol(aapl["pkey"], {"store_minute": True})


@pytest.mark.asyncio
async def test_01_metadata_fetch(psyscale_async: PsyscaleAsync):
    rsp = await psyscale_async.search_symbols_async({"symbol": "aapl"})
    assert len(rsp) == 1
    aapl = rsp[0]
    assert aapl["symbol"] == "AAPL"
    assert "pkey" in aapl
    aapl_pkey = aapl["pkey"]

    # No Data inserted yet, MetaData should be empty, No Async Metadata function
    metadata = psyscale_async.symbol_metadata(aapl_pkey)
    assert len(metadata) == 0

    # Should show that we need to store data in the Minute table of minute schema
    metadata = psyscale_async.symbol_metadata(aapl_pkey, _all=True)
    assert len(metadata) == 1
    metadata = metadata[0]

    assert metadata.schema_name == Schema.MINUTE_DATA
    assert metadata.start_date == metadata.end_date
    assert metadata.start_date == pd.Timestamp("1800-01-01", tz="UTC")
    assert metadata.timeframe == pd.Timedelta("1min")


@pytest.mark.asyncio
async def test_02_aggregate_data_insert(
    psyscale_async: PsyscaleAsync, caplog, AAPL_MIN_DATA
):
    # pkey and metadata fetch already asserted working in first test.
    aapl = (await psyscale_async.search_symbols_async({"store_minute": True}))[0]
    metadata = psyscale_async.symbol_metadata(aapl["pkey"], _all=True)[0]

    with pytest.raises(ValueError):
        # Should error since a 'rth' column is needed but not given.
        await psyscale_async.upsert_series_async(
            aapl["pkey"], metadata, AAPL_MIN_DATA, None
        )

    with pytest.raises(AttributeError):
        # Should error since a column is missing
        await psyscale_async.upsert_series_async(
            aapl["pkey"], metadata, AAPL_MIN_DATA.drop(columns="date"), None
        )

    # These inter trackers should not yet be populated
    assert not hasattr(psyscale_async, "_altered_tables")
    assert not hasattr(psyscale_async, "_altered_tables_mdata")

    # Should Work, renames columns and all (column renaming tests in series_df tests)
    # And Dropps Extra Columns
    with caplog.at_level("DEBUG"):
        await psyscale_async.upsert_series_async(
            aapl["pkey"], metadata, AAPL_MIN_DATA, "NYSE"
        )

    # Assert there was no cursor error in the upsert function.
    assert all(record.levelname != "ERROR" for record in caplog.records)

    # Now they should be populated
    assert hasattr(psyscale_async, "_altered_tables")
    assert hasattr(psyscale_async, "_altered_tables_mdata")

    with caplog.at_level("INFO"):
        await psyscale_async.refresh_aggregate_metadata_async()

    # Assert tracking variables are cleaned up
    assert not hasattr(psyscale_async, "_altered_tables")
    assert not hasattr(psyscale_async, "_altered_tables_mdata")

    # check what's available
    metadata = psyscale_async.symbol_metadata(aapl["pkey"])
    assert len(metadata) == 4  # One for inserted data, 3 more for the aggregates


@pytest.mark.asyncio
async def test_03_check_inserted_data(psyscale_async: PsyscaleAsync):
    aapl = (await psyscale_async.search_symbols_async({"store_minute": True}))[0]

    # check what's available
    metadata = psyscale_async.symbol_metadata(aapl["pkey"])
    assert len(metadata) == 4  # One for inserted data, 3 more for the aggregates

    minute_metadata = [m for m in metadata if m.timeframe == pd.Timedelta("1min")][0]

    # check that the full dataset got inserted
    raw_data = Series_DF(pd.read_csv("example_data/AAPL_1min.csv"), "NYSE")
    raw_data.df.drop(
        columns=set(raw_data.columns).difference(AGGREGATE_ARGS), inplace=True
    )
    raw_data.df.set_index(keys=pd.RangeIndex(0, 2083), inplace=True)
    raw_data.df["rth"] = raw_data.df["rth"].astype("int64")

    inserted_data = await psyscale_async.get_series_async(
        aapl["pkey"],
        pd.Timedelta("1min"),
        rth=False,
        rtn_args={"open", "high", "low", "close", "volume", "rth"},
        asset_class=aapl["asset_class"],
        schema=minute_metadata.schema_name,
    )
    assert inserted_data is not None

    for col in raw_data.columns:
        assert_series_equal(raw_data.df[col], inserted_data[col])

    # check that only the 'rth' data cna be retrieved
    raw_data.df = raw_data.df[raw_data.df["rth"] == 0]
    raw_data.df.set_index(keys=pd.RangeIndex(0, 780), inplace=True)

    inserted_data = await psyscale_async.get_series_async(
        aapl["pkey"],
        pd.Timedelta("1min"),
        rth=True,
        rtn_args={"open", "high", "low", "close", "volume", "rth"},
    )
    assert inserted_data is not None

    for col in raw_data.columns:
        assert_series_equal(raw_data.df[col], inserted_data[col])
