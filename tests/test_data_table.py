from psycopg import DatabaseError
import pytest
import pandas as pd
from pandas.testing import assert_series_equal, assert_frame_equal

from psyscale.manager import PsyscaleMod
from psyscale.dev import TimeseriesConfig
from psyscale.dev import Schema
from psyscale.psql.timeseries import AGGREGATE_ARGS
from psyscale.series_df import Series_DF

# region ---- ---- Test Fixtures ---- ----


@pytest.fixture
def AAPL_MIN_DATA():
    df = pd.read_csv("example_data/AAPL_1min.csv")
    df.rename({"dt": "date", "open": "o", "high": "max"}, inplace=True)
    yield df


@pytest.fixture
def TICK_DATA():
    yield pd.read_csv("example_data/example_ticks.csv")


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


TICK_CONFIG = TimeseriesConfig(
    ["equity"],  # type:ignore
    rth_origins={
        "equity": pd.Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "equity": pd.Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"equity": True},
    aggregate_periods={"default": [pd.Timedelta("15s"), pd.Timedelta("30s")]},
    inserted_aggregate_periods={"default": [pd.Timedelta(0)]},
)


@pytest.fixture(scope="module")
def psyscale_db(test_url):
    "Module level PsyscaleMod that inits a couple symbols and the needed timeseries config"
    db = PsyscaleMod(test_url)
    db.configure_timeseries_schema(minute_tables=MINUTE_CONFIG, tick_tables=TICK_CONFIG)
    db.upsert_securities(
        pd.DataFrame(
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
        ),
        source="unit_test",
    )
    aapl = db.search_symbols({"symbol": "AAPL"}, strict_symbol_search=True)[0]
    db.update_symbol(aapl["pkey"], {"store_minute": True})
    db.update_symbol("goog", {"store_tick": True})
    yield db


def test_01_metadata_fetch(psyscale_db):
    symbols_to_insert = psyscale_db.search_symbols({"store": True})
    assert len(symbols_to_insert) == 2

    aapl_search = psyscale_db.search_symbols({"store_minute": True})
    assert len(aapl_search) == 1
    aapl = aapl_search[0]
    assert aapl["symbol"] == "AAPL"
    assert "pkey" in aapl
    aapl_pkey = aapl["pkey"]

    # No Data inserted yet, MetaData should be empty
    metadata = psyscale_db.stored_symbol_metadata(aapl_pkey)
    assert len(metadata) == 0

    # Should show that we need to store data in the Minute table of minute schema
    metadata = psyscale_db.get_all_symbol_series_metadata(aapl_pkey)
    assert len(metadata) == 1
    metadata = metadata[0]

    assert metadata.schema_name == Schema.MINUTE_DATA
    assert metadata.start_date == metadata.end_date
    assert metadata.start_date == pd.Timestamp("1800-01-01", tz="UTC")
    assert metadata.timeframe == pd.Timedelta("1min")


# region ---- ---- Aggregate Data Tests ---- ----


def test_02_aggregate_data_insert(psyscale_db, caplog, AAPL_MIN_DATA):
    # pkey and metadata fetch already asserted working in first test.
    aapl = psyscale_db.search_symbols({"store_minute": True})[0]
    metadata = psyscale_db.get_all_symbol_series_metadata(aapl["pkey"])[0]

    with pytest.raises(ValueError):
        # Should error since a 'rth' column is needed but not given.
        psyscale_db.upsert_symbol_data(aapl["pkey"], metadata, AAPL_MIN_DATA, None)

    with pytest.raises(AttributeError):
        # Should error since a column is missing
        psyscale_db.upsert_symbol_data(
            aapl["pkey"], metadata, AAPL_MIN_DATA.drop(columns="date"), None
        )

    # These inter trackers should not yet be populated
    assert not hasattr(psyscale_db, "_altered_tables")
    assert not hasattr(psyscale_db, "_altered_tables_mdata")

    # Should Work, renames columns and all (column renaming tests in series_df tests)
    # And Dropps Extra Columns
    with caplog.at_level("DEBUG"):
        psyscale_db.upsert_symbol_data(aapl["pkey"], metadata, AAPL_MIN_DATA, "NYSE")

    # Assert there was no cursor error in the upsert function.
    assert all(record.levelname != "ERROR" for record in caplog.records)

    # Now they should be populated
    assert hasattr(psyscale_db, "_altered_tables")
    assert hasattr(psyscale_db, "_altered_tables_mdata")


def test_03_check_inserted_data(psyscale_db, caplog):
    aapl = psyscale_db.search_symbols({"store_minute": True})[0]

    # Data is stored, but the metadata table should not reflect this yet.
    # it should only show this once the refresh data has been called.
    metadata = psyscale_db.stored_symbol_metadata(aapl["pkey"])
    assert len(metadata) == 0

    assert hasattr(psyscale_db, "_altered_tables")
    assert hasattr(psyscale_db, "_altered_tables_mdata")

    with caplog.at_level("INFO"):
        psyscale_db.refresh_aggregate_metadata()

    # Assert tracking variables are cleaned up
    assert not hasattr(psyscale_db, "_altered_tables")
    assert not hasattr(psyscale_db, "_altered_tables_mdata")

    # check what's available
    metadata = psyscale_db.stored_symbol_metadata(aapl["pkey"])
    assert len(metadata) == 4  # One for inserted data, 3 more for the aggregates

    minute_metadata = [m for m in metadata if m.timeframe == pd.Timedelta("1min")][0]

    # check that the full dataset got inserted
    raw_data = Series_DF(pd.read_csv("example_data/AAPL_1min.csv"), "NYSE")
    raw_data.df.drop(
        columns=set(raw_data.columns).difference(AGGREGATE_ARGS), inplace=True
    )
    raw_data.df.set_index(keys=pd.RangeIndex(0, 2083), inplace=True)
    raw_data.df["rth"] = raw_data.df["rth"].astype("int64")

    inserted_data = psyscale_db.get_hist(
        aapl["pkey"],
        pd.Timedelta("1min"),
        rth=False,
        rtn_args={"open", "high", "low", "close", "volume", "rth"},
        asset_class=aapl["asset_class"],
        schema=minute_metadata.schema_name,
    )

    for col in raw_data.columns:
        assert_series_equal(raw_data.df[col], inserted_data[col])

    # check that only the 'rth' data cna be retrieved
    raw_data.df = raw_data.df[raw_data.df["rth"] == 0]
    raw_data.df.set_index(keys=pd.RangeIndex(0, 780), inplace=True)

    inserted_data = psyscale_db.get_hist(
        aapl["pkey"],
        pd.Timedelta("1min"),
        rth=True,
        rtn_args={"open", "high", "low", "close", "volume", "rth"},
    )

    for col in raw_data.columns:
        assert_series_equal(raw_data.df[col], inserted_data[col])


def test_04_upsert_on_conflict_states(psyscale_db, AAPL_MIN_DATA, caplog):
    aapl = psyscale_db.search_symbols({"store_minute": True})[0]
    metadata = psyscale_db.get_all_symbol_series_metadata(aapl["pkey"])[0]

    alt_data = Series_DF(AAPL_MIN_DATA, "NYSE").df
    extra_cols = set(alt_data.columns).difference(AGGREGATE_ARGS)
    alt_data = AAPL_MIN_DATA.drop(columns=extra_cols)
    # Make some Adjustments
    alt_data.loc[0, "low"] = -1
    alt_data.loc[0, "high"] = -1
    alt_data.loc[0, "open"] = -1
    alt_data.loc[0, "close"] = -1
    alt_data.loc[0, "volume"] = -1

    with pytest.raises(DatabaseError):
        psyscale_db.upsert_symbol_data(
            aapl["pkey"], metadata, alt_data, "NYSE", on_conflict="error"
        )

    with caplog.at_level("WARNING"):
        psyscale_db.upsert_symbol_data(
            aapl["pkey"], metadata, alt_data, "NYSE", on_conflict="ignore"
        )
    assert any(r.levelname == "WARNING" for r in caplog.records)
    stored_data = psyscale_db.get_hist(
        aapl["pkey"], pd.Timedelta("1min"), rth=False, limit=10
    )

    assert stored_data["dt"].iloc[0] == alt_data["dt"].iloc[0]
    assert stored_data.iloc[0]["low"] != alt_data.iloc[0]["low"]
    assert stored_data.iloc[0]["high"] != alt_data.iloc[0]["high"]
    assert stored_data.iloc[0]["open"] != alt_data.iloc[0]["open"]
    assert stored_data.iloc[0]["close"] != alt_data.iloc[0]["close"]
    assert stored_data.iloc[0]["volume"] != alt_data.iloc[0]["volume"]

    psyscale_db.upsert_symbol_data(
        aapl["pkey"], metadata, alt_data, "NYSE", on_conflict="update"
    )
    stored_data = psyscale_db.get_hist(
        aapl["pkey"], pd.Timedelta("1min"), rth=False, limit=10
    )

    assert stored_data["dt"].iloc[0] == alt_data["dt"].iloc[0]
    assert_series_equal(stored_data.iloc[0], alt_data.iloc[0])

    # Re-insert & clean the original data for use in following tests
    psyscale_db.upsert_symbol_data(
        aapl["pkey"], metadata, AAPL_MIN_DATA.iloc[0:10], "NYSE", on_conflict="update"
    )
    psyscale_db.refresh_aggregate_metadata()


def test_05_stored_aggregates(psyscale_db, caplog):
    stored_data = psyscale_db.get_hist("aapl", pd.Timedelta("30min"), limit=10)
    # fmt: off
    expect_df = pd.DataFrame({
        "dt": [
            "2023-05-24 21:00:00+00:00",
            "2023-05-24 21:30:00+00:00",
            "2023-05-24 22:00:00+00:00",
            "2023-05-24 22:30:00+00:00",
            "2023-05-24 23:00:00+00:00",
            "2023-05-24 23:30:00+00:00",
            "2023-05-25 08:00:00+00:00",
            "2023-05-25 08:30:00+00:00",
            "2023-05-25 09:00:00+00:00",
            "2023-05-25 09:30:00+00:00",
        ],
        "open": [171.68, 171.43, 171.61, 171.38, 171.18, 170.88, 171.37, 171.58, 171.72, 171.53],
        "high": [171.69, 171.73, 171.69, 171.40, 171.18, 171.15, 171.80, 171.71, 171.75, 171.68],
        "low": [171.30, 171.32, 171.22, 171.00, 170.85, 170.84, 171.37, 171.41, 171.52, 171.53],
        "close": [171.42, 171.61, 171.31, 171.04, 170.88, 171.00, 171.58, 171.64, 171.54, 171.65],
        "volume": [39688.0, 42391.0, 71238.0, 50434.0, 34025.0, 37182.0, 27388.0, 9683.0, 12406.0, 5810.0],
    })
    # fmt: on
    expect_df["dt"] = pd.to_datetime(expect_df["dt"])
    assert_frame_equal(stored_data, expect_df)


def test_06_calculated_aggregates(psyscale_db, caplog):
    stored_data = psyscale_db.get_hist("aapl", pd.Timedelta("15min"), limit=10)

    # fmt: off
    expect_df = pd.DataFrame({
        "dt": [
            "2023-05-24 21:15:00+00:00",
            "2023-05-24 21:30:00+00:00",
            "2023-05-24 21:45:00+00:00",
            "2023-05-24 22:00:00+00:00",
            "2023-05-24 22:15:00+00:00",
            "2023-05-24 22:30:00+00:00",
            "2023-05-24 22:45:00+00:00",
            "2023-05-24 23:00:00+00:00",
            "2023-05-24 23:15:00+00:00",
            "2023-05-24 23:30:00+00:00",
        ],
        "open": [171.68, 171.43, 171.61, 171.61, 171.51, 171.38, 171.14, 171.18, 171.01, 170.88],
        "high": [171.69, 171.70, 171.73, 171.69, 171.62, 171.40, 171.28, 171.18, 171.06, 171.15],
        "low": [171.30, 171.32, 171.36, 171.37, 171.22, 171.00, 171.03, 170.90, 170.85, 170.84],
        "close": [171.42, 171.65, 171.61, 171.63, 171.31, 171.20, 171.04, 171.00, 170.88, 171.14],
        "volume": [39688.0, 17973.0, 24418.0, 37748.0, 33490.0, 27235.0, 23199.0, 19566.0, 14459.0, 13893.0],
    })
    # fmt: on
    expect_df["dt"] = pd.to_datetime(expect_df["dt"])

    assert_frame_equal(stored_data, expect_df)

    with caplog.at_level("WARNING"):
        stored_data = psyscale_db.get_hist("aapl", pd.Timedelta("7.5min"), limit=10)
        assert stored_data is None
    # should raise a warning message that the data cannot be derived from the stored data
    assert any(r.levelname == "WARNING" for r in caplog.records)


# endregion

# region ---- ---- Tick Data Tests ---- ----


def test_07_tick_data_insert(psyscale_db, caplog, TICK_DATA):
    # pkey and metadata fetch already asserted working in first test.
    goog = psyscale_db.search_symbols({"symbol": "goog"})[0]
    metadata = psyscale_db.get_all_symbol_series_metadata(goog["pkey"])[0]

    with pytest.raises(ValueError):
        # Should error since a 'rth' column is needed but not given.
        psyscale_db.upsert_symbol_data(goog["pkey"], metadata, TICK_DATA, None)

    with pytest.raises(AttributeError):
        # Should error since a column is missing
        psyscale_db.upsert_symbol_data(
            goog["pkey"], metadata, TICK_DATA.drop(columns="time"), None
        )

    # These inter trackers should not yet be populated
    assert not hasattr(psyscale_db, "_altered_tables")
    assert not hasattr(psyscale_db, "_altered_tables_mdata")

    # Should Work, renames columns and all (column renaming tests in series_df tests)
    # And Dropps Extra Columns
    with caplog.at_level("DEBUG"):
        psyscale_db.upsert_symbol_data(goog["pkey"], metadata, TICK_DATA, "NYSE")

    # Assert there was no cursor error in the upsert function.
    assert all(record.levelname != "ERROR" for record in caplog.records)

    # Now they should be populated
    assert hasattr(psyscale_db, "_altered_tables")
    assert hasattr(psyscale_db, "_altered_tables_mdata")


def test_08_check_inserted_tick_data(psyscale_db):
    goog = psyscale_db.search_symbols({"store_tick": True})[0]

    # Data is stored, but the metadata table should not reflect this yet.
    # it should only show this once the refresh data has been called.
    metadata = psyscale_db.stored_symbol_metadata(goog["pkey"])
    assert len(metadata) == 0

    assert hasattr(psyscale_db, "_altered_tables")
    assert hasattr(psyscale_db, "_altered_tables_mdata")

    psyscale_db.refresh_aggregate_metadata()

    # Assert tracking variables are cleaned up
    assert not hasattr(psyscale_db, "_altered_tables")
    assert not hasattr(psyscale_db, "_altered_tables_mdata")

    # check what's available
    metadata = psyscale_db.stored_symbol_metadata(goog["pkey"])
    assert len(metadata) == 3  # One for inserted data, 3 more for the aggregates

    # check that the full dataset got inserted
    raw_data = Series_DF(pd.read_csv("example_data/example_ticks.csv"), "NYSE")
    raw_data.df.drop(
        columns=set(raw_data.columns).difference(AGGREGATE_ARGS), inplace=True
    )
    raw_data.df.set_index(keys=pd.RangeIndex(0, 2465), inplace=True)
    raw_data.df["rth"] = raw_data.df["rth"].astype("int64")

    inserted_data = psyscale_db.get_hist(
        goog["pkey"],
        pd.Timedelta(0),
        rth=False,
        rtn_args={"volume", "rth", "price"},
    )
    for col in raw_data.columns:
        assert_series_equal(raw_data.df[col], inserted_data[col])


def test_09_stored_tick_aggregates(psyscale_db):
    stored_data = psyscale_db.get_hist("goog", pd.Timedelta("15s"), limit=10)
    # fmt: off
    expected_df = pd.DataFrame({
        "dt": [
            "2023-05-04 15:15:15+00:00",
            "2023-05-04 15:15:30+00:00",
            "2023-05-04 15:15:45+00:00",
            "2023-05-04 15:16:00+00:00",
            "2023-05-04 15:16:15+00:00",
            "2023-05-04 15:16:30+00:00",
            "2023-05-04 15:16:45+00:00",
            "2023-05-04 15:17:00+00:00",
            "2023-05-04 15:17:15+00:00",
            "2023-05-04 15:17:30+00:00",
        ],
        "open":  [162.56, 162.49, 162.44, 162.29, 162.33, 162.33, 162.24, 162.31, 162.21, 162.45],
        "high":  [162.56, 162.52, 162.46, 162.39, 162.39, 162.37, 162.37, 162.31, 162.46, 162.52],
        "low":   [162.46, 162.44, 162.29, 162.27, 162.29, 162.23, 162.24, 162.21, 162.21, 162.38],
        "close": [162.49, 162.44, 162.29, 162.33, 162.33, 162.24, 162.31, 162.21, 162.45, 162.50],
        "volume": [None] * 10,
    })
    expected_df["dt"] = pd.to_datetime(expected_df["dt"])
    # fmt: on
    assert_frame_equal(stored_data, expected_df)


def test_10_calculated_tick_aggregates(psyscale_db):

    with pytest.raises(ValueError):
        # Cannot due intervals that are not multiples of one second
        stored_data = psyscale_db.get_hist("goog", pd.Timedelta("17.5s"), limit=10)

    stored_data = psyscale_db.get_hist("goog", pd.Timedelta("7s"), limit=10)
    # fmt: off
    df = pd.DataFrame({
        "dt": [
            "2023-05-04 15:15:15+00:00",
            "2023-05-04 15:15:22+00:00",
            "2023-05-04 15:15:29+00:00",
            "2023-05-04 15:15:36+00:00",
            "2023-05-04 15:15:43+00:00",
            "2023-05-04 15:15:50+00:00",
            "2023-05-04 15:15:57+00:00",
            "2023-05-04 15:16:04+00:00",
            "2023-05-04 15:16:11+00:00",
            "2023-05-04 15:16:18+00:00",
        ],
        "open":  [162.56, 162.50, 162.51, 162.52, 162.48, 162.43, 162.29, 162.30, 162.30, 162.32],
        "high":  [162.56, 162.55, 162.52, 162.52, 162.48, 162.44, 162.32, 162.39, 162.35, 162.34],
        "low":   [162.48, 162.46, 162.46, 162.46, 162.42, 162.33, 162.27, 162.30, 162.29, 162.30],
        "close": [162.50, 162.51, 162.52, 162.48, 162.43, 162.33, 162.30, 162.30, 162.32, 162.33],
        "volume": [None] * 10,
    })
    df["dt"] = pd.to_datetime(df["dt"])
    # fmt: on
    assert_frame_equal(stored_data, df)
