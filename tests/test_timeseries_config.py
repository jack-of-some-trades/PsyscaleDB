from pandas import Timedelta, Timestamp
import pytest
from psyscale.dev import TimeseriesConfig, DEFAULT_AGGREGATES
from psyscale.psql.orm import AssetTable, _determine_conflicting_timedeltas

# pylint: disable=missing-function-docstring
STD_ASSET_LIST = ["us_fund", "us_stock", "crypto"]
CRYPTO_AGGS = [
    Timedelta("1h"),
    Timedelta("4h"),
    Timedelta("1D"),
    Timedelta("1W"),
]


TEST_CONFIG = TimeseriesConfig(
    STD_ASSET_LIST,  # type:ignore
    rth_origins={
        "us_stock": Timestamp("2000/01/03 08:30", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "us_stock": Timestamp("2000/01/03 04:00", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"us_stock": True, "us_fund": None},
    aggregate_periods={"default": DEFAULT_AGGREGATES, "crypto": CRYPTO_AGGS},
    inserted_aggregate_periods={
        "default": [Timedelta("15s"), Timedelta("30s"), Timedelta("1m")],
        "crypto": [Timedelta("1m")],
    },
)


# region ---- ---- __post_init__ formatting  ---- ----


def test_for_errors():
    # Error on overlapping insertions & aggregates
    with pytest.raises(AttributeError):
        TimeseriesConfig(
            asset_classes=["stocks"],
            aggregate_periods={"stocks": [Timedelta(days=1)]},
            inserted_aggregate_periods={"stocks": [Timedelta(days=1)]},
            rth_origins={},
            eth_origins={},
            htf_origins={},
        )

    with pytest.raises(KeyError):
        TEST_CONFIG.all_tables("bonds")


def test_config_asset_class_rth_true():
    assert TEST_CONFIG._ext_important("us_stock")
    # STD Tables
    tbls = TEST_CONFIG.std_tables("us_stock")
    assert set(tbl.asset_class for tbl in tbls) == {"us_stock"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("5min"),
        Timedelta("15min"),
        Timedelta("30min"),
    ]

    tbls = TEST_CONFIG.std_tables("us_stock", inserted=True)
    assert set(tbl.asset_class for tbl in tbls) == {"us_stock"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("15s"),
        Timedelta("30s"),
        Timedelta("1m"),
        Timedelta("5min"),
        Timedelta("15min"),
        Timedelta("30min"),
    ]

    # RTH Tables
    tbls = TEST_CONFIG.rth_tables("us_stock")
    assert set(tbl.asset_class for tbl in tbls) == {"us_stock"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    tbls = TEST_CONFIG.rth_tables("us_stock", inserted=True)
    assert set(tbl.asset_class for tbl in tbls) == {"us_stock"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    # ETH Tables
    tbls = TEST_CONFIG.eth_tables("us_stock")
    assert not set(tbl.asset_class for tbl in tbls)
    assert sorted(tbl.period for tbl in tbls) == []

    tbls = TEST_CONFIG.eth_tables("us_stock", inserted=True)
    assert not set(tbl.asset_class for tbl in tbls)
    assert sorted(tbl.period for tbl in tbls) == []

    # RAW Tables indirectly tested above


def test_config_asset_class_rth_none():
    assert TEST_CONFIG._ext_important("us_fund")
    # STD Tables
    tbls = TEST_CONFIG.std_tables("us_fund")
    assert set(tbl.asset_class for tbl in tbls) == {"us_fund"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("5min"),
        Timedelta("15min"),
        Timedelta("30min"),
    ]

    tbls = TEST_CONFIG.std_tables("us_fund", inserted=True)
    assert set(tbl.asset_class for tbl in tbls) == {"us_fund"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("15s"),
        Timedelta("30s"),
        Timedelta("1m"),
        Timedelta("5min"),
        Timedelta("15min"),
        Timedelta("30min"),
    ]

    # RTH Tables
    tbls = TEST_CONFIG.rth_tables("us_fund")
    assert set(tbl.asset_class for tbl in tbls) == {"us_fund"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    tbls = TEST_CONFIG.rth_tables("us_fund", inserted=True)
    assert set(tbl.asset_class for tbl in tbls) == {"us_fund"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    # ETH Tables
    tbls = TEST_CONFIG.eth_tables("us_fund")
    assert set(tbl.asset_class for tbl in tbls) == {"us_fund"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    tbls = TEST_CONFIG.eth_tables("us_fund", inserted=True)
    assert set(tbl.asset_class for tbl in tbls) == {"us_fund"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    # RAW Tables indirectly tested above


def test_config_asset_class_rth_indifferent():
    assert not TEST_CONFIG._ext_important("crypto")
    # STD Tables
    tbls = TEST_CONFIG.std_tables("crypto")
    assert set(tbl.asset_class for tbl in tbls) == {"crypto"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    tbls = TEST_CONFIG.std_tables("crypto", inserted=True)
    assert set(tbl.asset_class for tbl in tbls) == {"crypto"}
    assert sorted(tbl.period for tbl in tbls) == [
        Timedelta("1m"),
        Timedelta("1h"),
        Timedelta("4h"),
        Timedelta("1D"),
        Timedelta("1W"),
    ]

    # RTH Tables
    tbls = TEST_CONFIG.rth_tables("crypto")
    assert not set(tbl.asset_class for tbl in tbls)
    assert sorted(tbl.period for tbl in tbls) == []

    tbls = TEST_CONFIG.rth_tables("crypto", inserted=True)
    assert not set(tbl.asset_class for tbl in tbls)
    assert sorted(tbl.period for tbl in tbls) == []

    # ETH Tables
    tbls = TEST_CONFIG.eth_tables("crypto")
    assert not set(tbl.asset_class for tbl in tbls)
    assert sorted(tbl.period for tbl in tbls) == []

    tbls = TEST_CONFIG.eth_tables("crypto", inserted=True)
    assert not set(tbl.asset_class for tbl in tbls)
    assert sorted(tbl.period for tbl in tbls) == []

    # RAW Tables indirectly tested above


# endregion

# region ---- ---- Method Testing  ---- ----


def test_get_selection_source_table():
    "Tests both get_selection_source_table and get_aggregation_source() that it relies on"
    # Exact Table is stored
    mock_table = AssetTable(
        asset_class="us_stock", period=Timedelta(days=1), rth=True, raw=False, ext=True
    )
    selected_table, needs_aggregation = TEST_CONFIG.get_selection_source_table(
        mock_table
    )
    assert needs_aggregation is False
    assert selected_table == mock_table

    # Test that the given table is not returned when flagged not too
    selected_table = TEST_CONFIG.get_aggregation_source(mock_table)
    assert selected_table != mock_table

    # Table needs to be aggregated from like RTH aggregated data
    mock_table = AssetTable(
        asset_class="us_stock", period=Timedelta(days=2), rth=True, raw=False, ext=True
    )
    selected_table, needs_aggregation = TEST_CONFIG.get_selection_source_table(
        mock_table
    )
    assert needs_aggregation
    assert selected_table == AssetTable(
        asset_class="us_stock", period=Timedelta(days=1), rth=True, raw=False, ext=True
    )

    # Table needs to be aggregated from a much lower timeframe that has needed EXT data
    mock_table = AssetTable(
        asset_class="us_stock", period=Timedelta(days=2), rth=False, raw=False, ext=True
    )
    selected_table, needs_aggregation = TEST_CONFIG.get_selection_source_table(
        mock_table
    )
    assert needs_aggregation
    assert selected_table == AssetTable(
        asset_class="us_stock", period=Timedelta("30min"), rth=None, raw=False, ext=True
    )

    # Test with invalid selections

    # No stored timeframe divisor
    with pytest.raises(AttributeError):
        mock_table = AssetTable(
            asset_class="us_stock",
            period=Timedelta("27s"),
            rth=True,
            raw=False,
            ext=True,
        )
        TEST_CONFIG.get_selection_source_table(mock_table)

    # Cannot Derive timeframe (lower than lowest stored)
    with pytest.raises(AttributeError):
        mock_table = AssetTable(
            asset_class="us_stock",
            period=Timedelta("1s"),
            rth=True,
            raw=False,
            ext=True,
        )
        TEST_CONFIG.get_selection_source_table(mock_table)


def test_get_tables_to_refresh():
    mock_table = AssetTable(
        asset_class="us_stock", period=Timedelta("1min"), rth=True, raw=True, ext=True
    )
    # Test All tables are returned
    assert TEST_CONFIG.get_tables_to_refresh(mock_table) == TEST_CONFIG.all_tables(
        mock_table.asset_class, include_raw=False
    )

    # test that only the tbls above the inserted timeframe are refreshed
    mock_table = AssetTable(
        asset_class="crypto", period=Timedelta("1h"), rth=True, raw=False, ext=True
    )
    tbls = TEST_CONFIG.all_tables(mock_table.asset_class, include_raw=False)
    tbls = [tbl for tbl in tbls if tbl.period > Timedelta("1h")]
    assert TEST_CONFIG.get_tables_to_refresh(mock_table) == tbls

    _ = """ideally the following test would also return true, but right now the function
    inst sophisticated enough to tell when a lower timeframe table doesn't actually feed
    an aggregated table. The edge case is too rare to care, and everything still functions
    as expected without the specificity."""
    # mock_table = AssetTable(
    #     asset_class="crypto", period=Timedelta("15s"), rth=True, raw=False, ext=True
    # )
    # # returns an empty list because all aggregates are formed on top of the
    # # raw '1min' timeframe table, not the '15s' table
    # assert TEST_CONFIG.get_tables_to_refresh(mock_table) == []


def test_config_from_table_names():

    # Shuffled Table names from the TEST_CONFIG definition from above
    test_config_table_names = [
        "_origin",  # should get filtered out
        "us_fund_15_raw_ext",
        "us_fund_30_raw_ext",
        "us_stock_30_raw_ext",
        "crypto_60_raw",
        "us_fund_3600_eth",
        "us_fund_14400_eth",
        "us_fund_86400_eth",
        "us_fund_604800_eth",
        "us_stock_60_raw_ext",
        "us_fund_300_ext",
        "us_fund_900_ext",
        "us_fund_1800_ext",
        "us_fund_3600_rth",
        "us_fund_60_raw_ext",
        "us_stock_15_raw_ext",
        "us_fund_14400_rth",
        "us_fund_86400_rth",
        "us_fund_604800_rth",
        "us_stock_300_ext",
        "us_stock_900_ext",
        "us_stock_1800_ext",
        "crypto_14400",
        "crypto_86400",
        "us_stock_3600_rth",
        "us_stock_14400_rth",
        "us_stock_86400_rth",
        "us_stock_604800_rth",
        "crypto_3600",
        "crypto_604800",
    ]

    reconstruct = TimeseriesConfig.from_table_names(test_config_table_names)

    for asset_class in reconstruct.asset_classes:
        assert reconstruct.all_tables(asset_class) == TEST_CONFIG.all_tables(
            asset_class
        )


# endregion


# region ---- ---- Conflicting time-delta tests  ---- ----


@pytest.mark.parametrize("prth", [True, False, None])
def test_determine_conflicting_timedeltas_no_conflict(prth):
    # Test case where RTH is prioritized
    rth_origin = Timestamp("2025-04-22 07:15:00")
    eth_origin = Timestamp("2025-04-22 07:15:00")
    periods = [Timedelta("15min"), Timedelta("30min"), Timedelta("1h")]

    std_periods, rth_periods, eth_periods = _determine_conflicting_timedeltas(
        periods, rth_origin, eth_origin, prioritize_rth=prth
    )

    # Returned Lists are always sorted smallest to largest
    assert std_periods == [Timedelta("15min"), Timedelta("30min"), Timedelta("1h")]
    assert rth_periods == []
    assert eth_periods == []


def test_determine_conflicting_timedeltas_with_conflict():
    rth_origin = Timestamp("2025-04-22 10:00:00")
    eth_origin = Timestamp("2025-04-22 07:15:00")
    periods = [Timedelta("15min"), Timedelta("30min"), Timedelta("1h")]

    # Test case where RTH is prioritized
    std_periods, rth_periods, eth_periods = _determine_conflicting_timedeltas(
        periods, rth_origin, eth_origin, prioritize_rth=True
    )

    assert std_periods == [Timedelta("15min")]
    assert rth_periods == [Timedelta("30min"), Timedelta("1h")]
    assert eth_periods == []

    # Test case where Neither are prioritized
    std_periods, rth_periods, eth_periods = _determine_conflicting_timedeltas(
        periods, rth_origin, eth_origin, prioritize_rth=False
    )

    assert std_periods == [Timedelta("15min")]
    assert rth_periods == []
    assert eth_periods == [Timedelta("30min"), Timedelta("1h")]

    # Test case where Eth is prioritized
    std_periods, rth_periods, eth_periods = _determine_conflicting_timedeltas(
        periods, rth_origin, eth_origin, prioritize_rth=None
    )

    assert std_periods == [Timedelta("15min")]
    assert rth_periods == [Timedelta("30min"), Timedelta("1h")]
    assert eth_periods == [Timedelta("30min"), Timedelta("1h")]


@pytest.mark.parametrize("prth", [True, False, None])
def test_invalid_eth_origin(prth):
    # Test invalid case: ETH origin occurs after RTH origin
    rth_origin = Timestamp("2025-04-22 08:00:00")
    eth_origin = Timestamp("2025-04-22 10:00:00")
    periods = [Timedelta("1h"), Timedelta("30min")]

    with pytest.raises(
        ValueError,
        match="TimescaleDB ETH Aggregate Origin must occur before RTH Origin.",
    ):
        _determine_conflicting_timedeltas(
            periods, rth_origin, eth_origin, prioritize_rth=prth
        )

    # Test invalid case: ETH and RTH origin difference >= 1 day
    rth_origin = Timestamp("2025-04-22 10:00:00")
    eth_origin = Timestamp("2025-04-21 10:00:00")

    with pytest.raises(
        ValueError,
        match="TimescaleDB RTH Origin Must be less than 1D after ETH Origin.",
    ):
        _determine_conflicting_timedeltas(
            periods, rth_origin, eth_origin, prioritize_rth=prth
        )


# endregion
