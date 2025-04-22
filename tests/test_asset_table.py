from pandas import Timedelta, Timestamp
from psyscale.dev import AssetTable

# region ---- ---- Asset Table ---- ----


def test_asset_table_table_name_variants():
    at = AssetTable("stock", Timedelta("5min"), raw=True, ext=True, rth=None)
    assert at.table_name == "stock_300_raw_ext"

    at_rth = AssetTable("crypto", Timedelta("5min"), raw=True, ext=True, rth=True)
    assert at_rth.table_name == "crypto_300_raw_rth"

    at_eth = AssetTable("stock", Timedelta("5min"), raw=True, ext=True, rth=False)
    assert at_eth.table_name == "stock_300_raw_eth"

    at_nonext = AssetTable("etf", Timedelta("5min"), raw=True, ext=False, rth=None)
    assert at_nonext.table_name == "etf_300_raw"

    at_agg = AssetTable("stock", Timedelta("5min"), raw=False, ext=False, rth=None)
    assert at_agg.table_name == "stock_300"


def test_asset_table_equality_repr_hash():
    at1 = AssetTable("crypto", Timedelta("1h"), raw=False, ext=True, rth=None)
    at2 = AssetTable("crypto", Timedelta("1h"), raw=False, ext=True, rth=None)
    assert at1 == at2
    assert hash(at1) == hash(at2)

    at = AssetTable("equity", Timedelta("1d"), raw=False, ext=False, rth=None)
    assert repr(at) == "equity_86400"


def test_asset_table_properties():
    at = AssetTable("etf", Timedelta("3d"), raw=False, ext=True, rth=None)
    assert isinstance(at.origin_ts, Timestamp)
    assert isinstance(at.origin_ltf, str)
    assert isinstance(at.origin_htf, str)
    assert at.psql_interval == "259200 seconds"

    # Has RTH Cases
    assert at.has_rth is True
    assert AssetTable("etf", Timedelta("3d"), raw=True, ext=True, rth=False).has_rth
    assert not AssetTable("etf", Timedelta("3d"), raw=False, ext=True, rth=True).has_rth
    assert not AssetTable(
        "etf", Timedelta("3d"), raw=False, ext=False, rth=None
    ).has_rth


def test_asset_table_from_table_name():
    table_name = "futures_3600_raw_ext"
    at = AssetTable.from_table_name(table_name)
    assert at.asset_class == "futures"
    assert at.period == Timedelta(seconds=3600)
    assert at.raw is True
    assert at.ext is True
    assert at.rth is None


def test_asset_table_origin_fallback():
    at_ltf = AssetTable("test", Timedelta("3d"), raw=False, ext=False, rth=None)
    assert at_ltf.origin == str(at_ltf.origin_ts)

    at_htf = AssetTable("test", Timedelta("6W"), raw=False, ext=False, rth=None)
    assert at_htf.origin == str(at_htf.origin_ts)


# endregion
