import pytest
import pandas as pd
from psyscale.dev import sql, Schema, AssetTbls

# region ---- ---- Test Fixtures ---- ----


@pytest.fixture
def symbols_df():
    return pd.DataFrame(
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
            {
                "symbol": "SPY",
                "name": "S&P 500",
                "exchange": "ARCA",
                "asset_class": "fund",
                "sector": "Something",
            },
        ]
    )


@pytest.fixture
def attrs_symbols():
    return pd.DataFrame(
        [
            {
                "symbol": "TEST1",
                "name": "Test Symbol One",
                "exchange": "NYSE",
                "asset_class": "equity",
                "sector": "Technology",  # Extra column → goes into `attrs`
                "shortable": True,
            },
            {
                "symbol": "TEST2",
                "name": "Test Symbol Two",
                "exchange": "NASDAQ",
                "asset_class": "equity",
                "sector": "Healthcare",
                "shortable": False,
            },
        ]
    )


def _clean(psyscale_db):
    psyscale_db.execute(
        sql.SQL("TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE;").format(
            schema=sql.Identifier(Schema.SECURITY),
            table=sql.Identifier(AssetTbls.SYMBOLS),
        )
    )


@pytest.fixture
def clean_symbols_table(psyscale_db):
    """Truncates the symbols table before & after each test run"""
    _clean(psyscale_db)
    yield
    _clean(psyscale_db)


# endregion

# region ---- ---- Test Basic Symbol Insert & Search ---- ----


def test_upsert_inserts_symbols(psyscale_db, clean_symbols_table, symbols_df):
    inserted, updated = psyscale_db.upsert_securities(symbols_df, source="test_api")

    assert set(inserted.tolist()) == {"AAPL", "GOOG", "SPY"}
    assert updated.empty


def test_select_distinct_symbols(psyscale_db, clean_symbols_table, symbols_df):
    psyscale_db.upsert_securities(symbols_df, source="test_api")

    equal_sets = lambda x, y: set(x).issubset(y) and set(x).issuperset(y)

    assert equal_sets(psyscale_db.distinct_sources(), ["test_api"])
    assert equal_sets(psyscale_db.distinct_exchanges(), ["NASDAQ", "ARCA"])
    assert equal_sets(psyscale_db.distinct_asset_classes(), ["equity", "fund"])

    combos = psyscale_db.select_distinct_symbols(["asset_class", "exchange"])

    assert ("equity", "NASDAQ") in combos
    assert ("fund", "ARCA") in combos


def test_upsert_ignore_conflict(psyscale_db, clean_symbols_table, symbols_df):
    # Initial insert
    psyscale_db.upsert_securities(symbols_df, source="test_api")

    # Modify names to test if they're ignored
    modified = symbols_df.copy()
    modified.loc[modified.symbol == "AAPL", "name"] = "Fake Apple Inc."

    inserted, updated = psyscale_db.upsert_securities(modified, source="test_api", on_conflict="ignore")

    assert inserted.empty
    assert updated.empty

    # Fetch from db and confirm original value remains
    result = psyscale_db.search_symbols({"symbol": "AAPL", "source": "test_api"}, strict_symbol_search="=")
    assert result[0]["name"] == "Apple Inc."  # Original name retained

    psyscale_db.execute(
        sql.SQL("TRUNCATE TABLE {schema}.{table};").format(schema=Schema.SECURITY, table=AssetTbls.SYMBOLS)
    )


def test_upsert_updates_on_conflict(psyscale_db, clean_symbols_table, symbols_df):
    # Initial insert
    psyscale_db.upsert_securities(symbols_df, source="test_api")

    # Confirm the insert
    result = psyscale_db.search_symbols({"symbol": "GOOG", "source": "test_api"}, strict_symbol_search="=")
    assert result[0]["name"] == "Alphabet Inc."

    # Modify one symbol to simulate an update
    modified = symbols_df.copy()
    modified.loc[modified.symbol == "GOOG", "name"] = "Alphabet Inc. Class A"

    inserted, updated = psyscale_db.upsert_securities(modified, source="test_api", on_conflict="update")

    assert inserted.empty
    assert updated.tolist() == ["AAPL", "GOOG", "SPY"]

    # Confirm the update
    result = psyscale_db.search_symbols({"symbol": "GOOG", "source": "test_api"}, strict_symbol_search="=")
    assert result[0]["name"] == "Alphabet Inc. Class A"

    psyscale_db.execute(
        sql.SQL("TRUNCATE TABLE {schema}.{table};").format(schema=Schema.SECURITY, table=AssetTbls.SYMBOLS)
    )


# endregion

# region ---- ---- Test symbol Atters Insert & Search ---- ----


def test_upsert_with_attrs_and_retrieval(psyscale_db, clean_symbols_table, attrs_symbols):
    inserted, updated = psyscale_db.upsert_securities(attrs_symbols, source="UnitTest")

    # Validate both were inserted
    assert set(inserted.tolist()) == {"TEST1", "TEST2"}
    assert updated.empty

    # Validate full retrieval including attrs
    for symbol in ["TEST1", "TEST2"]:
        result = psyscale_db.search_symbols(
            {
                "symbol": symbol,
                "exchange": attrs_symbols.loc[attrs_symbols["symbol"] == symbol, "exchange"].iloc[0],
            },
            return_attrs=True,
            strict_symbol_search="=",
        )
        assert len(result) == 1
        row = result[0]

        assert row["symbol"] == symbol
        assert "attrs" in row
        assert row["attrs"].get("sector") == attrs_symbols.loc[attrs_symbols["symbol"] == symbol, "sector"].iloc[0]
        assert (
            row["attrs"].get("shortable") == attrs_symbols.loc[attrs_symbols["symbol"] == symbol, "shortable"].iloc[0]
        )


def test_search_by_jsonb_attr(psyscale_db, clean_symbols_table, attrs_symbols):
    # Ensure test data is inserted
    psyscale_db.upsert_securities(attrs_symbols, source="UnitTest")

    # Search for a symbol by JSONB attribute
    results = psyscale_db.search_symbols({"shortable": True}, return_attrs=True, attrs_search=True)

    assert isinstance(results, list)
    assert any(row["symbol"] == "TEST1" for row in results)
    assert all("attrs" in row for row in results)
    assert all(row["attrs"].get("shortable") is True for row in results)


# endregion


def test_fuzzy_trigram_search_orders_results(psyscale_db, clean_symbols_table):
    test_data = pd.DataFrame(
        [
            {
                "symbol": "TST",
                "name": "Test Symbol",
                "exchange": "NYSE",
                "asset_class": "equity",
            },
            {
                "symbol": "TST1",
                "name": "Test Symbol 1",
                "exchange": "NYSE",
                "asset_class": "equity",
            },
            {
                "symbol": "TST2",
                "name": "Testing Simbol",
                "exchange": "NYSE",
                "asset_class": "equity",
            },
            {
                "symbol": "TXS",
                "name": "Tex Symbol",
                "exchange": "NYSE",
                "asset_class": "equity",
            },
        ]
    )

    psyscale_db.upsert_securities(test_data, source="UnitTest")

    # Fuzzy symbol search
    results = psyscale_db.search_symbols({"symbol": "tst"}, strict_symbol_search=False)

    assert len(results) == 4  # All Results are returned
    symbols = [r["symbol"] for r in results]

    # Check that the most similar match ("TST") is first
    assert symbols[0] == "TST"
    # Check that the least similar match ("TXS") is last
    assert symbols[-1] == "TXS"

    # Fuzzy name search
    results = psyscale_db.search_symbols({"name": "symbol"}, strict_symbol_search=False)
    names = [r["name"] for r in results]

    assert len(names) == 4  # All Results are returned
    # Most relevant has desired result & the fewest extra characters
    assert names[0] == "Tex Symbol"
    assert names[1] == "Test Symbol"
    assert names[2] == "Test Symbol 1"
    assert names[3] == "Testing Simbol"  # Least Relevant Should be last.


def test_update_symbol_valid_and_constraint_violation(psyscale_db, symbols_df, clean_symbols_table, caplog):
    # Insert test symbols
    psyscale_db.upsert_securities(symbols_df, source="UnitTest")

    # Retrieve pkey of "AAPL"
    aapl = psyscale_db.search_symbols({"symbol": "AAPL", "source": "UnitTest"}, strict_symbol_search="=")
    assert aapl and "pkey" in aapl[0]
    pkey = aapl[0]["pkey"]

    # Valid update
    result = psyscale_db.update_symbol(pkey, {"store_tick": True})
    assert result is True

    # Ensure update can be retrieved (and pkey search works)
    aapl = psyscale_db.search_symbols({"pkey": pkey}, strict_symbol_search="=", return_attrs=True)[0]
    assert "store_tick" in aapl and aapl["store_tick"]
    assert "store" in aapl and aapl["store"]

    # Check Symbols table constraint shows error
    with caplog.at_level("ERROR"):
        result = psyscale_db.update_symbol(pkey, {"store_minute": True})
        assert result is False
    with caplog.at_level("ERROR"):
        result = psyscale_db.update_symbol(pkey, {"store_aggregate": True})
        assert result is False

    # Optional: Confirm original value was not modified post-exception
    updated = psyscale_db.search_symbols({"pkey": pkey})[0]
    assert updated["store_tick"] is True
    assert updated["store_minute"] is False
    assert updated["store_aggregate"] is False
