"Script to Inject Symbols from Broker APIs"

# pylint: disable='missing-function-docstring'
import asyncio
import logging
from typing import Literal

import lightweight_pycharts.broker_apis as lwc_apis  # type:ignore -- while using editable install

import pycharts_timescaledb as tsdb
from pycharts_timescaledb.api_extention import TimescaleDB_EXT

tsdb.set_timescale_db_log_level("INFO")
log = logging.getLogger("pycharts-timescaledb")


async def main():
    on_conflict: Literal["ignore", "update"] = "update"
    db = TimescaleDB_EXT()

    alpaca_api = lwc_apis.AlpacaAPI()
    _import_alpaca(db, alpaca_api, on_conflict)


def _import_alpaca(db: TimescaleDB_EXT, api, on_conflict: Literal["ignore", "update"]):
    """
    Inserts securities from alpaca separating them into asset_classes US_Stock, US_Fund, & Crypto
    Updates are not dynamic, they're forced when a Symbol & Exchange pair are already in the table.
    """
    filtered_assets = api.assets[
        api.assets["tradable"].to_numpy()
        & (api.assets["status"] == "active").to_numpy()
    ].copy()

    extra_cols = set(api.assets.columns) - {
        "sec_type",
        "cusip",  # Standardized Id of the symbol
        "name",
        "ticker",
        "exchange",
        "shortable",
        "marginable",
        "easy_to_borrow",
        "fractionable",
    }
    filtered_assets.drop(columns=extra_cols, inplace=True)
    filtered_assets.rename(columns={"ticker": "symbol"}, inplace=True)
    filtered_assets.reset_index(inplace=True)

    _etfs = filtered_assets["name"].str.contains("ETF", case=False).to_numpy()
    _cryptos = (filtered_assets["sec_type"] == "crypto").to_numpy()
    filtered_assets.drop(columns="sec_type", inplace=True)

    filtered_assets.loc[:, "asset_class"] = None
    filtered_assets.loc[_etfs, "asset_class"] = "us_fund"
    filtered_assets.loc[~_etfs & ~_cryptos, "asset_class"] = "us_stock"
    filtered_assets.loc[_cryptos, "asset_class"] = "crypto"
    filtered_assets.loc[_cryptos, "exchange"] = "alpaca"

    log.info("# of Alpaca Symbols: %s", len(filtered_assets))
    log.info(
        "Filtered Alpaca Asset Classes: %s", {*filtered_assets["asset_class"].to_list()}
    )
    log.info("Filtered Alpaca Exchanges: %s", {*filtered_assets["exchange"].to_list()})
    log.debug("Filtered Alpaca Assets: \n%s", filtered_assets)

    inserted, updated = db.upsert_securities(
        filtered_assets, "Alpaca", on_conflict=on_conflict
    )

    log.info("# Alpaca Symbols Inserted: %s", len(inserted))
    if len(inserted) > 0:
        inserted_rows = filtered_assets.loc[filtered_assets["symbol"].isin(inserted)]
        log.info("Alpaca Symbols Inserted: \n%s", inserted_rows)

    log.info("# Alpaca Symbols Updated: %s", len(updated))
    log.debug("Alpaca Symbols Updated: \n%s", updated)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
