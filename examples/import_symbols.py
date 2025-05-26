"Script to Inject Symbols from Broker APIs"

# pylint: disable='missing-function-docstring'
import logging
from typing import Literal

import dotenv

from broker_apis.alpaca_api import AlpacaAPI

from psyscale import PsyscaleDB, set_psyscale_log_level

dotenv.load_dotenv(dotenv.find_dotenv())

set_psyscale_log_level("INFO")
log = logging.getLogger("psyscale_log")


def main():
    on_conflict: Literal["ignore", "update"] = "update"
    db = PsyscaleDB()

    _import_alpaca(db, on_conflict)


def _import_alpaca(db: PsyscaleDB, on_conflict: Literal["ignore", "update"]):
    """
    Inserts securities from alpaca separating them into asset_classes US_Stock, US_Fund, & Crypto
    Updates are not dynamic, they're forced when a Symbol & Exchange pair are already in the table.

    Currently this will always insert unique (Symbol, Exchange) pairs. This leads to an edge case bug.
    If either of these parameters update than a new pkey will be made when in practice the value needs
    to be updated. This could be fixed by ensuring a unique ID or CUSID that is located within the
    attrs column for Alpaca Symbols.
    """
    # The AlpacaAPI loads the following environment variables in order to run.
    # ALPACA_API_URL = "https://paper-api.alpaca.markets"
    # ALPACA_API_KEY = ""
    # ALPACA_API_SECRET_KEY = ""
    alpaca_api = AlpacaAPI()

    filtered_assets = alpaca_api.assets[
        alpaca_api.assets["tradable"].to_numpy() & (alpaca_api.assets["status"] == "active").to_numpy()
    ].copy()

    extra_cols = set(alpaca_api.assets.columns) - {
        "class",
        "cusip",  # Standardized SEC Id of the symbol
        "name",
        "symbol",
        "exchange",
        "shortable",
        "marginable",
        "easy_to_borrow",
        "fractionable",
    }
    filtered_assets.drop(columns=extra_cols, inplace=True)
    filtered_assets.reset_index(inplace=True)

    _etfs = filtered_assets["name"].str.contains("ETF", case=False).to_numpy()
    _cryptos = (filtered_assets["class"] == "crypto").to_numpy()
    filtered_assets.drop(columns="class", inplace=True)

    filtered_assets.loc[:, "asset_class"] = None
    filtered_assets.loc[_etfs, "asset_class"] = "us_fund"
    filtered_assets.loc[~_etfs & ~_cryptos, "asset_class"] = "us_stock"
    filtered_assets.loc[_cryptos, "asset_class"] = "crypto"
    filtered_assets.loc[_cryptos, "exchange"] = "alpaca_crypto"

    log.info("# of Alpaca Symbols: %s", len(filtered_assets))
    log.info("Filtered Alpaca Asset Classes: %s", {*filtered_assets["asset_class"].to_list()})
    log.info("Filtered Alpaca Exchanges: %s", {*filtered_assets["exchange"].to_list()})
    log.debug("Filtered Alpaca Assets: \n%s", filtered_assets)

    inserted, updated = db.upsert_securities(filtered_assets, "Alpaca", on_conflict=on_conflict)

    log.info("# Alpaca Symbols Inserted: %s", len(inserted))
    if len(inserted) > 0:
        inserted_rows = filtered_assets.loc[filtered_assets["symbol"].isin(inserted)]
        log.info("Alpaca Symbols Inserted: \n%s", inserted_rows)

    log.info("# Alpaca Symbols Updated: %s", len(updated))
    log.debug("Alpaca Symbols Updated: \n%s", updated)


if __name__ == "__main__":
    main()
