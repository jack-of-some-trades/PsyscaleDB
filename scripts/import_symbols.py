"Script to Inject Symbols from Broker APIs"

# pylint: disable='missing-function-docstring'
import asyncio
import logging
import time
from typing import Literal

import lightweight_pycharts.broker_apis as lwc_apis  # type:ignore -- while using editable install

import pycharts_timescaledb as tsdb
from pycharts_timescaledb.api_extention import TimescaleDB_EXT

tsdb.set_timescale_db_log_level("INFO")
log = logging.getLogger("pycharts-timescaledb")


async def main():
    on_conflict: Literal["ignore", "update"] = "ignore"
    db = TimescaleDB_EXT()

    alpaca_api = lwc_apis.AlpacaAPI()
    if alpaca_api is not None:
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

    exec = lambda: db.upsert_securities(
        filtered_assets, "Alpaca", on_conflict=on_conflict
    )
    exec_many = lambda: db.upsert_securities_exec_many(
        filtered_assets, "Alpaca", on_conflict=on_conflict
    )
    copy = lambda: db.upsert_securities_copy_table(
        filtered_assets, "Alpaca", on_conflict=on_conflict
    )
    drop = lambda: db.drop_all_symbols()
    drop()
    # log.info(f"Exec Time = {timeit(exec, drop, number=25) / 25} ")
    # log.info(f"Exec MAny Time = {timeit(exec_many, drop, number=25) / 25} ")

    log.info("Insert No Conflict Check - Pipeline")
    log.info(f"Exec Time = {_timeit(exec, drop, 250)} ")
    log.info(f"Exec_Many Time = {_timeit(exec_many, drop, 250)} ")
    # log.info(f"Copy Buffer Time = {_timeit(copy, drop, 250)} ")

    exec = lambda: db.upsert_securities(filtered_assets, "Alpaca", on_conflict="update")
    exec_many = lambda: db.upsert_securities_exec_many(
        filtered_assets, "Alpaca", on_conflict="update"
    )
    copy = lambda: db.upsert_securities_copy_table(
        filtered_assets, "Alpaca", on_conflict="update"
    )

    log.info("Insert With Conflict Check - Pipeline")
    log.info(f"Exec Time = {_timeit(exec, drop, 250)} ")
    log.info(f"Exec_Many Time = {_timeit(exec_many, drop, 250)} ")
    # log.info(f"Copy Buffer Time = {_timeit(copy, drop, 250)} ")

    log.info("Update With Conflict Check - Pipeline")
    log.info(f"Exec Time = {_timeit(exec, lambda: None, 250)} ")
    log.info(f"Exec_Many Time = {_timeit(exec_many, lambda: None, 250)} ")
    # log.info(f"Copy Buffer Time = {_timeit(copy, lambda: None, 250)} ")

    # All Times are Averages over 250 Calls
    # INFO: Insert No Conflict Check
    # INFO: Exec Time = 0.4525
    # INFO: Exec_Many Time = 0.5232
    # INFO: Copy Buffer Time = 0.4148

    # INFO: Insert With Conflict Check
    # INFO: Exec Time = 0.4774
    # INFO: Exec_Many Time = 0.5324
    # INFO: Copy Buffer Time = 0.4443

    # INFO: Update With Conflict Check
    # INFO: Exec Time = 0.4280
    # INFO: Exec_Many Time = 0.5491
    # INFO: Copy Buffer Time = 0.3632

    # INFO: Insert No Conflict Check - Pipeline
    # INFO: Exec = 0.5699729681015014
    # INFO: Exec_Many = 0.6573

    # INFO: Insert With Conflict Check - Pipeline
    # INFO: Exec Time With Conflict Check - Pipeline = 0.5957747182846069
    # INFO: Exec_Many = 0.6704

    # INFO: Update With Conflict Check - Pipeline
    # INFO: Exec = 0.5186
    # INFO: Exec_Many = 0.6646

    # inserted, updated = db.upsert_securities_copy_table(
    #     filtered_assets, "Alpaca", on_conflict=on_conflict
    # )

    # log.info("# Alpaca Symbols Inserted: %s", len(inserted))
    # if len(inserted) > 0:
    #     log.info("Alpaca Symbols Inserted: \n%s", inserted)

    # log.info("# Alpaca Symbols Updated: %s", len(updated))
    # log.debug("Alpaca Symbols Updated: \n%s", updated)


def _timeit(statement, prep, n):
    tot_time = 0
    for _ in range(n):
        prep()
        t_start = time.time()
        statement()
        tot_time += time.time() - t_start

    return tot_time / n


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
