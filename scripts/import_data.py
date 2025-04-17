"Script to Inject Symbols from Broker APIs"

# pylint: disable='missing-function-docstring'
import asyncio
from json import dumps
import logging
from time import time
from typing import Literal

from pandas import DataFrame

# pylint: disable=import-error, logging-fstring-interpolation
import lightweight_pycharts.broker_apis as lwc_apis  # type: ignore
from lightweight_pycharts.orm.types import Symbol, TF  # type: ignore

import pycharts_timescaledb as tsdb
from pycharts_timescaledb.api_extention import TimescaleDB_EXT

tsdb.set_timescale_db_log_level("DEBUG")
log = logging.getLogger("pycharts-timescaledb")

# Used to manually set symbols to download data, Only needs to be called once per symbol
SYMBOLS_TO_IMPORT = [
    # {"symbol": "SPY", "source": "Alpaca", "store": "minute"},
    # Format for importing symbols:
    # {filter_key_1: value_1, ..., filter_key_n:value_n, store: [tick/minute/aggregate]}
]
# Used When Importing the above symbols. See TimescaleDB.Search_Symbols Docstring for info.
STRICT_SYMBOL_SEARCH: bool | Literal["ILIKE", "LIKE", "="] = False


async def main():
    on_conflict: Literal["error", "update"] = "update"
    db = TimescaleDB_EXT()

    _update_stored_symbols(db)

    alpaca_api = lwc_apis.AlpacaAPI()
    _import_alpaca(db, alpaca_api, on_conflict)

    db.refresh_aggregate_metadata()


def _update_stored_symbols(db: TimescaleDB_EXT):
    """
    Update the Database to set list of symbols filtered by the above [SYMBOLS_TO_IMPORT]
    filters to 'store_[]=True' so they get imported.

    Important note, to simplify accessing data, a symbol can only store one type of data
    [tick, minute, or aggregate]. To store multiple types of data for the same symbol,
    the 'source' column of the data must change. i.e. Alpaca_minute & Alpaca_aggregate
    would be required 'sources' to store data from alpaca into both of those data schemas.
    """

    for _filter in SYMBOLS_TO_IMPORT:
        if "store" not in _filter:
            log.info("No Storeage method update given for filter args: %s", _filter)
            continue

        store = _filter.pop("store").lower()
        if store not in {"minute", "tick", "aggregate"}:
            log.info("Cannot update storeage method. Unknown storage Schema: %s", store)
            continue
        store = "store_" + store

        symbols = db.search_symbols(
            _filter, limit=None, strict_symbol_search=STRICT_SYMBOL_SEARCH
        )

        if len(symbols) == 1:
            log.info(
                "Filter Returned a Single Symbol: \n%s", dumps(symbols[0], indent=2)
            )
            rsp = input(f"Set the Symbol to Import {store} Data? y/[N] : ")
            if rsp.lower() == "y":
                db.update_symbol(symbols[0]["pkey"], {store: True})
            continue

        elif len(symbols) == 0:
            log.info(
                "Filter Set below did not return any symbols. \n%s",
                dumps(_filter, indent=2),
            )
            continue

        else:
            log.info(
                "The Following Filter Returned %s Symbols: \nFilter: %s \nSymbols:\n%s",
                len(symbols),
                dumps(_filter, indent=2),
                DataFrame(symbols),
            )
            rsp = input(
                f"Set All Symbols to Import {store} Data or Step through Each? all/skip/[each] : "
            )
            if rsp.lower() == "skip":
                continue
            if rsp.lower() == "all":
                log.info("Updating All Symbols to import.")
                pkeys = [s["pkey"] for s in symbols]
                db.update_symbol(pkeys, {store: True})
                continue

            for symbol in symbols:
                log.info("Symbol : %s", dumps(symbol, indent=2))
                rsp = input(f"Set Symbol to Import '{store}' Data? y/break/[N] : ")
                if rsp.lower() == "break":
                    log.info("Skipping Remainder")
                    break
                elif rsp.lower() == "y":
                    log.info("Updating Symbol")
                    db.update_symbol(symbol["pkey"], {store: True})
                else:
                    log.info("Skipping Symbol")


def _import_alpaca(
    db: TimescaleDB_EXT, alpaca_api, on_conflict: Literal["error", "update"]
):
    "Select all the Stored Symbols from Alpaca and fetch & store their most recent data"

    # Fetch all the Symbols that need to fetch data from Alpaca
    symbols = db.search_symbols(
        {
            "source": "Alpaca",
            "store": True,
        },
        return_attrs=True,
        limit=None,
    )

    for symbol in symbols:
        log.info(
            "Fetching Data for '%s':'%s':'%s'",
            symbol["symbol"],
            symbol["exchange"],
            symbol["source"],
        )

        # Fetch All the Metadata for the Symbol showing what data needs to be fetched
        metadata_list = db.get_all_symbol_series_metadata(symbol["pkey"])
        log.debug("Metadata List: %s", metadata_list)

        for metadata in metadata_list:
            log.info(
                "Fetching Data @ Timeframe: '%s' from '%s' Forward ...",
                str(metadata.timeframe),
                metadata.end_date,
            )

            # Fetch the Data from Alpaca.. At an abysmally slow rate
            t_start = time()
            symbol_obj = Symbol.from_dict(symbol)
            tf_obj = TF.from_timedelta(metadata.timeframe)
            data = alpaca_api.get_hist_unlimited(
                symbol_obj, tf_obj, start=metadata.end_date
            )
            log.debug("Data Fetch Time = %s", time() - t_start)

            # Pass the data off to the database to be inserted
            t_start = time()
            db.upsert_symbol_data(
                symbol["pkey"],
                metadata,
                data,
                symbol["exchange"],
                on_conflict=on_conflict,
            )
            log.debug("Data Insert Time = %s", time() - t_start)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
