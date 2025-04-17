"""Historical data stream access through alpaca"""

from datetime import datetime
from json import dumps
import logging
from math import floor
import os
from typing import Literal, Optional, Dict, Any
from pandas import DataFrame, Timedelta

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.data.enums import Adjustment
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest

log = logging.getLogger("pycharts-timescaledb")

ALPACA_API_KEYS: Dict[str, Any] = {
    "raw_data": True,
    "api_key": os.getenv("ALPACA_API_KEY"),
    "secret_key": os.getenv("ALPACA_API_SECRET_KEY"),
}


class AlpacaAPI:
    """
    Simple Bridge between Alpaca Api and pandas Objects for historical bars requests.

    Currently painfully slow due to the *3 second* blocking call to sleep() when the api
    limit is reached. Also, no asyncio support yet.
    """

    # _api: alpaca.REST
    # _async_api: alpaca.AsyncRest

    def __init__(self, api_keys: dict[str, Any] = ALPACA_API_KEYS):
        self.api_keys = api_keys

        if (
            self.api_keys.get("api_key", None) is None
            or self.api_keys.get("secret_key", None) is None
        ):
            raise ValueError(
                "ALPACA_API_KEY and/or ALPACA_SECRET_KEY were not loaded as env variables."
            )

        self.stock_client = StockHistoricalDataClient(**self.api_keys)
        self.crypto_client = CryptoHistoricalDataClient(**self.api_keys)

        self.series_ticker_map: Dict[int, str] = {}
        self._assets = None

    @property
    def assets(self) -> DataFrame:
        "Lazy Loaded Asset List of Symbols available on Alpaca."
        if self._assets is not None:
            return self._assets

        # Store Alpaca's Full asset list so it can be searched later without another API Request
        client = TradingClient(**self.api_keys, paper=True)
        # Why does this not have an Async Version? IT TAKES LIKE 3 DAMN SECONDS.
        assets_json = client.get_all_assets()
        self._assets = DataFrame(assets_json).set_index("id")

        # Drop All OTC since they aren't Tradable
        self._assets = self._assets[self._assets.exchange != "OTC"]
        return self._assets

    def get_hist(
        self,
        symbol: str,
        asset_class: Literal["us_equity", "crypto"],
        timeframe: Timedelta,
        start: Optional[str | datetime] = None,
        end: Optional[str | datetime] = None,
        limit: Optional[int] = None,
    ) -> Optional[DataFrame]:
        "Return timeseries data for the given symbol"
        args = {
            "symbol_or_symbols": symbol,
            "timeframe": _timedelta_to_alp(timeframe),
        }
        if start is not None:
            args["start"] = start
        if end is not None:
            args["end"] = end
        if limit is not None:
            args["limit"] = limit

        # This library currently doesn't support async requests on history fetches.
        # To make matters worse, there's is a 3 second sleep timer in the request call
        # that gets invoked when the request limit is reached. That timer probably why
        # this is painfully slow when requesting large sets of data.
        try:
            if asset_class == "crypto":
                rsp: Dict[str, Any] = self.crypto_client.get_crypto_bars(  # type: ignore
                    CryptoBarsRequest(**args)
                )
                return DataFrame(rsp[symbol]) if symbol in rsp else None
            else:
                rsp: Dict[str, Any] = self.stock_client.get_stock_bars(  # type: ignore
                    StockBarsRequest(**args, adjustment=Adjustment.ALL)
                )
                return DataFrame(rsp[symbol]) if symbol in rsp else None
        except APIError as e:
            log.error("get_bars() APIError: %s", e)
            return None


def _timedelta_to_alp(interval: Timedelta) -> TimeFrame:
    "Create a TF Object from a Pandas Timedelta Object"
    if interval < Timedelta("1min"):
        raise ValueError("Alpaca Does not support sub 1-minute intervals")

    errmsg = f"Interval [{interval}] invalid. Series Data must be a simple interval. (An integer multiple of a single period [D|h|m|s])"

    comp = interval.components
    if comp.days > 0:
        if comp.hours + comp.minutes + comp.seconds > 0:
            raise ValueError(errmsg)
        if comp.days < 7:
            return TimeFrame(comp.days, TimeFrameUnit.Day)
        if comp.days < 28:
            log.info("Attempting to Classify Weekly Interval, %s", interval)
            return TimeFrame(floor(comp.days / 7), TimeFrameUnit.Week)
        else:
            log.info("Attempting to Classify Monthly Interval, %s", interval)
            # Denominator is 1 Unix Month / 1 Unix Day
            return TimeFrame(floor(comp.days / (2629743 / 86400)), TimeFrameUnit.Month)
    elif comp.hours > 0:
        if comp.minutes + comp.seconds > 0:
            raise ValueError(errmsg)
        return TimeFrame(comp.hours, TimeFrameUnit.Hour)
    elif comp.minutes > 0:
        if comp.seconds > 0:
            raise ValueError(errmsg)
        return TimeFrame(comp.minutes, TimeFrameUnit.Minute)

    # This should never be reached.
    raise ValueError(f"Failed to Classify Interval {interval}")


# endregion
