"""
A simple Python library for storing & retrieving financial data
in a TimescaleDB optimized Postgres Database.
"""

import asyncio
import logging
import sys
from typing import Literal


from .psql import AssetTable, TimeseriesConfig, MetadataInfo
from .core import PsyscaleConnectParams
from .symbols_partial import AsyncSymbolsPartial, SymbolsPartial
from .timeseries_partial import TimerseriesPartial, TimeseriesAsyncPartial


class PsyscaleDB(TimerseriesPartial, SymbolsPartial):
    """
    Synchronous client interface for connecting to a PostgreSQL + TimescaleDB Database.
    Timescale DB Docker self-host instructions
    https://docs.timescale.com/self-hosted/latest/install/installation-docker/
    """


class PsyscaleAsync(TimeseriesAsyncPartial, AsyncSymbolsPartial):
    """
    Asynchronous client interface for connecting to a PostgreSQL + TimescaleDB Database.

    Timescale DB Docker self-host instructions
    https://docs.timescale.com/self-hosted/latest/install/installation-docker/
    """


__all__ = (
    "PsyscaleDB",
    "PsyscaleAsync",
    "PsyscaleConnectParams",
    "set_psyscale_log_level",
    "AssetTable",
    "TimeseriesConfig",
    "MetadataInfo",
)


_log = logging.getLogger("psyscale_log")
handler = logging.StreamHandler(None)
formatter = logging.Formatter("[PsyscaleDB] - [.\\%(filename)s Line: %(lineno)d] - %(levelname)s: %(message)s")
handler.setFormatter(formatter)
_log.addHandler(handler)
_log.setLevel("WARNING")


def set_psyscale_log_level(
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
):
    "Set the logging Level for all TimescaleDB Logs."
    _log.setLevel(level)


if sys.platform == "win32":
    _log.info("Setting Asyncio Loop Policy to be compatible with asyncio + psycopg3")
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
