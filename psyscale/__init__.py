"""
A simple Python library for storing & retrieving financial data
in a TimescaleDB optimized Postgres Database.
"""

import asyncio
import logging
import sys
from typing import Literal

from .core import PsyscaleConnectParams
from .metadata_partial import MetadataPartial
from .symbols_partial import AsyncSymbolsPartial, SymbolsPartial
from .timeseries_config_partial import ConfigureTimeseriesPartial
from .series_data_partial import AsyncSeriesDataPartial, SeriesDataPartial


class PsyscaleDB(
    ConfigureTimeseriesPartial, MetadataPartial, SeriesDataPartial, SymbolsPartial
):
    """
    Synchronous client interface for connecting to a PostgreSQL + TimescaleDB Database.

    Timescale DB Docker self-host instructions
    https://docs.timescale.com/self-hosted/latest/install/installation-docker/
    """


class PsyscaleAsync(
    ConfigureTimeseriesPartial,
    MetadataPartial,
    AsyncSeriesDataPartial,
    AsyncSymbolsPartial,
):
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
)


_log = logging.getLogger("psyscale_log")
handler = logging.StreamHandler(None)
formatter = logging.Formatter(
    "[PsyscaleDB] - [.\\%(filename)s Line: %(lineno)d] - %(levelname)s: %(message)s"
)
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
