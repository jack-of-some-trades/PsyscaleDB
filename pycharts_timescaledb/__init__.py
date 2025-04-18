"""
API to handle Data Communications between Python and a PostgreSQL + Timescale Database
Specifically Designed for use with Lightweight-Pycharts
"""

import logging
from typing import Literal
from .api import TimeScaleDB
from .psql import (
    Operation,
    AssetTbls,
    SeriesTbls,
    AssetTable,
    TimeseriesConfig,
    DEFAULT_AGGREGATES,
)

# Not Importing EXT to preserve lazy loading
# from .api_extention import TimescaleDB_EXT

__all__ = (
    "TimeScaleDB",
    "AssetTable",
    "TimeseriesConfig",
    "DEFAULT_AGGREGATES",
    "set_timescale_db_log_level",
    "Operation",
    "AssetTbls",
    "SeriesTbls",
)


_log = logging.getLogger("pycharts-timescaledb")
handler = logging.StreamHandler(None)
formatter = logging.Formatter(
    "[pycharts-DB] - [.\\%(filename)s Line: %(lineno)d] - %(levelname)s: %(message)s"
)
handler.setFormatter(formatter)
_log.addHandler(handler)
_log.setLevel("WARNING")


def set_timescale_db_log_level(
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
):
    "Set the logging Level for all TimescaleDB Logs."
    _log.setLevel(level)
