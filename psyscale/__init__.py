"""
A simple Python library for storing & retrieving financial data
in a TimescaleDB optimized Postgres Database.
"""

import logging
from typing import Literal
from .client import PsyscaleDB

# Not Importing .manager to preserve lazy loading
# from .manager import PsyscaleMod

__all__ = (
    "PsyscaleDB",
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
