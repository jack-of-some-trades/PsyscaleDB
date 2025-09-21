"Metadata Partial Class Functions"

import logging
from typing import Any

from pandas import Timedelta, Timestamp
from psyscale.core import TupleCursor
from psyscale.psql.orm import HTF_CROSSOVER
from psyscale.timeseries_partial import TimeseriesPartialAbstract

from .psql import (
    METADATA_ARGS,
    MetadataArgs,
    MetadataInfo,
    Operation as Op,
    AssetTbls,
    SeriesTbls,
    GenericTbls,
    Schema,
    AssetTable,
)

log = logging.getLogger("psyscale_log")

# pylint: disable='protected-access','abstract-method'


class MetadataPartial(TimeseriesPartialAbstract):
    """
    Series Metadata related sub-routines. Relevant Table initialization handled by
    SymbolsPartial._ensure_securities_schema_format() to ensure symbols table is
    created first.
    """

    def inferred_metadata(self, symbol: int | str, timeframe: Timedelta, rth: bool = False) -> MetadataInfo | None:
        """
        Return the Metadata for a symbol at a given timeframe. This function will use the stored
        metadata to infer what series data can be derived from the stored series data in the event
        the desired timeframe / rth state is not directly stored.

        Large Timeframes, (Months, Quarters, Years), Can be Passed by passing timedeltas that are close to
        their unix equivalent values. (~30.4 Days / Month, ~365.25 Days / Years)
        """
        timeframe = _round_large_timeframe(timeframe)

        with self._cursor(dict_cursor=True) as cursor:
            # region ---- Fetch asset_class and stored state
            if isinstance(symbol, str):
                _filters = [("symbol", "ILIKE", symbol)]
            else:
                _filters = [("pkey", "=", symbol)]
            cursor.execute(
                self[Op.SELECT, GenericTbls.TABLE](
                    Schema.SECURITY,
                    AssetTbls.SYMBOLS,
                    ["pkey", "asset_class", "source"],
                    _filters,
                )
            )
            rsp = cursor.fetchall()
            if len(rsp) == 0:
                log.warning("Unknown key : %s", symbol)
                return None
            if len(rsp) > 1:
                log.info("ill defined key, using first result : %s", rsp[0])
            rsp = rsp[0]

            pkey = rsp["pkey"]
            asset_class = rsp["asset_class"]
            assert asset_class

            cursor.execute(self[Op.SELECT, AssetTbls._METADATA](("pkey", "=", pkey)))
            stored_mdata = [MetadataInfo(**row) for row in cursor.fetchall()]

            if len(stored_mdata) == 0:
                log.warning("Key '%s' is known, but not stored.", symbol)
                return None
        # endregion

        _config = self._table_config[Schema(stored_mdata[0].schema_name)]
        desired_table = AssetTable(asset_class, timeframe, False, False, rth)

        # This is the table that will either be selected, or aggregated, from
        # to produce the desired series
        src_table = _config.get_aggregation_source(desired_table, rtn_self=True)
        # Return the metadata associated with the src_table
        return next(mdata for mdata in stored_mdata if mdata.table == src_table)

    def stored_metadata(
        self,
        symbol: int | str,
        filters: dict[MetadataArgs | str, Any] = {},
        *,
        _all: bool = False,
    ) -> list[MetadataInfo]:
        """
        Return Metadata about the series data stored for a given symbol, by ticker or primary key.
        Be default, only returns information about what has been stored & aggregated. Metadata for
        tables that are set to be stored, but are not yet are only returned when _all=True.

        --PARAMS--
        - pkey : integer
            - Primary Key of the desired Symbol. Value is returned as a value in the search_symbols
            return object.
        - filters : Dict[MetadataArgs, Any]
            - Filtering Arguments for the returned MetaData. Extra Keys that don't map to columns
            of the table are ignored. Optional Arguments are as follows.
            - table_name : str
            - schema_name : str
            - is_raw_data : boolean
            - Timeframe : int (Number of seconds elapsed in the period)
            - trading_hours_type : Literal['ext', 'rth', 'eth', 'none']
        - all : boolean
            - When True, Will ignore the given filters and return metadata for what is stored *and*
            what should be stored
            - When False, Will only return metadata for series tables that have data for this
            symbol stored in them
        """

        pkey = self._get_pkey(symbol)
        if pkey is None:
            log.info("Cannot get Metadata, Unknown Symbol: %s", symbol)
            return []

        if _all:
            return _fetch_all_metadata(self, pkey)
        else:
            return _fetch_stored_metadata(self, pkey, filters)

    def manually_refresh_aggregate_metadata(self):
        """
        CLI Script to Manually Refresh Continuous Aggregates as needed.
        Input Options will be presented to completely or partially automate this process
        """
        print(
            """
            Attempting to refresh *all* Continuous Aggregates over *All time*
            This can be an extremely slow process.
              
            The following are options on how to proceed :
              - 'all' - Do everything
              - 'abort' - Do Nothing
              - 'none' - Only refresh the security._metadata view
              - 'schema' - Ask to refresh per schema
              - 'asset' - Ask to refresh per asset_class
              - 'table' - ask to refresh per individual table

            When choosing 'asset' or 'table' the higher level filters will also be available.
        """
        )

        for _ in range(3):
            method = input("'all' / 'none' / 'schema' / 'asset' / 'table' / 'abort' : ")
            if method == "abort":
                return
            if method.lower() in {"all", "none", "schema", "asset", "table"}:
                break
            print("Unknown input")

        if method.lower() not in {"all", "none", "schema", "asset", "table"}:
            print("Learn to type.")
            return

        with self._cursor(auto_commit=True) as cursor:
            if method != "none":
                try:
                    self._manual_refresh_loop(cursor, method)
                except AssertionError:
                    pass

            log.info("---- ---- Refreshing 'Security._Metadata' Materialized View ---- ----")
            cursor.execute(self[Op.REFRESH, AssetTbls._METADATA]())

    def _manual_refresh_loop(self, cursor: TupleCursor, method: str):
        "Inner function that can return but still allow the cursor to refresh the MetaData Table."
        for schema, config in self._table_config.items():
            if method in {"schema", "table", "asset"}:
                rsp = input(f"Refresh schema {schema}? : y/abort/[N] : ")
                if rsp.lower() == "abort":
                    assert False
                if rsp.lower() != "y":
                    continue

            log.info("---- ---- Refreshing Schema : %s ---- ---- ", schema)

            all_aggregates = []

            for asset_class in config.asset_classes:
                if method in {"table", "asset"}:
                    rsp = input(f"Refresh asset_class {asset_class}? : y/abort/[N] : ")
                    if rsp.lower() == "abort":
                        assert False
                    if rsp.lower() != "y":
                        continue

                aggs = config.all_tables(asset_class, include_raw=False)
                aggs.sort(key=lambda x: x.period)
                all_aggregates.extend(aggs)

            for table in all_aggregates:
                if method == "table":
                    rsp = input(f"Refresh table {table}? : y/abort/[N] : ")
                    if rsp.lower() == "abort":
                        assert False
                    if rsp.lower() != "y":
                        continue

                log.info("Refreshing Continuous Aggregate : %s ", table)
                cursor.execute(self[Op.REFRESH, SeriesTbls.CONTINUOUS_AGG](schema, table))


def _fetch_stored_metadata(db: MetadataPartial, pkey: int, filters: dict[MetadataArgs | str, Any] = {}):
    _filters = [("pkey", "=", pkey)]  # Ensure a Pkey filter is Passed
    _filters.extend([(k, "=", v) for k, v in filters.items() if k in (METADATA_ARGS - {"pkey"})])

    rsp, _ = db.execute(db[Op.SELECT, AssetTbls._METADATA](_filters), dict_cursor=True)
    return [MetadataInfo(**row) for row in rsp]


def _fetch_all_metadata(db: MetadataPartial, pkey: int) -> list[MetadataInfo]:
    """
    Fetch MetadataInfo for what is stored *and* what should be stored fpr a given symbol pkey.

    i.e. When A Symbol was just flagged to be stored but has nothing stored yet
    symbol_series_metadata will not return any Metadata since it only checks what *is* stored,
    not what should be. This function will return what is stored + what should be stored.

    If no information is stored for a given table, but should be, the start_date & end_date will
    be set to "1800-01-01" So when a request is made it should automatically fetch all recorded
    data for the symbol.
    """

    _rtn_args = ["asset_class", "store_tick", "store_minute", "store_aggregate"]
    _filter = ("pkey", "=", pkey)

    rsp, _ = db.execute(
        db[Op.SELECT, GenericTbls.TABLE](Schema.SECURITY, AssetTbls.SYMBOLS, _rtn_args, _filter),
        dict_cursor=True,
    )
    if len(rsp) == 0:
        raise ValueError(f"Cannot determine Symbol updates needed. {pkey = } is unknown.")
    rsp = rsp[0]
    asset_class = rsp["asset_class"]

    # A Symbol can only be stored in one schema at a time
    if rsp["store_tick"]:
        schema = Schema.TICK_DATA
    elif rsp["store_minute"]:
        schema = Schema.MINUTE_DATA
    elif rsp["store_aggregate"]:
        schema = Schema.AGGREGATE_DATA
    else:
        log.warning("Requested metadata for Symbol w/ pkey %s, but it is not set to be stored.")
        return []

    try:
        metadata = _fetch_stored_metadata(db, pkey)
        req_tables = db._table_config[schema].raw_tables(asset_class)
        metadata.extend(_missing_metadata(pkey, metadata, req_tables, schema))
    except KeyError as e:
        raise KeyError(  # Reraise a more informative error.
            "Ensure configure_timeseries_schema has been run prior to inserting symbol data."
        ) from e

    return metadata


def _missing_metadata(
    pkey: int,
    stored_metadata: list[MetadataInfo],
    req_tables: list[AssetTable],
    schema: Schema,
) -> list[MetadataInfo]:
    "Determines what metadata is missing, if any, given a list of required tables"
    stored_tables = [mdata.table for mdata in stored_metadata]
    # As long as the hash of an AssetTable is a string this will work.
    missing_tables = set(req_tables).difference(stored_tables)
    missing_metadata = [
        MetadataInfo(
            pkey,
            table.table_name,
            schema,
            Timestamp("1800-01-01", tz="UTC"),  # Default values
            Timestamp("1800-01-01", tz="UTC"),
            table,
        )
        for table in missing_tables
    ]
    return missing_metadata


def _round_large_timeframe(timeframe: Timedelta) -> Timedelta:
    """
    Round Timeframes greater than the HTF_Origin Cross over to unix Week/Month/Year Values
    This function works in tandem with timeseries.py::_interval()
    """
    if timeframe < HTF_CROSSOVER:
        return timeframe

    ratio = timeframe / Timedelta("365.25D")
    # Check if Within ~ 2 Days of a unix year.
    if abs(ratio - round(ratio)) < 0.005:
        # Return Timedelta of some multiple of 365 days. Will be translated into years interval
        return Timedelta(days=round(ratio) * 365)

    ratio = timeframe / Timedelta("30.44D")
    # Check if Within ~ a Day of a unix month.
    if abs(ratio - round(ratio)) < 0.035:
        # Return Timedelta of some multiple of 30 days. Will be translated into months interval
        return Timedelta(days=round(ratio) * 30)

    return timeframe
