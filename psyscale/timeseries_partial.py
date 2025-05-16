"Psyscale Partial Class to add Timeseries Schema Configuration script"

from abc import abstractmethod
import logging
from typing import Any, Dict, Iterable, Literal, Optional, get_args
from itertools import chain

import psycopg as pg
from pandas import DataFrame, Timestamp, Timedelta

from psyscale.async_core import PsyscaleAsyncCore
from psyscale.psql.orm import MetadataArgs, MetadataInfo
from psyscale.psql.timeseries import AggregateArgs, TickArgs

from .psql import (
    GenericTbls,
    Operation as Op,
    Schema,
    SeriesTbls,
    TimeseriesConfig,
    Commands,
)

from .core import PsyscaleConnectParams, PsyscaleCore, TupleCursor

log = logging.getLogger("psyscale_log")

# pylint: disable='missing-function-docstring','unused-argument','protected-access'


class TimeseriesPartialAbstract(PsyscaleCore):
    """
    Psyscale Partial Timeseries Abstract base Class to add Timeseries Schema Configuration script.

    Defines abstract methods for MetadataPartial & Series Partial. Complicated Class structiure is
    done to organize the complex functionality required by the Psyscale Class.
    """

    def __init__(
        self,
        conn_params: Optional[PsyscaleConnectParams | str] = None,
        *,
        down_on_del: bool = False,
        docker_compose_fpath: Optional[str] = None,
    ):
        # Chain the __init__ Docstring up the MRO since nothing changed
        self.__class__.__init__.__doc__ = super().__init__.__doc__
        super().__init__(
            conn_params,
            down_on_del=down_on_del,
            docker_compose_fpath=docker_compose_fpath,
        )
        self._read_db_timeseries_config()

    def configure_timeseries_schema(
        self,
        tick_tables: Optional[TimeseriesConfig] = None,
        minute_tables: Optional[TimeseriesConfig] = None,
        aggregate_tables: Optional[TimeseriesConfig] = None,
    ):
        """
        Compare the given TimeseriesConfig Object to what is stored. Makes Database changes via CLI
        as needed. Deletion of Calculated and Stored information will be confirmed before execution.
        """

        with self._cursor() as cursor:
            cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA]())
            schemas = set(get_args(Schema))

            # Check & Create Schemas
            for schema in schemas.difference({rsp[0] for rsp in cursor.fetchall()}):
                log.info("Creating Schema '%s'", schema)
                cursor.execute(self[Op.CREATE, GenericTbls.SCHEMA](schema))

            cursor.connection.commit()

            # Create Each Class of Timeseries Table
            if tick_tables is not None:
                _configure_timeseries_schema(
                    self, cursor, Schema.TICK_DATA, tick_tables
                )
            if minute_tables is not None:
                _configure_timeseries_schema(
                    self, cursor, Schema.MINUTE_DATA, minute_tables
                )
            if aggregate_tables is not None:
                _configure_timeseries_schema(
                    self, cursor, Schema.AGGREGATE_DATA, aggregate_tables
                )

        # Ensure The appropriate timeseries info is stored in the event this class is used
        # directly after setting the config
        self._read_db_timeseries_config()

    def _read_db_timeseries_config(self):
        "Read off the TimeseriesConfig for each schema by probing all the table names."
        self._table_config: Dict[Schema, TimeseriesConfig] = {}

        with self._cursor() as cursor:
            for schema in (Schema.TICK_DATA, Schema.MINUTE_DATA, Schema.AGGREGATE_DATA):
                # ---- ---- Read the Origin Timestamp Table ---- ----
                origin_map = {}
                try:
                    cursor.execute(
                        self[Op.SELECT, SeriesTbls._ORIGIN](schema, _all=True)
                    )
                    for (asset, *origins), *_ in cursor.fetchall():
                        # Cursed parsing for the cursor response tuple.
                        # Origins must be RTH, ETH, then HTF
                        origin_map[asset] = tuple(map(Timestamp, origins))

                except pg.DatabaseError:
                    # Origin Table does not exist, Rollback to clear error state
                    cursor.connection.rollback()
                    log.debug("Origin table not found in Schema: %s", schema)

                # ---- Reconstruct Timeseries Config from existing table names ----
                cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA_TABLES](schema))
                tbl_names = [
                    rsp[0]
                    for rsp in cursor.fetchall()
                    if rsp[0] != SeriesTbls._ORIGIN.value
                ]
                config = TimeseriesConfig.from_table_names(tbl_names, origin_map)
                self._table_config[schema] = config

                # ---- ---- Check that all the origin times are preset ---- ----
                missing_asset_origins = set(config.asset_classes).difference(
                    origin_map.keys()
                )
                if len(missing_asset_origins) > 0:
                    log.error(
                        "TimescaleDB Origins Table in schema '%s' is missing values "
                        "for the following assets: %s",
                        schema,
                        missing_asset_origins,
                    )

        # Give a notification on how to setup the database if it appears like it hasn't been
        all_assets = {
            chain(map(lambda x: x.asset_classes, self._table_config.values()))
        }
        if len(all_assets) == 0:
            log.warning(
                "No Asset Types Detected in the Database. To Initialize the Database call "
                "PsyscaleDB.configure_timeseries_schema() with the appropriate arguments.\n"
            )

    # region ---- ---- Abstract methods implemented by partial subclasses ---- ----
    @abstractmethod
    def inferred_metadata(
        self, symbol: int | str, timeframe: Timedelta, rth: bool = False
    ) -> MetadataInfo | None: ...

    @abstractmethod
    def stored_metadata(
        self,
        symbol: int | str,
        filters: dict[MetadataArgs | str, Any] = {},
        *,
        _all: bool = False,
    ) -> list[MetadataInfo]: ...

    @abstractmethod
    def manually_refresh_aggregate_metadata(self): ...

    @abstractmethod
    def get_series(
        self,
        symbol: int | str,
        timeframe: Timedelta,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        limit: Optional[int] = None,
        rth: bool = False,
        rtn_args: Optional[Iterable[AggregateArgs | TickArgs | str]] = None,
        *,
        mdata: Optional[MetadataInfo] = None,
    ) -> Optional[DataFrame]: ...

    @abstractmethod
    def upsert_series(
        self,
        pkey: int,
        metadata: MetadataInfo,
        data: DataFrame,
        exchange: Optional[str] = None,
        *,
        on_conflict: Literal["update", "error", "ignore"] = "ignore",
    ): ...

    @abstractmethod
    def refresh_aggregate_metadata(self): ...

    # endregion


class TimeseriesPartialAsyncAbstract(PsyscaleAsyncCore):
    "Abstract Class for Async Timeseries Database Operations"

    @abstractmethod
    async def upsert_series_async(
        self,
        pkey: int,
        metadata: MetadataInfo,
        data: DataFrame,
        exchange: Optional[str] = None,
        *,
        on_conflict: Literal["update", "error", "ignore"] = "ignore",
    ): ...

    @abstractmethod
    async def get_series_async(
        self,
        symbol: int | str,
        timeframe: Timedelta,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        limit: Optional[int] = None,
        rth: bool = False,
        rtn_args: Optional[Iterable[AggregateArgs | TickArgs | str]] = None,
        *,
        mdata: Optional[MetadataInfo] = None,
    ) -> Optional[DataFrame]: ...

    @abstractmethod
    async def refresh_aggregate_metadata_async(self): ...


def _configure_timeseries_schema(
    db: TimeseriesPartialAbstract,
    cursor: TupleCursor,
    schema: Schema,
    config: TimeseriesConfig,
):
    "Script to Make Changes to the configuration of stored Timeseries Data"
    cursor.execute(db[Op.SELECT, GenericTbls.SCHEMA_TABLES](schema))
    tables: set[str] = {rsp[0] for rsp in cursor.fetchall()}
    log.info("---- ---- ---- Configuring Timeseries Schema '%s' ---- ---- ----", schema)

    # Ensure Origins Timestamp Table exists in the schema
    if SeriesTbls._ORIGIN not in tables:
        log.info("Creating '%s'.'%s' Table\n", schema, SeriesTbls._ORIGIN)
        cursor.execute(db[Op.CREATE, SeriesTbls._ORIGIN](schema))
    else:
        tables -= {SeriesTbls._ORIGIN.value}
        log.debug("'%s'.'%s' Table Already Exists\n", schema, SeriesTbls._ORIGIN)

    stored_config = db._table_config[schema]

    log.info("---- Checking for Assets that need to be added. ----")
    _add_timeseries_asset_classes(db.cmds, cursor, schema, config, stored_config)
    cursor.connection.commit()

    log.info("---- Checking for Assets that need to be Changed. ----")
    _update_timeseries_asset_classes(db.cmds, cursor, schema, config, stored_config)
    cursor.connection.commit()

    log.info("---- Checking for Assets that need to be Removed. ----")
    _del_timeseries_asset_classes(db.cmds, cursor, schema, config, stored_config)
    cursor.connection.commit()


def _add_timeseries_asset_classes(
    cmds: Commands,
    cursor: TupleCursor,
    schema: Schema,
    config: TimeseriesConfig,
    stored_config: TimeseriesConfig,
):
    additions = set(config.asset_classes).difference(stored_config.asset_classes)
    if len(additions) == 0:
        log.info("No Asset_classes need to be Added.")
        return

    for asset in additions:
        log.info("Generating all tables for asset_class: %s", asset)

        origin_args = {
            "rth_origin": config.rth_origins[asset],
            "eth_origin": config.eth_origins[asset],
            "htf_origin": config.htf_origins[asset],
        }
        log.info("Inserting Origin Timestamps: %s", origin_args)
        cursor.execute(
            cmds[Op.INSERT, SeriesTbls._ORIGIN](schema, asset, **origin_args)
        )

        # Generate Raw insertion tables
        for tbl in config.raw_tables(asset):
            log.info("Generating table for: '%s'.'%s'", schema, tbl)
            tbl_type = (
                SeriesTbls.TICK
                if tbl.period == Timedelta(0)
                else SeriesTbls.RAW_AGGREGATE
            )

            cursor.execute(cmds[Op.CREATE, tbl_type](schema, tbl))

        # Generate Continuous Aggregates
        tbls = config.all_tables(asset, include_raw=False)
        tbls.sort(key=lambda x: x.period)  # Must generate lowest periods first
        for tbl in tbls:
            log.info("Generating Continuous Aggregate for: '%s'.'%s'", schema, tbl)
            ref_table = config.get_aggregation_source(tbl)
            tbl_type = (
                SeriesTbls.CONTINUOUS_TICK_AGG
                if ref_table.period == Timedelta(0)
                else SeriesTbls.CONTINUOUS_AGG
            )
            cursor.execute(cmd := cmds[Op.CREATE, tbl_type](schema, tbl, ref_table))
            log.debug("CMD: %s", cmd.as_string())


def _update_timeseries_asset_classes(
    cmds: Commands,
    cursor: TupleCursor,
    schema: Schema,
    config: TimeseriesConfig,
    stored_config: TimeseriesConfig,
):
    asset_updates = set(config.asset_classes).intersection(stored_config.asset_classes)
    if len(asset_updates) == 0:
        log.info("No Asset_classes need to be Updated.")
        return

    for asset in asset_updates:
        removals = set(stored_config.all_tables(asset)).difference(
            config.all_tables(asset)
        )

        additions = set(config.all_tables(asset)).difference(
            stored_config.all_tables(asset)
        )

        if len(removals) == 0 and len(additions) == 0:
            log.info("No changes needed for asset_class: %s", asset)
            continue

        _del = input(
            f"Aggregated Data Table Changes exist for Asset_class: '{schema}'.'{asset}'\n"
            "Updating these changes requires all Calculated Aggregates to be removed and "
            "recalculated.\n-- All Inserted data *will* be retained --\n"
            "Update Config? y/[N] : "
        )
        if not (_del == "y" or _del == "Y"):
            continue

        origin_args = {
            "rth_origin": config.rth_origins[asset],
            "eth_origin": config.eth_origins[asset],
            "htf_origin": config.htf_origins[asset],
        }
        log.info("Updating Origin Timestamps: %s", origin_args)
        cursor.execute(
            cmds[Op.UPDATE, SeriesTbls._ORIGIN](schema, asset, **origin_args)
        )

        # Remove All Calculated Data Tables
        log.info("Updating config for Asset: %s", asset)

        # Must Remove Longest Aggregates first.
        all_aggregates = stored_config.all_tables(asset, include_raw=False)
        all_aggregates.sort(key=lambda x: x.period, reverse=True)
        for tbl in all_aggregates:
            log.info("Dropping Table: %s", tbl.table_name)
            cursor.execute(cmds[Op.DROP, GenericTbls.VIEW](schema, tbl.table_name))

        # Remove Unwanted Inserted Table Data
        for tbl in [tbl for tbl in removals if tbl.raw]:
            _del = input(
                f"Inserted Data Table '{tbl}' exists in current database, but not in the new "
                "config.\nThis table contains inserted raw data with an aggregation period of "
                f"{tbl.period}. \nDelete it? y/[N] : "
            )
            # Technically this introduces a bug but it's too much an edge case to care atm.
            # If the table is retained it will only be used for data retrieval after restart.
            # Despite if it is the lowest timeframe and should be used as the source for all
            # aggregations.
            if not (_del == "y" or _del == "Y"):
                continue

            log.info("Dropping Inserted Table: %s", tbl.table_name)
            cursor.execute(cmds[Op.DROP, GenericTbls.TABLE](schema, tbl.table_name))

        # Create new Raw Tables
        for tbl in [tbl for tbl in additions if tbl.raw]:
            log.info("Generating table for: '%s'.'%s'", schema, tbl)
            tbl_type = (
                SeriesTbls.TICK
                if tbl.period == Timedelta(0)
                else SeriesTbls.RAW_AGGREGATE
            )

            cursor.execute(cmds[Op.CREATE, tbl_type](schema, tbl))

        # Generate Continuous Aggregates
        tbls = config.all_tables(asset, include_raw=False)
        tbls.sort(key=lambda x: x.period)  # Must generate lowest periods first
        for tbl in tbls:
            log.info("Generating Continuous Aggregate for: '%s'.'%s'", schema, tbl)
            ref_table = config.get_aggregation_source(tbl)
            tbl_type = (
                SeriesTbls.CONTINUOUS_TICK_AGG
                if ref_table.period == Timedelta(0)
                else SeriesTbls.CONTINUOUS_AGG
            )
            cursor.execute(cmd := cmds[Op.CREATE, tbl_type](schema, tbl, ref_table))
            log.debug("CMD: %s", cmd.as_string())


def _del_timeseries_asset_classes(
    cmds: Commands,
    cursor: TupleCursor,
    schema: Schema,
    config: TimeseriesConfig,
    stored_config: TimeseriesConfig,
):

    removals = set(stored_config.asset_classes).difference(config.asset_classes)
    if len(removals) == 0:
        log.info("No Asset_classes need to be removed.")
        return

    for asset in removals:
        log.info("Checking if asset_class should be removed: %s", asset)

        _del = input(
            f"Asset_class: '{schema}'.'{asset}' exists in current database, "
            "but not in the given config. Remove it? y/[N] : "
        )

        if not (_del == "y" or _del == "Y"):
            log.info("Keeping asset_class: %s", asset)
            continue

        _del = input(
            "This will permanently remove all Downloaded and Calculated Data. "
            "Are you Sure? y/[N] : "
        )

        if not (_del == "y" or _del == "Y"):
            log.info("Keeping asset_class: %s", asset)
            continue

        log.info("Removing Asset Class: %s", asset)
        cursor.execute(cmds[Op.DELETE, SeriesTbls._ORIGIN](schema, asset))

        # Must delete Largest Aggregates First
        tbls = stored_config.all_tables(asset)
        tbls.sort(key=lambda x: x.period, reverse=True)
        for tbl in tbls:
            # Catch all Table Type for Generic Drop Commands, Will Cascade
            tbl_type = GenericTbls.TABLE if tbl.raw else GenericTbls.VIEW
            cursor.execute(cmd := cmds[Op.DROP, tbl_type](schema, tbl.table_name))
            log.debug(cmd.as_string())


# pylint: disable='wrong-import-position'
# At EOF import the Partials that are built on the above abstract classes
# to create the partials that have the full functionality and are actually used.
from .metadata_partial import MetadataPartial
from .series_data_partial import SeriesDataPartial, AsyncSeriesDataPartial


class TimerseriesPartial(MetadataPartial, SeriesDataPartial):
    "Partial PsyscaleDB Class that includes all SeriesData & Metadata Functions"


class TimeseriesAsyncPartial(MetadataPartial, AsyncSeriesDataPartial):
    "Partial PsyscaleAsync Class that includes all Sync & Async SeriesData & Metadata Functions"
