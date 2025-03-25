"""Extension to the TimescaleDB Class to add Function Scripts that Configure and Manage Data"""

import logging
from typing import Literal, Optional, Tuple

from pandas import DataFrame, Series, Timedelta

from .orm import TimeseriesConfig
from .sql_cmds import (
    Generic,
    Operation as Op,
    Schema,
    AssetTbls,
    SeriesTbls,
)

from .api import TimeScaleDB, TupleCursor

log = logging.getLogger("pycharts-timescaledb")

# pylint: disable='invalid-name','protected-access'


class TimescaleDB_EXT(TimeScaleDB):
    """
    Class Extension to add Configuration and Data Management Functions.

    This class is designed to extend the core functionality to enable one-off scripts,
    and reoccurring data-management scripts without cluttering the core functionality
    needed at application runtime.
    """

    # region ---- ---- ---- Public Dunder Sub-routines ---- ---- ----

    def __configure_db_format__(
        self,
        tick_tables: Optional[TimeseriesConfig] = None,
        minute_tables: Optional[TimeseriesConfig] = None,
        aggregate_tables: Optional[TimeseriesConfig] = None,
    ):
        """
        Compare the given TimeseriesConfig Object to what is stored. Make changes to the Database
        as needed. Deletion of Calculated and Stored information will be confirmed before execution.
        """

        with self._cursor() as cursor:
            cursor.execute(self[Op.SELECT, Generic.SCHEMA]())
            schemas = self.cmds.all_schemas

            # Check & Create Schemas
            for schema in schemas.difference({rsp[0] for rsp in cursor.fetchall()}):
                log.info("Creating Schema '%s'", schema)
                cursor.execute(self[Op.CREATE, Generic.SCHEMA](schema))

            cursor.connection.commit()

            # Create Security Tables as Needed.
            self._ensure_symbols_table_exists(cursor)

            # Ensure a sub_function needed for the _metadata table is present
            log.debug("Ensuring Timeseries _metadata sub-function exists.")
            cursor.execute(self[Op.CREATE, SeriesTbls._METADATA_FUNC]())

            # Create Each Class of Timeseries Table
            if tick_tables is not None:
                self._configure_timeseries_schema(cursor, Schema.TICK_DATA, tick_tables)
            if minute_tables is not None:
                self._configure_timeseries_schema(
                    cursor, Schema.MINUTE_DATA, minute_tables
                )
            if aggregate_tables is not None:
                self._configure_timeseries_schema(
                    cursor, Schema.AGGREGATE_DATA, aggregate_tables
                )

    # endregion

    # region ---- ---- ---- Public Security Sub-routines ---- ---- ----

    def upsert_securities(
        self,
        symbols: DataFrame,
        source: str,
        *,
        on_conflict: Literal["update", "ignore"] = "update",
    ) -> Tuple[Series, Series]:
        """
        Insert the Dataframe of symbols into the database.
        Primary Each for each entry is (Ticker, Exchange, Source)

        -- PARAMS --

        symbols: Dataframe.
            - Required Columns {ticker:str, name:str, exchange:str}:
                -- symbol:str - Ticker Symbol abbreviation
                -- name:Str - Full String Name of the Symbol
                -- exchange:str - Abbrv. Exchange Name for the symbol
                -- asset_class:str - Type of asset, This must match the Timeseries Config
                    asset_classes otherwise data for this symbol cannot be inserted into the database.

            - Any Extra Columns will be packaged into JSON and dumped into an 'attrs' Dictionary.
            To prevent bloat, drop all columns that will not be used.

        source: string.
            - string representation of what API Sourced the symbol
            - e.g. Alpaca, IBKR, Polygon, Coinbase, etc.

        on_conflict: Literal["update", "ignore"] : default = "update"
            Update or Do Nothing when given a symbol that has a conflicting primary key.

        -- RETURNS --
            Tuple of [Series, Series]
            First Series Object is inserted Symbols
            Second Series Object is Updated Symbols
        """
        if not isinstance(source, str) or source == "":
            log.error("Cannot Insert Symbols, Invalid Source, Source = %s", source)
            return Series(), Series()
        if not isinstance(symbols, DataFrame):
            log.error("Cannot Insert Symbols, Invalid Symbols Argument")
            return Series(), Series()

        symbols.columns = symbols.columns.str.lower()
        req_cols = {"symbol", "name", "exchange", "asset_class"}
        missing_cols = req_cols.difference(symbols.columns)
        if len(missing_cols) != 0:
            log.error(
                "Cannot insert symbols. Dataframe missing Columns: %s", missing_cols
            )
            return Series(), Series()

        # Convert to a format that can be inserted into the database
        symbols_fmt = symbols[[*req_cols]].copy()

        # Turn all extra Columns into an attributes json obj.
        symbols_fmt.loc[:, "attrs"] = symbols[
            [*set(symbols.columns).difference(req_cols)]
        ].apply(lambda x: x.to_json(), axis="columns")

        insert_args = {}
        for k, v in symbols_fmt.to_dict(orient="series").items():
            insert_args[str(k)] = v.to_list()

        # Insert Args a Dictionary of numpy arrays and a contant source.
        # This dictionary is passed as arguments to Postgres that are then unnested.
        insert_args["source"] = source
        response = DataFrame()

        with self._cursor() as cursor:
            self._ensure_symbols_table_exists(cursor)

            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            cursor.execute(self[_op, AssetTbls.SYMBOLS](), insert_args)
            response = DataFrame(cursor.fetchall())

        if len(response) == 0:
            return Series(), Series()

        if _op == Op.INSERT:
            # All Returned symbols in response were inserted, none updated.
            return response[0], Series()
        else:
            # Second Column is xmax, on insertion this is 0, on update its != 0
            inserted = response[1] == "0"
            return response.loc[inserted, 0], response.loc[~inserted, 0]

    # endregion

    # region ---- ---- ---- Private Security Sub-routines ---- ---- ----

    def _ensure_symbols_table_exists(self, cursor: TupleCursor):
        cursor.execute(self[Op.SELECT, Generic.TABLE](Schema.SECURITY))
        tables: set[str] = {rsp[0] for rsp in cursor.fetchall()}

        if AssetTbls.SYMBOLS not in tables:
            log.info("Creating Table '%s'.'%s'", Schema.SECURITY, AssetTbls.SYMBOLS)
            cursor.execute(self[Op.CREATE, AssetTbls.SYMBOLS]())

    # endregion

    # region ---- ---- ---- Private Timeseries Sub-routines ---- ---- ----

    def _configure_timeseries_schema(
        self,
        cursor: TupleCursor,
        schema: Schema,
        config: TimeseriesConfig,
    ):
        "Script to Make Changes to the configuration of stored Timeseries Data"
        cursor.execute(self[Op.SELECT, Generic.TABLE](schema))
        tables: set[str] = {rsp[0] for rsp in cursor.fetchall()}
        log.info(
            "---- ---- ---- Configuring Timeseries Schema '%s' ---- ---- ----", schema
        )

        # Ensure Origins Timestamp Table exists in the schema
        if SeriesTbls._ORIGIN not in tables:
            log.info("Creating '%s'.'%s' Table\n", schema, SeriesTbls._ORIGIN)
            cursor.execute(self[Op.CREATE, SeriesTbls._ORIGIN](schema))
        else:
            tables -= {SeriesTbls._ORIGIN.value}
            log.debug("'%s'.'%s' Table Already Exists\n", schema, SeriesTbls._ORIGIN)

        # Create Metadata View if it does not exist in the schema (wont be listed in tables)
        log.info("Ensuring Creation of '%s'.'%s' View\n", schema, SeriesTbls._METADATA)
        cursor.execute(self[Op.CREATE, SeriesTbls._METADATA](schema))

        stored_config = self.table_config[schema]

        log.info("---- Checking for Assets that need to be added. ----")
        self._add_timeseries_asset_classes(cursor, schema, config, stored_config)
        cursor.connection.commit()

        log.info("---- Checking for Assets that need to be Changed. ----")
        self._update_timeseries_asset_classes(cursor, schema, config, stored_config)
        cursor.connection.commit()

        log.info("---- Checking for Assets that need to be Removed. ----")
        self._del_timeseries_asset_classes(cursor, schema, config, stored_config)
        cursor.connection.commit()

    def _add_timeseries_asset_classes(
        self,
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
                self[Op.INSERT, SeriesTbls._ORIGIN](schema, asset, **origin_args)
            )

            # Generate Raw insertion tables
            for tbl in config.inserted_tables(asset):
                log.info("Generating table for: '%s'.'%s'", schema, tbl)
                tbl_type = (
                    SeriesTbls.TICK
                    if tbl.period == Timedelta(0)
                    else SeriesTbls.RAW_AGGREGATE
                )

                cursor.execute(cmd := self[Op.CREATE, tbl_type](schema, tbl))
                log.debug("CMD: %s", cmd.as_string())

            # Generate Continuous Aggregates
            tbls = config.all_tables(asset, include_raw=False)
            tbls.sort(key=lambda x: x.period)  # Must generate lowest periods first
            for tbl in tbls:
                log.info("Generating Continuous Aggregate for: '%s'.'%s'", schema, tbl)
                ref_table = config.get_aggregation_source(tbl)
                tbl_type = (
                    SeriesTbls.TICK_AGGREGATE
                    if ref_table.period == Timedelta(0)
                    else SeriesTbls.AGGREGATE
                )
                cursor.execute(cmd := self[Op.CREATE, tbl_type](schema, tbl, ref_table))
                log.debug("CMD: %s", cmd.as_string())

    def _update_timeseries_asset_classes(
        self,
        cursor: TupleCursor,
        schema: Schema,
        config: TimeseriesConfig,
        stored_config: TimeseriesConfig,
    ):
        asset_updates = set(config.asset_classes).intersection(
            stored_config.asset_classes
        )
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
                f"Detected Differences with Config of Asset_class: '{schema}'.'{asset}'\n"
                "This requires all Calculated Aggregates to be removed and recalculated.\n"
                "However, All Inserted data *will* be retained.\n"
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
                self[Op.UPDATE, SeriesTbls._ORIGIN](schema, asset, **origin_args)
            )

            # Remove All Calculated Data Tables
            log.info("Updating config for Asset: %s", asset)

            # Must Remove Longest Aggregates first.
            all_aggregates = stored_config.all_tables(asset, include_raw=False)
            all_aggregates.sort(key=lambda x: x.period, reverse=True)
            for tbl in all_aggregates:
                log.info("Dropping Table: %s", tbl.table_name)
                cursor.execute(self[Op.DROP, Generic.VIEW](schema, tbl.table_name))

            # Remove Unwanted Inserted Table Data
            for tbl in [tbl for tbl in removals if tbl.raw]:
                _del = input(
                    f"Table '{tbl}' exists in current database, but not in the new config.\n"
                    "This table contains inserted raw data with an aggregation period of "
                    f"{tbl.period}. \nDelete it? y/[N] : "
                )
                # Technically this introduces a bug but it's too much an edge case to care atm.
                # If the table is retained it will only be used for data retrieval after restart.
                # Despite if it is the lowest timeframe and should be used as the source for all
                # aggregations.
                if not (_del == "y" or _del == "Y"):
                    continue

                log.info("Dropping Inserted Table: %s", tbl.table_name)
                cursor.execute(self[Op.DROP, Generic.TABLE](schema, tbl.table_name))

            # Create new Raw Tables
            for tbl in [tbl for tbl in additions if tbl.raw]:
                log.info("Generating table for: '%s'.'%s'", schema, tbl)
                tbl_type = (
                    SeriesTbls.TICK
                    if tbl.period == Timedelta(0)
                    else SeriesTbls.RAW_AGGREGATE
                )

                cursor.execute(cmd := self[Op.CREATE, tbl_type](schema, tbl))
                log.debug("CMD: %s", cmd.as_string())

            # Generate Continuous Aggregates
            tbls = config.all_tables(asset, include_raw=False)
            tbls.sort(key=lambda x: x.period)  # Must generate lowest periods first
            for tbl in tbls:
                log.info("Generating Continuous Aggregate for: '%s'.'%s'", schema, tbl)
                ref_table = config.get_aggregation_source(tbl)
                tbl_type = (
                    SeriesTbls.TICK_AGGREGATE
                    if ref_table.period == Timedelta(0)
                    else SeriesTbls.AGGREGATE
                )
                cursor.execute(cmd := self[Op.CREATE, tbl_type](schema, tbl, ref_table))
                log.debug("CMD: %s", cmd.as_string())

    def _del_timeseries_asset_classes(
        self,
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
            cursor.execute(self[Op.DELETE, SeriesTbls._ORIGIN](schema, asset))

            # Must delete Largest Aggregates First
            tbls = stored_config.all_tables(asset)
            tbls.sort(key=lambda x: x.period, reverse=True)
            for tbl in tbls:
                # Catch all Table Type for Generic Drop Commands, Will Cascade
                tbl_type = Generic.TABLE if tbl.raw else Generic.VIEW
                cursor.execute(cmd := self[Op.DROP, tbl_type](schema, tbl.table_name))
                log.debug(cmd.as_string())

    # endregion
