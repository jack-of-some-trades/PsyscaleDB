"""Extension to the TimescaleDB Class to add Function Scripts that Configure and Manage Data"""

import logging
from typing import Literal, Optional, Tuple, get_args

from pandas import DataFrame, Series, Timedelta, Timestamp

from .series_df import Series_DF
from .psql import (
    GenericTbls,
    MetadataInfo,
    Operation as Op,
    Schema,
    AssetTbls,
    SeriesTbls,
    AssetTable,
    TimeseriesConfig,
)

from .client import PsyscaleDB, TupleCursor

log = logging.getLogger("psyscale_log")

# pylint: disable='invalid-name','protected-access'


class PsyscaleMod(PsyscaleDB):
    """
    PsyscaleDB class extension for connecting to a PostgreSQL Database in a read/write mode.
    This class offers additional configuration and data management functions that are not
    normally needed at runtime.
    """

    # region ---- ---- ---- Public Dunder Sub-routines ---- ---- ----

    def configure_timeseries_schema(
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
            cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA]())
            schemas = set(get_args(Schema))

            # Check & Create Schemas
            for schema in schemas.difference({rsp[0] for rsp in cursor.fetchall()}):
                log.info("Creating Schema '%s'", schema)
                cursor.execute(self[Op.CREATE, GenericTbls.SCHEMA](schema))

            cursor.connection.commit()

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
                    asset_classes otherwise data for this symbol cannot be inserted into the
                    database.

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

        # Insert Args a Dictionary of numpy arrays and a contant source.
        # This dictionary is passed as arguments to Postgres that are then unnested.
        response = DataFrame()

        with self._cursor() as cursor:
            # Create & Inject the Data into a Temporary Table
            cursor.execute(self[Op.CREATE, AssetTbls.SYMBOLS_BUFFER]())
            copy_cmd = self[Op.COPY, AssetTbls.SYMBOLS_BUFFER](
                # Sends the COPY Cmd & the order of the Columns of the Dataframe
                [str(c) for c in symbols_fmt.columns]
            )
            with cursor.copy(copy_cmd) as copy:
                for row in symbols_fmt.itertuples(index=False, name=None):
                    # Writes each row as a Tuple that matches the Dataframe Column Order
                    copy.write_row(row)

            # Merge the Temp Table By inserting / upserting from the Temporary Table
            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            cursor.execute(self[_op, AssetTbls.SYMBOLS_BUFFER](source))

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

    # region ---- ---- ---- Public Timeseries Sub-routines ---- ---- ----

    def get_all_symbol_series_metadata(self, pkey: int) -> list[MetadataInfo]:
        """
        Augmented call to TimesacleDB.symbol_series_metadata() to ensure MetadataInfo dataclasses
        are returned for tables if the Schema's TableConfig says there should be data for the symbol
        but there isn't.

        i.e. When A Symbol was just flagged to be stored but has nothing stored yet
        symbol_series_metadata will not return any Metadata since it only checks what *is* stored,
        not what should be. This function will return what is stored + what should be stored.

        If no information is stored for a given table, but should be, the start_date & end_date will
        be set to "1800-01-01" So when a request is made it should automatically fetch all recorded
        data for the symbol.
        """

        with self._cursor(dict_cursor=True) as cursor:
            _rtn_args = ["asset_class", "store_tick", "store_minute", "store_aggregate"]
            _filter = ("pkey", "=", pkey)

            cursor.execute(
                self[Op.SELECT, GenericTbls.TABLE](
                    Schema.SECURITY, AssetTbls.SYMBOLS, _rtn_args, _filter
                )
            )
            # fetchall instead of fetchone is preferred to guarantee buffer is empty.
            rsp = cursor.fetchall()
            if len(rsp) == 0:
                raise ValueError(
                    f"Cannot determine Symbol updates needed. {pkey = } is unknown."
                )
            rsp = rsp[0]
            asset_class = rsp["asset_class"]

        metadata = []
        try:
            if rsp["store_tick"]:
                metadata.extend(
                    self._all_symbol_metadata(pkey, asset_class, Schema.TICK_DATA)
                )
            if rsp["store_minute"]:
                metadata.extend(
                    self._all_symbol_metadata(pkey, asset_class, Schema.MINUTE_DATA)
                )
            if rsp["store_aggregate"]:
                metadata.extend(
                    self._all_symbol_metadata(pkey, asset_class, Schema.AGGREGATE_DATA)
                )
        except KeyError as e:
            raise KeyError(  # Reraise a more informative error.
                "Ensure configure_timeseries_schema has been run prior to inserting symbol data."
            ) from e

        return metadata

    def upsert_symbol_data(
        self,
        pkey: int,
        metadata: MetadataInfo,
        data: DataFrame,
        exchange: Optional[str] = None,
        *,
        on_conflict: Literal["update", "error"] = "error",
    ):
        """
        Insert or Upsert symbol data to the database.

        -- PARAMS --
        - pkey : int. Primary Key of the symbol to insert
        - metadata: MetadataInfo Object
            - Contains the schema_name & table_name to insert the data into, can be retrieved from
            calling 'get_all_symbol_series_metadata' or 'symbol_series_metadata'
        - Data: Dataframe.
            - Should contain all series data needed to be inserted. Multiple names will be
            recognized for each given series parameter.
            - i.e. time/datetime/date/dt ... etc will all be recognized as the timestamp column.
        - exchange : str | None
            - Exchange that the Asset is traded on. This will be passed to pandas_market_calendars
            so the RTH/ETH session of each datapoint can be determined and stored as necessary.
            - None can be passed for 24/7 Exchanges such as Crypto. (Note: Forex would require 24/5 be passed)
        - on_conflict : Literal["update", "error"] = "error"
            - Action to take when a UNIQUE conflict occurs. Erroring allows for faster insertion
            if it can be ensured that given data will be unique
        """
        table = (
            metadata.table
            if metadata.table is not None
            else AssetTable.from_table_name(metadata.table_name)
        )

        # region ---- Check that the data matches name and 'NOT NULL' expectations
        series_df = Series_DF(data, exchange)  # Rename cols & Populate 'rth'

        try:
            if table.period != Timedelta(0):
                # Aggregate Specific Expectations
                assert table.period == series_df.timedelta
                assert "close" in series_df.columns
                assert not series_df.df["close"].isna().any()
            else:
                # Tick Specific Expectations
                assert "price" in series_df.columns
                assert not series_df.df["price"].isna().any()

            # Regardless if Tick or aggregate, check the 'rth' to be NOT NULL when needed.
            if table.has_rth:
                assert "rth" in series_df.columns
                if (nans := series_df.df["rth"].isna()).any():
                    drops = series_df.df[nans]
                    log.warning(
                        "Edge-case mark session error. Dropping %s data-points: %s.",
                        len(drops),
                        drops,
                    )
                    series_df.df = series_df.df[~nans]
            elif "rth" in series_df.columns:
                extra_rows = series_df.df["rth"] != 0
                if len(extra_rows) > 0:
                    log.warning(
                        "Given Extended hours data to an aggregate table that doesn't need it."
                        "Dropping extra data rows : %s",
                        extra_rows,
                    )
                    # drop all ext hours datapoints
                    series_df.df = series_df.df[~extra_rows]
                series_df.df.drop(columns="rth")

            assert "dt" in series_df.columns
            assert not series_df.df["dt"].isna().any()
        except AssertionError as e:
            raise ValueError(
                "Cannot insert symbol data into TimescaleDB. Dataframe is not formatted correctly"
            ) from e

        # endregion

        # Setup and copy data into database
        buffer_tbl_type = (
            SeriesTbls.TICK_BUFFER
            if table.period == Timedelta(0)
            else SeriesTbls.RAW_AGG_BUFFER
        )

        # region ---- Filter Timestamps When Purely Inserting Data. ---
        data_fmt = series_df.df

        if on_conflict == "error":
            # When inserting ensure only the range that needs to be added is.
            # Pretty common there's 1 extra data point at the start of a df.
            before_start = data_fmt["dt"] < metadata.start_date
            after_end = data_fmt["dt"] > metadata.end_date
            dt_filter = before_start | after_end

            if not dt_filter.all():
                extra_data = series_df.df[~dt_filter]
                log.debug(
                    "Given %s extra data point(s), dropping the following :\n %s",
                    len(extra_data),
                    extra_data,
                )
                data_fmt = data_fmt[dt_filter]
        # endregion

        with self._cursor() as cursor:
            # Create & Inject the Data into a Temporary Table
            cursor.execute(self[Op.CREATE, buffer_tbl_type](table))
            copy_cmd = self[Op.COPY, buffer_tbl_type](
                # Sends the COPY Cmd & the order of the Columns of the Dataframe
                [str(c) for c in data_fmt.columns]
            )
            with cursor.copy(copy_cmd) as copy:
                for row in data_fmt.itertuples(index=False, name=None):
                    # Writes each row as a Tuple that matches the Dataframe Column Order
                    copy.write_row(row)

            # Merge the Temp Table By inserting / upserting from the Temporary Table
            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            cursor.execute(
                self[_op, buffer_tbl_type](metadata.schema_name, table, pkey)
            )
            log.info("Symbol Data Upsert Status Message: %s", cursor.statusmessage)

        self._update_series_data_edit_record(metadata, data_fmt, table)
        # endregion

    def refresh_aggregate_metadata(self):
        """
        Refresh Continuous Aggregates & the Timeseries Metadata Table based on upserts made.
        Designed to be called after all known data insertions have been made.

        Edits made using 'upsert_symbol_data()' are tracked. This includes individual tables and the
        respective time-ranges edited. This method uses that stored information to update only what
        needs to be updated.

        CAVEAT: This only works so long as this is the same class instance that made the updates
        in the first place. If that instance is deleted before calling this function
        refresh_all_aggregates_and_metadata() must be invoked manually.
        """
        if not hasattr(self, "_altered_tables"):
            log.info("No Series Data has been inserted, Skipping Metadata Refresh.")
            return

        with self._cursor(auto_commit=True) as cursor:

            # Loop Through Schemas
            for schema, mdata_dict in self._altered_tables_mdata.items():
                log.info(
                    " ---- ---- Refreshing Timeseries Schema : %s  ---- ---- ", schema
                )

                # Loop Through Edited Tables
                for table_name, mdata in mdata_dict.items():
                    log.info(
                        " --- Refreshing Aggregates Associated with Table : %s ---- ",
                        table_name,
                    )
                    assert mdata.table  # Ensuring mata.Table is defined by post_init
                    cont_aggs = self.table_config[Schema(schema)].get_tables_to_refresh(
                        mdata.table
                    )
                    # Add some buffer dates so entire time chucks are covered
                    # Times Chucks will not refresh unless they are completely included
                    mdata.start_date -= Timedelta("4W")
                    mdata.end_date += Timedelta("4W")

                    for table in cont_aggs:
                        if table.raw:
                            continue

                        log.info(
                            "Refreshing Continuous Aggregate : %s ", table.table_name
                        )
                        cursor.execute(
                            self[Op.REFRESH, SeriesTbls.CONTINUOUS_AGG](
                                schema, table, mdata.start_date, mdata.end_date
                            )
                        )

            # Refresh the metadata View to Reflect Updates
            log.info(
                "---- ---- Refreshing 'Security._Metadata' Materialized View ---- ----"
            )
            cursor.execute(self[Op.REFRESH, AssetTbls._METADATA]())

        # Reset the mdata memory just in case
        del self._altered_tables
        del self._altered_tables_mdata

    def refresh_all_aggregate_metadata(self):
        """
        Refresh All Continuous Aggregates.
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

            log.info(
                "---- ---- Refreshing 'Security._Metadata' Materialized View ---- ----"
            )
            cursor.execute(self[Op.REFRESH, AssetTbls._METADATA]())

    def _manual_refresh_loop(self, cursor: TupleCursor, method: str):
        "Inner function that can return but still allow the cursor to refresh the MetaData Table."
        for schema, config in self.table_config.items():
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
                cursor.execute(
                    self[Op.REFRESH, SeriesTbls.CONTINUOUS_AGG](schema, table)
                )

    # endregion

    # region ---- ---- ---- Private Timeseries Sub-routines ---- ---- ----

    def _configure_timeseries_schema(
        self,
        cursor: TupleCursor,
        schema: Schema,
        config: TimeseriesConfig,
    ):
        "Script to Make Changes to the configuration of stored Timeseries Data"
        cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA_TABLES](schema))
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
            for tbl in config.raw_tables(asset):
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
                ref_table = config.get_cont_agg_source(tbl)
                tbl_type = (
                    SeriesTbls.CONTINUOUS_TICK_AGG
                    if ref_table.period == Timedelta(0)
                    else SeriesTbls.CONTINUOUS_AGG
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
                self[Op.UPDATE, SeriesTbls._ORIGIN](schema, asset, **origin_args)
            )

            # Remove All Calculated Data Tables
            log.info("Updating config for Asset: %s", asset)

            # Must Remove Longest Aggregates first.
            all_aggregates = stored_config.all_tables(asset, include_raw=False)
            all_aggregates.sort(key=lambda x: x.period, reverse=True)
            for tbl in all_aggregates:
                log.info("Dropping Table: %s", tbl.table_name)
                cursor.execute(self[Op.DROP, GenericTbls.VIEW](schema, tbl.table_name))

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
                cursor.execute(self[Op.DROP, GenericTbls.TABLE](schema, tbl.table_name))

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
                ref_table = config.get_cont_agg_source(tbl)
                tbl_type = (
                    SeriesTbls.CONTINUOUS_TICK_AGG
                    if ref_table.period == Timedelta(0)
                    else SeriesTbls.CONTINUOUS_AGG
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
                tbl_type = GenericTbls.TABLE if tbl.raw else GenericTbls.VIEW
                cursor.execute(cmd := self[Op.DROP, tbl_type](schema, tbl.table_name))
                log.debug(cmd.as_string())

    def _all_symbol_metadata(
        self, pkey: int, asset_class: str, schema: Schema
    ) -> list[MetadataInfo]:
        """
        Returns a list of all Metadata for a given pkey, asset class, and schema.
        Returned list is guerenteed to have both metadata that is stored and that should be stored.
        """
        stored_metadata = self.stored_symbol_metadata(
            pkey, {"schema_name": schema, "is_raw_data": True}
        )
        req_tables = self.table_config[schema].raw_tables(asset_class)
        stored_tables = [mdata.table for mdata in stored_metadata]
        # As long as the hash of an AssetTable is a string this will work.
        missing_tables = set(req_tables).difference(stored_tables)
        missing_metadata = [
            MetadataInfo(
                table.table_name,
                schema,
                Timestamp("1800-01-01"),  # Default values
                Timestamp("1800-01-01"),
                table,
            )
            for table in missing_tables
        ]

        stored_metadata.extend(missing_metadata)
        return stored_metadata

    def _update_series_data_edit_record(
        self, metadata: MetadataInfo, data: DataFrame, table: AssetTable
    ):
        # Ensure records exist in this instance
        if not hasattr(self, "_altered_tables"):
            # pylint: disable=attribute-defined-outside-init
            self._altered_tables: dict[Schema | str, set[str]] = {}
            self._altered_tables_mdata: dict[Schema | str, dict[str, MetadataInfo]] = {}

        # Ensure the schema key exists in both dicts
        if metadata.schema_name not in self._altered_tables:
            self._altered_tables[metadata.schema_name] = set()
            self._altered_tables_mdata[metadata.schema_name] = {}

        # Update / Add the Necessary Metadata.
        if table in self._altered_tables[metadata.schema_name]:
            # Join the metadata keeping track of the full data-range edited
            mdata = self._altered_tables_mdata[metadata.schema_name][table.table_name]
            mdata.start_date = min(mdata.start_date, data.iloc[0]["dt"])
            mdata.end_date = max(mdata.end_date, data.iloc[-1]["dt"])
            self._altered_tables_mdata[metadata.schema_name][table.table_name] = mdata

        else:
            # Construct a new metadata instance to add to the records
            # Start / End Dates represent ranges that were updated/inserted
            mdata = MetadataInfo(
                table.table_name,
                metadata.schema_name,
                data.iloc[0]["dt"],
                data.iloc[-1]["dt"],
                table,
            )
            self._altered_tables[metadata.schema_name].add(table.table_name)
            self._altered_tables_mdata[metadata.schema_name][table.table_name] = mdata

    # endregion
