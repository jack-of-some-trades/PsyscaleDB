"Series Data Partial Class Functions"

from enum import StrEnum
from io import BytesIO
import logging
from typing import Iterable, Literal, Optional

from pandas import DataFrame, DatetimeIndex, Timedelta, Timestamp
import pandas

from psyscale.async_core import PsyscaleAsyncCore
from psyscale.psql.timeseries import AGGREGATE_ARGS, TICK_ARGS, AggregateArgs, TickArgs
from .series_df import Series_DF
from .psql import (
    MetadataInfo,
    Operation as Op,
    Schema,
    AssetTbls,
    SeriesTbls,
    AssetTable,
    GenericTbls,
)

from .core import PsyscaleCore

log = logging.getLogger("psyscale_log")

# pylint: disable='protected-access'


class SeriesDataPartial(PsyscaleCore):
    "Series Data Upsert and Fetch Functions"

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
        schema: Optional[str | StrEnum] = None,
        asset_class: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """
        Fetch Series Data from the Database Aggregating the desired data as needed.

        -- PARAMS --
        - symbol : Int | Str
            - Symbol (str) or Primary Key (int) of the Symbol to Fetch (Case is ignored)
        - timeframe : pandas.Timedelta
            - Interval of the Data to Return. Doesn't not need to be a value stored in the
            database, merely one that can be derived from stored data.
            - Timedelta(0) will return Tick Data if it is stored for the given pkey
        - start : Optional pandas.Timestamp : Earliest Date of Data to Retrieve
        - end : Optional pandas.Timestamp : Latest Date of Data to Retrieve
        - limit : Optional Int : Maximum number of data points to return
        - rth : bool
            - When True, Return RTH Hours only
            - When False, Return All stored data, RTH/ETH/Closed/Breaks, etc
        - rtn_args : Optional list of arguments to return.
            - Default = {"dt", "open", "high", "low", "close", "volume", "price"}
            - Unknown args are ignored
            - Note: 'dt' will always be returned even if not included.

        - schema : Optional str | strEnum
            - Data Schema to fetch the data from.
        - asset_class : Optional str
            - Asset_class of the symbol requested
                - Note: Both asset_class & schema will be determined from the pkey/symbol if
                necessary. However, if both are already known and given as args that information
                will be used in place of querying the database for it.
        """

        # Search Metadata for available data at TF or lower
        with self._cursor(dict_cursor=True) as cursor:

            # region -- Fetch Asset Class & Schema & Pkey if necessary --
            if asset_class is None or schema is None or isinstance(symbol, str):
                if isinstance(symbol, str):
                    _filters = [("symbol", "ILIKE", symbol)]
                else:
                    _filters = [("pkey", "=", symbol)]
                cursor.execute(
                    self[Op.SELECT, GenericTbls.TABLE](
                        Schema.SECURITY,
                        AssetTbls.SYMBOLS,
                        [
                            "pkey",
                            "asset_class",
                            "store_tick",
                            "store_minute",
                            "store_aggregate",
                        ],
                        _filters,
                        limit,
                    )
                )
                rsp = cursor.fetchall()
                if len(rsp) == 0:
                    log.warning("Unknown key : %s", symbol)
                    return
                rsp = rsp[0]

                if rsp["store_minute"]:
                    schema = Schema.MINUTE_DATA
                elif rsp["store_aggregate"]:
                    schema = Schema.AGGREGATE_DATA
                elif rsp["store_tick"]:
                    schema = Schema.TICK_DATA
                else:
                    log.warning("Symbol : %s is known, but not stored", symbol)
                    return

                pkey = rsp["pkey"]
                asset_class = rsp["asset_class"]
                assert asset_class
            else:
                # Given schema, asset_class & integer pkey
                pkey = symbol
            # endregion

            desired_table = AssetTable(asset_class, timeframe, False, False, rth)
            try:
                src_table, need_to_calc = self._table_config[
                    Schema(schema)
                ].get_selection_source_table(desired_table)
            except AttributeError:
                log.warning(
                    "Cannot Aggregate timeframe %s for symbol %s from the data in the database",
                    timeframe,
                    symbol,
                )
                return None

            # Configure return args as a set
            if rtn_args is None:
                _rtns = {"dt", "open", "high", "low", "close", "volume", "price"}
            else:
                # sql formatting functions remove excess/ unkown args
                _rtns = {*rtn_args}

            if need_to_calc:
                log.info(
                    "Calculating Aggregate at Timeframe : %s, from %s Timeframe",
                    timeframe,
                    src_table.period,
                )
                cmd = self[Op.COPY, SeriesTbls.CALCULATE_AGGREGATE](
                    schema, src_table, timeframe, pkey, rth, start, end, limit, _rtns
                )
            else:
                # Works for both Aggregates and Raw Tick Data Retrieval
                cmd = self[Op.COPY, SeriesTbls.RAW_AGGREGATE](
                    schema, src_table, pkey, rth, start, end, limit, _rtns
                )

            buffer = BytesIO()
            with cursor.copy(cmd) as copy:
                buffer.writelines(copy)
            return _bytes_to_df(buffer)

    def upsert_series(
        self,
        pkey: int,
        metadata: MetadataInfo,
        data: DataFrame,
        exchange: Optional[str] = None,
        *,
        on_conflict: Literal["update", "error", "ignore"] = "ignore",
    ):
        """
        Insert or Upsert symbol data to the database.

        -- PARAMS --
        - pkey : int. Primary Key of the symbol to insert
        - metadata: MetadataInfo Object
            - Contains the schema_name & table_name to insert the data into, can be retrieved from
            calling 'symbol_series_metadata'
        - Data: Dataframe.
            - Should contain all series data needed to be inserted. Multiple names will be
            recognized for each given series parameter.
            - i.e. time/datetime/date/dt ... etc will all be recognized as the timestamp column.
        - exchange : str | None
            - Exchange that the Asset is traded on. This will be passed to pandas_market_calendars
            so the RTH/ETH session of each datapoint can be determined and stored as necessary.
            - None can be passed for 24/7 Exchanges such as Crypto. (Note: Forex would require 24/5 be passed)
        - on_conflict : Literal["update", "error", "ignore"] = "error"
            - Action to take when a UNIQUE conflict occurs. Erroring allows for faster insertion
            if it can be ensured that given data will be unique
        """
        table = (
            metadata.table
            if metadata.table is not None
            else AssetTable.from_table_name(metadata.table_name)
        )

        data = _configure_and_check_df(data, exchange, table)

        if on_conflict == "ignore":
            data = _filter_redundant_datapoints(data, metadata)
            if len(data) == 0:
                log.warning(
                    "Upsert_Symbol_Data() given only redundant data points and set to ignore.\n"
                    'No Data is being inserted. Set on_conflict="update" to edit existing data.'
                )
                return

        # Setup and copy data into database
        buffer_tbl_type = (
            SeriesTbls.TICK_BUFFER
            if table.period == Timedelta(0)
            else SeriesTbls.RAW_AGG_BUFFER
        )

        # Inject the data through a temporary table
        with self._cursor(raise_err=True) as cursor:
            # Create & Inject the Data into a Temporary Table
            cursor.execute(self[Op.CREATE, buffer_tbl_type](table))
            copy_cmd = self[Op.COPY, buffer_tbl_type](
                # Sends the COPY Cmd & the order of the Columns of the Dataframe
                [str(c) for c in data.columns]
            )
            with cursor.copy(copy_cmd) as copy:
                for row in data.itertuples(index=False, name=None):
                    # Writes each row as a Tuple that matches the Dataframe Column Order
                    copy.write_row(row)

            # Merge the Temp Table By inserting / upserting from the Temporary Table
            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            cursor.execute(
                self[_op, buffer_tbl_type](metadata.schema_name, table, pkey)
            )
            log.info("Symbol Data Upsert Status Message: %s", cursor.statusmessage)

        self._update_series_data_edit_record(metadata, data, table)

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
                    cont_aggs = self._table_config[
                        Schema(schema)
                    ].get_tables_to_refresh(mdata.table)
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


class AsyncSeriesDataPartial(PsyscaleAsyncCore, SeriesDataPartial):
    "Async extension for Series Data Upsert and Fetch Functions"

    async def upsert_series_async(
        self,
        pkey: int,
        metadata: MetadataInfo,
        data: DataFrame,
        exchange: Optional[str] = None,
        *,
        on_conflict: Literal["update", "error", "ignore"] = "ignore",
    ):
        "See upsert_series() Docstring"
        table = (
            metadata.table
            if metadata.table is not None
            else AssetTable.from_table_name(metadata.table_name)
        )

        data = _configure_and_check_df(data, exchange, table)

        if on_conflict == "ignore":
            data = _filter_redundant_datapoints(data, metadata)
            if len(data) == 0:
                log.warning(
                    "Upsert_Symbol_Data() given only redundant data points and set to ignore.\n"
                    'No Data is being inserted. Set on_conflict="update" to edit existing data.'
                )
                return

        buffer_tbl_type = (
            SeriesTbls.TICK_BUFFER
            if table.period == Timedelta(0)
            else SeriesTbls.RAW_AGG_BUFFER
        )

        # Inject the data through a temporary table
        async with self._acursor(raise_err=True) as cursor:
            await cursor.execute(self[Op.CREATE, buffer_tbl_type](table))
            copy_cmd = self[Op.COPY, buffer_tbl_type](
                # Sends the COPY Cmd & the order of the Columns of the Dataframe
                [str(c) for c in data.columns]
            )
            async with cursor.copy(copy_cmd) as copy:
                for row in data.itertuples(index=False):
                    # You'd think that calling await millions of times
                    # would produce a lot of overhead.... somehow it doesn't
                    await copy.write_row(row)

            # Merge the Temp Table By inserting / upserting from the Temporary Table
            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            await cursor.execute(
                self[_op, buffer_tbl_type](metadata.schema_name, table, pkey)
            )
            log.info("Symbol Data Upsert Status Message: %s", cursor.statusmessage)

        self._update_series_data_edit_record(metadata, data, table)

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
        schema: Optional[str | StrEnum] = None,
        asset_class: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """
        Fetch Series Data from the Database Aggregating the desired data as needed.

        CAVEAT: While this does work and is Asynchronous, there is no way to decouple
        the Select Query & the COPY FROM operation. While the former would benefit
        from an await, awaiting the latter slows down the data retrieval a lot.
        Overall this function takes about 60% longer than the synchronous version.

        -- PARAMS --
        - symbol : Int | Str
            - Symbol (str) or Primary Key (int) of the Symbol to Fetch (Case is ignored)
        - timeframe : pandas.Timedelta
            - Interval of the Data to Return. Doesn't not need to be a value stored in the
            database, merely one that can be derived from stored data.
            - Timedelta(0) will return Tick Data if it is stored for the given pkey
        - start : Optional pandas.Timestamp : Earliest Date of Data to Retrieve
        - end : Optional pandas.Timestamp : Latest Date of Data to Retrieve
        - limit : Optional Int : Maximum number of data points to return
        - rth : bool
            - When True, Return RTH Hours only
            - When False, Return All stored data, RTH/ETH/Closed/Breaks, etc
        - rtn_args : Optional list of arguments to return.
            - Default = {"dt", "open", "high", "low", "close", "volume", "price"}
            - Unknown args are ignored
            - Note: 'dt' will always be returned even if not included.

        - schema : Optional str | strEnum
            - Data Schema to fetch the data from.
        - asset_class : Optional str
            - Asset_class of the symbol requested
                - Note: Both asset_class & schema will be determined from the pkey/symbol if
                necessary. However, if both are already known and given as args that information
                will be used in place of querying the database for it.
        """

        # Search Metadata for available data at TF or lower
        async with self._acursor(dict_cursor=True) as cursor:

            # region -- Fetch Asset Class & Schema & Pkey if necessary --
            if asset_class is None or schema is None or isinstance(symbol, str):
                if isinstance(symbol, str):
                    _filters = [("symbol", "ILIKE", symbol)]
                else:
                    _filters = [("pkey", "=", symbol)]
                await cursor.execute(
                    self[Op.SELECT, GenericTbls.TABLE](
                        Schema.SECURITY,
                        AssetTbls.SYMBOLS,
                        [
                            "pkey",
                            "asset_class",
                            "store_tick",
                            "store_minute",
                            "store_aggregate",
                        ],
                        _filters,
                        limit,
                    )
                )
                rsp = await cursor.fetchall()
                if len(rsp) == 0:
                    log.warning("Unknown key : %s", symbol)
                    return
                rsp = rsp[0]

                if rsp["store_minute"]:
                    schema = Schema.MINUTE_DATA
                elif rsp["store_aggregate"]:
                    schema = Schema.AGGREGATE_DATA
                elif rsp["store_tick"]:
                    schema = Schema.TICK_DATA
                else:
                    log.warning("Symbol : %s is known, but not stored", symbol)
                    return

                pkey = rsp["pkey"]
                asset_class = rsp["asset_class"]
                assert asset_class
            else:
                # Given schema, asset_class & integer pkey
                pkey = symbol
            # endregion

            desired_table = AssetTable(asset_class, timeframe, False, False, rth)
            try:
                src_table, need_to_calc = self._table_config[
                    Schema(schema)
                ].get_selection_source_table(desired_table)
            except AttributeError:
                log.warning(
                    "Cannot Aggregate timeframe %s for symbol %s from the data in the database",
                    timeframe,
                    symbol,
                )
                return None

            # Configure return args as a set
            if rtn_args is None:
                _rtns = {"dt", "open", "high", "low", "close", "volume", "price"}
            else:
                # sql formatting functions remove excess/ unkown args
                _rtns = {*rtn_args}

            if need_to_calc:
                log.info(
                    "Calculating Aggregate at Timeframe : %s, from %s Timeframe",
                    timeframe,
                    src_table.period,
                )
                cmd = self[Op.COPY, SeriesTbls.CALCULATE_AGGREGATE](
                    schema, src_table, timeframe, pkey, rth, start, end, limit, _rtns
                )
            else:
                # Works for both Aggregates and Raw Tick Data Retrieval
                cmd = self[Op.COPY, SeriesTbls.RAW_AGGREGATE](
                    schema, src_table, pkey, rth, start, end, limit, _rtns
                )

            buffer = BytesIO()
            async with cursor.copy(cmd) as copy:
                async for line in copy:
                    buffer.write(line)
            return _bytes_to_df(buffer)

    async def refresh_aggregate_metadata_async(self):
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

        async with self._acursor(auto_commit=True) as cursor:

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
                    cont_aggs = self._table_config[
                        Schema(schema)
                    ].get_tables_to_refresh(mdata.table)
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
                        await cursor.execute(
                            self[Op.REFRESH, SeriesTbls.CONTINUOUS_AGG](
                                schema, table, mdata.start_date, mdata.end_date
                            )
                        )

            # Refresh the metadata View to Reflect Updates
            log.info(
                "---- ---- Refreshing 'Security._Metadata' Materialized View ---- ----"
            )
            await cursor.execute(self[Op.REFRESH, AssetTbls._METADATA]())

        # Reset the mdata memory just in case
        del self._altered_tables
        del self._altered_tables_mdata


def _configure_and_check_df(
    data: DataFrame, exchange: str | None, table: AssetTable
) -> DataFrame:
    # region ---- Check that the data matches name and 'NOT NULL' expectations
    series_df = Series_DF(data.copy(), exchange)  # Rename cols & Populate 'rth'

    try:
        if table.period != Timedelta(0):
            # Aggregate Specific Expectations
            assert table.period == series_df.timedelta
            assert "close" in series_df.columns
            assert not series_df.df["close"].isna().any()
            extra_cols = set(series_df.df.columns).difference(AGGREGATE_ARGS)
        else:
            # Tick Specific Expectations
            assert "price" in series_df.columns
            assert not series_df.df["price"].isna().any()
            extra_cols = set(series_df.df.columns).difference(TICK_ARGS)

        if len(extra_cols) > 0:
            series_df.df.drop(columns=extra_cols, inplace=True)
            log.debug("Ignoring extra columns in dataframe: %s", extra_cols)

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

    return series_df.df

    # endregion


def _filter_redundant_datapoints(data: DataFrame, metadata: MetadataInfo) -> DataFrame:
    # When inserting ensure only the range that needs to be added is.
    # Pretty common there's 1 extra data point at the start of a df.
    before_start = data["dt"] < metadata.start_date
    after_end = data["dt"] > metadata.end_date
    dt_filter = before_start | after_end

    if not dt_filter.all():
        extra_data = data[~dt_filter]
        log.debug(
            "Given %s extra data point(s), dropping the following :\n %s",
            len(extra_data),
            extra_data,
        )
        data = data[dt_filter]

    return data


def _bytes_to_df(buffer: BytesIO) -> DataFrame | None:
    buffer.seek(0)
    _rtn = pandas.read_csv(buffer)
    if len(_rtn) == 0:
        return None

    # Cursed? yes. Faster? Also yes.
    if _rtn["dt"].dtype == "int64":
        _rtn["dt"] = (_rtn["dt"] * 1e9).astype("datetime64[ns, UTC]")  # type: ignore
        # The proper way to do it...
        # _rtn["dt"] = pandas.to_datetime(_rtn["dt"], utc=True)
    else:
        _rtn["dt"] = DatetimeIndex(_rtn["dt"])
        # When returning tick data a timestamp is returned as a string.
        # its much slower, but retains the sub-second timestamp resolution
    return _rtn
