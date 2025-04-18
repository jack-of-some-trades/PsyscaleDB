"""An interface for reading and commiting data to a database"""

from enum import StrEnum
from itertools import chain
import logging
import os
import subprocess
from pathlib import Path
from inspect import stack
from contextlib import contextmanager
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Any,
    Tuple,
    TypeAlias,
    overload,
)
from pandas import DataFrame, Timedelta, Timestamp

import psycopg as pg
import psycopg.rows as pg_rows
from psycopg import OperationalError, sql
from psycopg.pq._enums import ExecStatus
from psycopg_pool import ConnectionPool, PoolTimeout

from .psql import (
    SYMBOL_ARGS,
    METADATA_ARGS,
    STRICT_SYMBOL_ARGS,
    TickArgs,
    SymbolArgs,
    AggregateArgs,
    MetadataArgs,
    MetadataInfo,
    GenericTbls,
    Operation as Op,
    Schema,
    AssetTbls,
    SeriesTbls,
    Commands,
    AssetTable,
    TimeseriesConfig,
)


# region ----------- Database Structures  -----------

log = logging.getLogger("psyscale_log")

# Get the Timescale.yml in the folder this file is stored in.
DEFAULT_YML_PATH = Path(__file__).parent.joinpath("timescale.yml").as_posix()
TIMESCALE_IMAGE = "timescale/timescaledb-ha:pg17"
POOL_GEN_TIMEOUT = 5  # seconds to wait for the connection pool to be generated
LOCAL_POOL_GEN_TIMEOUT = 0.2  # wait time when starting a local Connection Pool
# endregion
# pylint: disable='protected-access'

DictCursor: TypeAlias = pg.Cursor[pg_rows.DictRow]
TupleCursor: TypeAlias = pg.Cursor[pg_rows.TupleRow]


class PsyscaleDB:
    """
    A Python client interface for connecting to a PostgreSQL Database in a (mostly) read-only mode.

    Timescale DB Docker self-host instructions
    https://docs.timescale.com/self-hosted/latest/install/installation-docker/

    This class contains all the necessary functionality needed to interact
    with the Database at runtime. If provided environment variables that point to
    a local host, the initializer will start/create the docker container as needed.

    This class instance doesn't strictly prohibit write (Insert/Update/Delete) commands. However,
    it lacks functionality to streamline the symbol and data insertion process. These additional
    functions are accessible through the PsyscaleMod sub-class.

    Additional functionality (Such as one-off configuration scripts, Data Insertion, etc.)
    are handled by the PsyscaleMod Subclass. This is done to de-clutter the
    exceedingly large amount of functionality that is needed to manage a Database.
    """

    # This division of labor is done for two reasons.
    # 1: Separates & Organizes the large amt. of functions so it's not all in a single class
    # 2: It lazy loads all of that extra functionality, most notably, pandas_market_calendars.

    def __init__(
        self,
        *,
        docker_compose_fpath: Optional[str] = None,
    ):
        env_params: dict[str, Any] = {
            "host": os.getenv("PSYSCALE_HOST"),
            "port": os.getenv("PSYSCALE_PORT"),
            "user": os.getenv("PSYSCALE_USER"),
            "dbname": os.getenv("PSYSCALE_DB_NAME"),
            "password": os.getenv("PSYSCALE_PASSWORD"),
        }

        if env_params["host"] in {"localhost", "127.0.0.1"}:
            # Get additional params if using a local database
            env_params["volume_path"] = os.getenv("PSYSCALE_VOLUME_PATH")

        missing_keys = {key for key, value in env_params.items() if value is None}
        if len(missing_keys) > 0:
            raise AttributeError(
                f"Cannot instantiate PsyscaleDB. Missing Environment Variables: {missing_keys} \n"
            )

        self.conn_params: dict[str, Any] = {
            k: env_params[k] for k in ("user", "password", "dbname", "host", "port")
        }

        _local = self.conn_params["host"] in {"localhost", "127.0.0.1"}
        _timeout = LOCAL_POOL_GEN_TIMEOUT if _local else POOL_GEN_TIMEOUT

        try:
            self.pool = ConnectionPool(
                kwargs=self.conn_params, open=False, timeout=_timeout
            )
            self.pool.open(timeout=_timeout)
            log.debug("Health_check: %s", "good" if self._health_check() else "bad")
        except PoolTimeout as e:
            if not _local:
                raise e  # Give some more informative info here?

            # Try and start the local database, give extra buffer on the timeout.
            self._init_and_start_localdb(
                docker_compose_fpath, env_params["volume_path"]
            )
            with self.pool.connection(timeout=2.5) as conn:
                conn._check_connection_ok()

        except OperationalError as e:
            raise e  # Give some more informative info here?

        self.cmds = Commands()
        self._ensure_securities_schema_format()
        self._read_db_timeseries_config()

    def __del__(self):
        "DEL overload to stop the Docker Container so it isn't always running."
        #     if (
        #         not self.__stopdb_on_shutdown__
        #         or self.config["host"] not in {"localhost", "127.0.0.1"}
        #         or getattr(self, "yml_path", None) is None
        #     ):
        #         return

        #     subprocess.run(
        #         ["docker-compose", "-f", self.yml_path, "stop"],
        #         stdout=subprocess.DEVNULL,  # Capturing output can cause process to hang
        #         stderr=subprocess.DEVNULL,
        #         check=False,
        #     )

    def __getitem__(self, args: Tuple[Op, StrEnum]) -> Callable[..., sql.Composed]:
        "Accessor forwarder for the self.cmds object"
        return self.cmds[args]

    # region ----------- Connection & Cursor Methods -----------

    def _health_check(self) -> bool:
        "Simple Ping to the Database to ensure it is alive"
        with self._cursor() as cursor:
            cursor.execute("SELECT 1")
            rsp = cursor.fetchall()
            return rsp[0][0] == 1
        return False

    # region -- Cursor Overloading ---
    @overload
    @contextmanager
    def _cursor(
        self,
        dict_cursor: Literal[True],
        *,
        pipeline: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[DictCursor]: ...
    @overload
    @contextmanager
    def _cursor(
        self,
        dict_cursor: Literal[False] = False,
        *,
        pipeline: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[TupleCursor]: ...
    @overload
    @contextmanager
    def _cursor(
        self,
        dict_cursor: bool = False,
        *,
        pipeline: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[TupleCursor]: ...

    # endregion

    @contextmanager
    def _cursor(
        self,
        dict_cursor: bool = False,
        *,
        pipeline: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[TupleCursor] | Iterator[DictCursor]:
        """
        Returns a cursor to execute commands in a database.

        Default return product is a list of tuples. Returns can be made into lists of dictionaries
        by settign dict_cursor=True. This is less performant for large datasets though.

        Auto_Commit Allows for commands to be done outside of a transaction block which may be
        required for some commands.

        Pipeline is a feature of a cursor, that when set to True, avoids waiting for responses
        before executing new commands. In theory that should increase performance. In practice
        it seemed to only reduce performance.
        """
        cursor_factory = pg_rows.dict_row if dict_cursor else pg_rows.tuple_row
        conn: pg.Connection = self.pool.getconn()

        if auto_commit:
            conn.set_autocommit(True)

        try:
            if pipeline:
                with conn.pipeline(), conn.cursor(row_factory=cursor_factory) as cursor:
                    yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
            else:
                with conn, conn.cursor(row_factory=cursor_factory) as cursor:
                    yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
        except pg.DatabaseError as e:
            conn.rollback()  # Reset Database, InFailedSqlTransaction Err thrown if not reset
            log.error("Caught Database Error: \n '%s' \n...Rolling back changes.", e)
        finally:
            if auto_commit:
                conn.set_autocommit(False)
            else:
                conn.commit()

            self.pool.putconn(conn)

    @overload
    def execute(
        self,
        operation: Op,
        table: StrEnum,
        fmt_args: Mapping[str, Any] = {},
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: Literal[False] = False,
    ) -> Tuple[List[Tuple], str]: ...
    @overload
    def execute(
        self,
        operation: Op,
        table: StrEnum,
        fmt_args: Mapping[str, Any] = {},
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: Literal[True] = True,
    ) -> Tuple[List[Dict], str]: ...

    def execute(
        self,
        operation: Op,
        table: StrEnum,
        fmt_args: Mapping[str, Any] = {},
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: bool = False,
    ) -> Tuple[List[Dict] | List[Tuple], str]:
        "Execution Method to manually invoke a command within the database."

        if operation not in self.cmds.operation_map:
            log.error("Unknown Operation: %s", operation)
            return [], "CMD_ERROR"
        if table not in self.cmds.operation_map[operation]:
            log.error("Unknown Operation Table Pair: %s:%s", operation, table)
            return [], "CMD_ERROR"

        cmd = self[operation, table](**fmt_args)
        with self._cursor(dict_cursor) as cursor:
            try:
                log.debug("Executing PSQL Command: %s", cmd.as_string(cursor))
                cursor.execute(cmd, exec_args)

            except pg.DatabaseError as e:
                log.error(
                    "Cursor Execute Exception (%s) occured in '%s' \n  Exception Msg: %s",
                    e.__class__.__name__,
                    stack()[1].function,
                    e,
                )
                return [], str(cursor.statusmessage)

            response = []
            pgr = cursor.pgresult
            # Really sad I have to dig to check if there is data available.
            if pgr is not None and pgr.status == ExecStatus.TUPLES_OK:
                response = cursor.fetchall()

            return response, str(cursor.statusmessage)

    # endregion

    # region ----------- Private Database Interaction Methods -----------

    def _init_and_start_localdb(
        self, docker_compose_fpath: Optional[str], vol_path: str | None
    ):
        "Starts Up, via subprocess terminal cmds, a local Docker Container that runs TimescaleDB"
        try:  # Ensure Docker is installed
            subprocess.run(["docker", "--version"], capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            raise OSError(
                "Cannot Initialize Local PsyscaleDB, OS does not have docker installed."
            ) from e

        # Ensure Timescale Image has been pulled.
        try:
            p = subprocess.run(
                ["docker", "images", TIMESCALE_IMAGE],
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise OSError(
                "Docker Images Command Failed, Ensure Docker Engine is Running"
            ) from e

        # The following check may only work on windows...
        if len(p.stdout.decode().strip().split("\n")) <= 1:
            # i.e. STDOut only returned table heading and no rows listing available images.
            log.warning(
                "Missing required TimescaleDB Image. Pulling Docker Image: %s",
                TIMESCALE_IMAGE,
            )
            try:
                p = subprocess.run(
                    ["docker", "pull", TIMESCALE_IMAGE],
                    capture_output=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise OSError("Could not pull Docker Image.") from e
            log.info("Successfully pulled Docker Image")

        if vol_path and not os.path.exists(vol_path):
            log.info("Making Database Volume Folder at : %s", vol_path)
            os.mkdir(vol_path)

        if docker_compose_fpath is not None:
            # Overwrite Default YML path if given a valid filepath
            if not (
                os.path.isfile(docker_compose_fpath)
                and docker_compose_fpath.lower().endswith((".yaml", ".yml"))
            ):
                raise ValueError(f"{docker_compose_fpath = } must be a .yaml/.yml File")
            self.yml_path = docker_compose_fpath
        else:
            # Use Default Docker_Compose Config
            self.yml_path = DEFAULT_YML_PATH

        p = subprocess.run(  # Unfortunately this is the slowest command @ around 0.4s
            ["docker-compose", "-f", self.yml_path, "up", "-d"],
            capture_output=True,
            check=False,
        )

        if p.returncode != 0:
            raise OSError(
                f"Failed to start Docker-Compose with Err Msg: {p.stderr.decode()}"
            )

    def _read_db_timeseries_config(self):
        "Read off the TimeseriesConfig for each schema by probing all the table names."
        self.table_config: Dict[Schema, TimeseriesConfig] = {}

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
                self.table_config[schema] = config

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
        all_assets = {chain(map(lambda x: x.asset_classes, self.table_config.values()))}
        if len(all_assets) == 0:
            log.warning(
                "No Asset Types Detected in the Database. To Initialize the Database call "
                "TimescaleDBEXT__configure_db_format__() with the appropriate arguments.\n"
                "See timescale_ext.py for necessary class extention and an Example Configuration."
            )

    def _ensure_securities_schema_format(self):
        with self._cursor() as cursor:
            cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA_TABLES](Schema.SECURITY))
            tables: set[str] = {rsp[0] for rsp in cursor.fetchall()}

            if AssetTbls.SYMBOLS not in tables:
                # Init Symbols Table & pg_trgm Text Search Functions
                log.info("Creating Table '%s'.'%s'", Schema.SECURITY, AssetTbls.SYMBOLS)
                cursor.execute(self[Op.CREATE, AssetTbls._SYMBOL_SEARCH_FUNCS]())
                cursor.execute(self[Op.CREATE, AssetTbls.SYMBOLS]())

            # Init Symbol Data Range Metadata table & support function
            log.debug(
                "Ensuring Table '%s'.'%s' Exists",
                Schema.SECURITY,
                AssetTbls._METADATA,
            )
            cursor.execute(self[Op.CREATE, AssetTbls._METADATA_FUNC]())
            cursor.execute(self[Op.CREATE, AssetTbls._METADATA]())

    # endregion

    # region ----------- Public Database Interaction Methods -----------

    def search_symbols(
        self,
        filter_args: dict[SymbolArgs | str, Any],
        return_attrs: bool = False,
        attrs_search: bool = False,
        limit: int | None = 100,
        *,
        strict_symbol_search: bool | Literal["ILIKE", "LIKE", "="] = False,
    ) -> list[dict]:
        """
        Search the database's symbols table returning all the symbols that match the given criteria.
        Search function supports trigram based fuzzy name + symbol search.

        -- PARAMS --
        - filter_args: dict[SymbolArgs | str : Any]
            - The filtering arguments that need to be matched against. By default only the keys that
            match the table column names (SymbolArgs Literal, e.g. pkey, name, symbol, etc.) will be
            used.
            - All Arguments aside from 'name' and 'symbol' will be used in a strict '=' comparison
            filter. 'name' will always be used in a fuzzystr trigram search where the results are
            ordered by relevancy. by default, 'symbol' will also be a fuzzystr trigram search.
            - When a 'pkey' filter is given, all other filter keys are ignored and the table is
            searched for the given integer pkey. This is because, by table definition, the pkey
            will be unique and can only ever return a single row.
            - Additional argument keys  that are not column names of the table (Not in SymbolArgs
            Literal) will be ignored by default. See attrs_search for more on this behavior.

        - return_attrs: boolean.
            - When True return an 'attrs' dictionary that has all additional attributes of the
            symbol that are stored in the 'attrs' column of the table.

        - attrs_search: boolean.
            - When True any additional keys that are given as filters args, but not recognized as
            table columns, will be used in a strict search against that 'attrs' JSON Column of the
            table.
            - When False additional keys within the filter_args are ignored.
            - i.e. when true, if {'shortable':True} is passed in filter_args then only rows that
            have a defined 'shortable'= True Attrubute will be returned.
            - This search will only ever be a strict '=' comparison, so if searching for a string
            or int the value given must be exact.

        - limit: int
            - The Optional Integer limit of symbol results to return.

        - strict_symbol_search: Boolean | Literal["ILIKE", "LIKE", "="] : default False.
            - When False a fuzzystr trigram search is used and the results are ordered by relevancy.
            Even if an exact match for the symbol is returned, this will still result in other
            similar symbols being returned.
            - When not False the symbol search will use the given PostgreSQL comparator. True
            equates to passing 'ILIKE' Which ignores case.
            - This is far more useful when passing a symbol with wildcard args. e.g.
            'ILIKE' + {symbol:'sp'} will likely not return results, 'ILIKE' + {symbol:'sp%'}
            will return all symbols starting with 'SP', case insensitive.
        """

        if "pkey" in filter_args:
            # Fast Track PKEY Searches since there will only ever be 1 symbol returned
            filters = [("pkey", "=", filter_args["pkey"])]
            name, symbol, attrs, limit = None, None, None, 1
        else:
            filters = [
                (k, "=", v) for k, v in filter_args.items() if k in STRICT_SYMBOL_ARGS
            ]

            name = filter_args.get("name", None)
            symbol = filter_args.get("symbol", None)

            if strict_symbol_search and symbol is not None:
                # Determine if symbol is passed as a strict or fuzzy parameter
                compare_method = (
                    strict_symbol_search
                    if isinstance(strict_symbol_search, str)
                    else "ILIKE"  # Default search for similar symbols that match ignoring case.
                )
                filters.append(("symbol", compare_method, symbol))
                symbol = None  # Prevents the 'similarity' search from being added

            # Filter all extra given filter params into a separate dict
            attrs = (
                dict((k, v) for k, v in filter_args.items() if k not in SYMBOL_ARGS)
                if attrs_search
                else None
            )
            if attrs and len(attrs) == 0:
                attrs = None

        with self._cursor(dict_cursor=True) as cursor:
            cursor.execute(
                self[Op.SELECT, AssetTbls.SYMBOLS](
                    name, symbol, filters, return_attrs, attrs, limit
                )
            )
            return cursor.fetchall()

    def update_symbol(
        self, pkey: int | list[int], args: dict[SymbolArgs | str, Any]
    ) -> bool:
        """
        Update a Single Symbol or list of symbols, by primary key, with the given arguments.

        This method is remained general for utility purposes. It should generally just be used to
        update the stored_[tick/minute/aggregate] columns. All Other parameters should remain
        constant by nature. To insert symbols see the API_Extension that allows this to be done
        in bulk.

        Note: Setting any stored column = False does not Delete any Data.

        :params:
        - pkey : int or list[int]
            - Primary key of the symbol to update. May be a single value or list of values.
            - The Primary key can be retrived in the return object from symbol_search.

        - args : Dict[SymbolArgs, Any]
            - A Dictionary of Column Values to update. If PKEY is passed as a Key it will be
            ignored. Extra Keys are Ignored.
            - Note: This can throw a psycopg.Database Error if passed an update to Symbol, Source,
            or Exchange that would result in a change that would violate the UNIQUE flag on those
            collective parameters.

        :returns: Boolean, True on Successful Update.
        """
        if "pkey" in args:
            args.pop("pkey")
        _args = [(k, v) for k, v in args.items() if k in SYMBOL_ARGS]
        if len(_args) == 0:
            log.warning(
                "Attemping to update Database symbol but no arg updates where given. pkey(s) = %s",
                pkey,
            )
            return False

        pkey = [pkey] if isinstance(pkey, int) else pkey
        if len(pkey) == 0:
            log.warning(
                "Attemping to update Database symbol(s) but no pkeys were given"
            )
            return False

        # Convert The pkeys to a List so only one Update Command needs to be sent.
        _filter = sql.SQL("pkey=ANY(ARRAY[{pkeys}])").format(
            pkeys=sql.SQL(",").join(sql.Literal(v) for v in pkey)
        )

        with self._cursor() as cursor:
            cursor.execute(self[Op.UPDATE, AssetTbls.SYMBOLS](_args, _filter))
            return (
                cursor.statusmessage is not None and cursor.statusmessage == "UPDATE 1"
            )

    def stored_symbol_metadata(
        self,
        pkey: int,
        filters: dict[MetadataArgs | str, Any] = {},
    ) -> list[MetadataInfo]:
        """
        Return MetaData about the series information stored for a given symbol, by primary key.
        Only Returns information about what has been stored & aggregated, not everything that should
        be stored & aggregated

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
        """

        _filters = [("pkey", "=", pkey)]  # Ensure a Pkey filter is Passed
        _filters.extend(
            [(k, "=", v) for k, v in filters.items() if k in (METADATA_ARGS - {"pkey"})]
        )

        with self._cursor(dict_cursor=True) as cursor:
            cursor.execute(self[Op.SELECT, AssetTbls._METADATA](_filters))
            return [MetadataInfo(**row) for row in cursor.fetchall()]

    def get_hist(
        self,
        pkey: int,
        timeframe: Timedelta,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        limit: Optional[int] = None,
        rth: bool = True,
        rtn_args: Optional[Iterable[AggregateArgs | TickArgs]] = None,
        *,
        schema: Optional[str | StrEnum] = None,
        asset_class: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """
        Fetch Historical Data from the Database Aggregating the desired data as needed.

        -- PARAMS --
        - pkey : Int
            - Primary Key of the Symbol to Fetch
        - Timeframe : pandas.Timedelta
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
            - Note: 'dt' will always be returned even if not included.

        - schema : Optional str | strEnum
            - Data Schema to fetch the data from.
        - asset_class : Optional str
            - Asset_class of the symbol requested
                - Note: Both asset_class & schema will be determined from the pkey if necessary.
                However, if both are already known and given as args that information will be
                used in place of querying the database for it.
        """

        # Search Metadata for available data at TF or lower
        with self._cursor(dict_cursor=True) as cursor:

            # region -- Fetch Asset Class & Schema given the known pkey if necessary --
            if asset_class is None or schema is None:
                _filters = [("pkey", "=", pkey)]
                cursor.execute(
                    self[Op.SELECT, GenericTbls.TABLE](
                        Schema.SECURITY,
                        AssetTbls.SYMBOLS,
                        [
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
                    log.warning("Unknown pkey : %s", pkey)
                    return
                rsp = rsp[0]

                if rsp["store_minute"]:
                    schema = Schema.MINUTE_DATA
                elif rsp["store_aggregate"]:
                    schema = Schema.AGGREGATE_DATA
                elif rsp["store_tick"]:
                    schema = Schema.TICK_DATA
                else:
                    schema = None

                if schema is None:
                    log.warning("pkey = %s is known, but not Stored", pkey)
                    return

                asset_class = rsp["asset_class"]
                assert asset_class
            # endregion

            desired_table = AssetTable(asset_class, timeframe, False, False, rth)
            src_table, need_to_calc = self.table_config[
                Schema(schema)
            ].get_select_from_table(desired_table)

            # Configure return args as a set
            if rtn_args is None:
                _rtns = {"dt", "open", "high", "low", "close", "volume", "price"}
            else:
                _rtns = {*rtn_args}

            if need_to_calc:
                log.info(
                    "Calculating Aggregate at Timeframe : %s, from %s Timeframe",
                    timeframe,
                    src_table.period,
                )
                cmd = self[Op.SELECT, SeriesTbls.CALCULATE_AGGREGATE](
                    schema, src_table, timeframe, pkey, rth, start, end, limit, _rtns
                )
            else:
                # Works for both Aggregates and Raw Tick Data Retrieval
                cmd = self[Op.SELECT, SeriesTbls.RAW_AGGREGATE](
                    schema, src_table, pkey, rth, start, end, limit, _rtns
                )

            cursor.execute(cmd)
            rsp = cursor.fetchall()
            return None if len(rsp) == 0 else DataFrame(rsp[0])

    # endregion
