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
from pandas import Timestamp

import psycopg as pg
import psycopg.rows as pg_rows
from psycopg import OperationalError, sql
from psycopg_pool import ConnectionPool, PoolTimeout

from .sql_cmds import Generic, Operation as Op, Schema, SeriesTbls, Commands
from .orm import TimeseriesConfig


# region ----------- Database Structures  -----------

log = logging.getLogger("pycharts-timescaledb")

# Get the Timescale.yml in the folder this file is stored in.
DEFAULT_YML_PATH = Path(__file__).parent.joinpath("timescale.yml").as_posix()
TIMESCALE_IMAGE = "timescale/timescaledb-ha:pg17"
POOL_GEN_TIMEOUT = 5  # seconds to wait for the connection pool to be generated
# endregion
# pylint: disable='protected-access'

DictCursor: TypeAlias = pg.Cursor[pg_rows.DictRow]
TupleCursor: TypeAlias = pg.Cursor[pg_rows.TupleRow]


class TimeScaleDB:
    """
    Core Interface between Python and a PostgreSQL Database.

    Timescale DB Docker self-host instructions
    https://docs.timescale.com/self-hosted/latest/install/installation-docker/

    This Class contains all the necessary functionality needed to interact
    with the Database at Runtime.

    Additional functionality (Such as one-off configuration scripts, Data Insertion, etc.)
    are handled by the TimescaleDB_EXT Subclass. This is done to de-clutter the
    exceedingly large amount of functionality that is needed to manage a Database.
    """

    def __init__(
        self,
        *,
        docker_compose_fpath: Optional[str] = None,
    ):
        self.config: dict[str, Any] = {
            "host": os.getenv("TIMESCALE_HOST"),
            "port": os.getenv("TIMESCALE_PORT"),
            "user": os.getenv("TIMESCALE_USER"),
            "dbname": os.getenv("TIMESCALE_DB_NAME"),
            "password": os.getenv("TIMESCALE_PASSWORD"),
        }

        if self.config["host"] in {"localhost", "127.0.0.1"}:
            # Get additional params if using a local database
            self.config["volume_path"] = os.getenv("TIMESCALE_VOLUME_PATH")
            self.config["project_name"] = os.getenv("PROJECT_NAME")

        missing_keys = {key for key, value in self.config.items() if value is None}
        if len(missing_keys) > 0:
            raise AttributeError(
                f"Cannot instantiate Timescale DB. Missing Environment Variables: {missing_keys} \n"
            )

        self.conn_params: dict[str, Any] = {
            k: self.config[k] for k in ("user", "password", "dbname", "host", "port")
        }

        if self.config["host"] in {"localhost", "127.0.0.1"}:
            self._init_and_start_localdb(docker_compose_fpath)

        # Generate a Connection Pool
        self.pool = ConnectionPool(kwargs=self.conn_params, open=False)

        try:
            self.pool.open(timeout=POOL_GEN_TIMEOUT)
            log.debug("Health_check: %s", "good" if self._health_check() else "bad")
        except PoolTimeout as e:
            raise e  # TODO: Give some more informative info here?
        except OperationalError as e:
            raise e  # TODO: Give some more informative info here?

        # Generate a Bound PSQL Operation Map, Supply args to expand commands in the future?
        self.cmds = Commands()  # Commands(operation_map, schema_map)
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

    # region ----------- Private Connection & Cursor Methods -----------

    def _health_check(self) -> bool:
        "Simple Ping to the Database to ensure it is alive"
        with self._cursor() as cursor:
            cursor.execute("SELECT 1")
            rsp = cursor.fetchall()
            return rsp[0][0] == 1
        return False

    @overload
    @contextmanager
    def _cursor(self, dict_cursor: Literal[True]) -> Iterator[DictCursor]: ...
    @overload
    @contextmanager
    def _cursor(self, dict_cursor: Literal[False] = False) -> Iterator[TupleCursor]: ...
    @overload
    @contextmanager
    def _cursor(self, dict_cursor: bool = False) -> Iterator[TupleCursor]: ...

    @contextmanager
    def _cursor(
        self, dict_cursor: bool = False
    ) -> Iterator[TupleCursor] | Iterator[DictCursor]:
        cursor_factory = pg_rows.dict_row if dict_cursor else pg_rows.tuple_row

        conn: pg.Connection = self.pool.getconn()
        try:
            with conn, conn.cursor(row_factory=cursor_factory) as cursor:
                yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
        except pg.DatabaseError as e:
            conn.rollback()  # Reset Database for future cmds InFailedSqlTransaction Err thrown otherwise
            log.error("Caught Database Error: \n '%s' \n...Rolling back changes.", e)
        finally:
            conn.commit()
            self.pool.putconn(conn)

    @overload
    def _execute_catch(
        self,
        cmd: sql.Composed,
        args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: bool = False,
    ) -> Tuple[List[Tuple], str]: ...
    @overload
    def _execute_catch(
        self,
        cmd: sql.Composed,
        args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: bool = True,
    ) -> Tuple[List[Dict], str]: ...

    def _execute_catch(
        self,
        cmd: sql.Composed,
        args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: bool = False,
    ) -> Tuple[List[Dict] | List[Tuple], str]:
        with self._cursor(dict_cursor) as cursor:
            try:
                log.debug("Executing PSQL Command: %s", cmd.as_string(cursor))
                cursor.execute(cmd, args)

            except pg.DatabaseError as e:
                log.error(
                    "Cursor Execute Exception (%s) occured in '%s' \n  Exception Msg: %s",
                    e.__class__.__name__,
                    stack()[1].function,
                    e,
                )
                return [], str(cursor.statusmessage)

            response = []
            try:
                response = cursor.fetchall()
            except pg.ProgrammingError as e:
                # raise any errors other than "no data returned"
                if str(e) != "the last operation didn't produce a result":
                    raise e

            return response, str(cursor.statusmessage)

    # endregion

    # region ----------- Private Database Interaction Methods -----------

    def _init_and_start_localdb(self, docker_compose_fpath: Optional[str]):
        "Starts Up, via subprocess terminal cmds, a local Docker Container that runs TimescaleDB"
        try:  # Ensure Docker is installed
            subprocess.run(["docker", "--version"], capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            raise OSError(
                "Cannot Initialize Local TimescaleDB, OS does not have docker installed."
            ) from e

        # Ensure Timescale Image has been pulled.
        p = subprocess.run(
            ["docker", "images", TIMESCALE_IMAGE],
            capture_output=True,
            check=True,
        )
        if len(p.stdout.decode().strip().split("\n")) <= 1:
            # i.e. STDOut only returned table heading and no rows listing available images.
            raise OSError(
                "Missing Necessary TimescaleDB Image to initilize TimescaleDB(). \n"
                "Execute 'docker pull timescale/timescaledb-ha:pg17' in a terminal"
            )

        if not os.path.exists(self.config["volume_path"]):
            print(f'Making Database Volume Folder at {self.config["volume_path"] = }')
            os.mkdir(self.config["volume_path"])

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
                cursor.execute(self[Op.SELECT, Generic.TABLE](schema))
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
