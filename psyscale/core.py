"""An interface for reading and commiting data to a database"""

from dataclasses import dataclass, field
from enum import StrEnum
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
    Self,
    Tuple,
    TypeAlias,
    overload,
)
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import psycopg as pg
import psycopg.rows as pg_rows
from psycopg import OperationalError, sql
from psycopg.pq._enums import ExecStatus
from psycopg_pool import ConnectionPool, PoolTimeout

from .psql import (
    GenericTbls,
    Operation as Op,
    OperationMap,
    Schema,
    AssetTbls,
    Commands,
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

BASE_ENV_VARS = [
    "PSYSCALE_HOST",
    "PSYSCALE_PORT",
    "PSYSCALE_USER",
    "PSYSCALE_DB_NAME",
    "PSYSCALE_PASSWORD",
]
YML_ENV_VARS = [
    "PSYSCALE_PORT",
    "PSYSCALE_USER",
    "PSYSCALE_DB_NAME",
    "PSYSCALE_PASSWORD",
    "PSYSCALE_VOLUME_PATH",
]


@dataclass
class PsyscaleConnectParams:
    "Dataclass to derive and store Postgres Database Connection Parameters"

    host: str
    port: int
    user: str
    password: str
    database: str
    sslmode: Optional[str] = None
    application_name: Optional[str] = None
    volume_path: Optional[str] = None
    env_init: bool = field(default=False, init=False)
    url: str = field(init=False)

    @property
    def is_local(self) -> bool:
        "Return true when connection points to a local host"
        return self.host in {"localhost", "127.0.0.1", "::1"}

    def __post_init__(self):
        "Format Params into formatted URL."
        user_info = ""
        if self.user:
            user_info += quote_plus(self.user)
            if self.password:
                user_info += f":{quote_plus(self.password)}"
            user_info += "@"

        query_params = []
        if self.sslmode:
            query_params.append(f"sslmode={quote_plus(self.sslmode)}")
        if self.application_name:
            query_params.append(f"application_name={quote_plus(self.application_name)}")
        query_str = f"?{'&'.join(query_params)}" if query_params else ""

        self.url = f"postgresql://{user_info}{self.host}:{self.port}/{self.database}{query_str}"

    @classmethod
    def from_url(cls, url: str) -> Self:
        "Parses a PostgreSQL connection URL into a PsyscaleConnectParams instance."
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if parsed.scheme not in ("postgres", "postgresql"):
            raise ValueError(
                f"Invalid URL scheme '{parsed.scheme}'. Expected 'postgres' or 'postgresql'."
            )

        return cls(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            user=unquote(parsed.username) if parsed.username else "",
            password=unquote(parsed.password) if parsed.password else "",
            database=parsed.path.lstrip("/") if parsed.path else "",
            sslmode=query.get("sslmode", [None])[0],
            application_name=query.get("application_name", [None])[0],
        )

    @classmethod
    def from_env(cls) -> Self:
        """
        Return a PsyscaleConnectParams instance from environment variables parameters.
        Note: This function does not search for and load env variables from a .env, it only
        tries to pull the variables from the currently loaded environment variables.
        """
        if url := os.getenv("PSYSCALE_URL"):
            inst = cls.from_url(url)
        elif any(map(os.getenv, BASE_ENV_VARS)):
            # Ensure at least one ENV was given before supplying defaults
            inst = cls(
                host=os.getenv("PSYSCALE_HOST") or "localhost",
                port=int(os.getenv("PSYSCALE_PORT") or 5432),
                user=os.getenv("PSYSCALE_USER") or "postgres",
                database=os.getenv("PSYSCALE_DB_NAME") or "postgres",
                password=os.getenv("PSYSCALE_PASSWORD") or "password",
                sslmode=os.getenv("PSYSCALE_SSLMODE"),
                application_name=os.getenv("PSYSCALE_APP_NAME"),
            )
            # Can only docker_compose yml init when all these vars are present
            inst.env_init = all(map(os.getenv, YML_ENV_VARS))
        else:
            raise AttributeError(
                "Cannot Initialize PsyscaleDB from ENV. No Env variables given."
            )

        if inst.is_local:
            # Get additional params if using a local database
            inst.volume_path = os.getenv("PSYSCALE_VOLUME_PATH")

        return inst


class PsyscaleCore:
    """
    Core Synchronous client interface for connecting to a PostgreSQL Database.

    This class contains all the necessary functionality needed to interact
    with the Database at runtime. If provided environment variables that point to
    a local host, the initializer will start/create the docker container as needed.

    Additional functionality (Such as one-off configuration scripts, Data Insertion, etc.)
    are handled by the Psyscale Partial Classes. This is done to organize the
    exceedingly large amount of functionality that is needed to manage a Database.
    """

    def __init__(
        self,
        conn_params: Optional[PsyscaleConnectParams | str] = None,
        *,
        down_on_del: bool = False,
        docker_compose_fpath: Optional[str] = None,
    ):
        """
        Initilize the PsyscaleDB Client. When the Class detects that the connection parameters given
        point to a local database the Client will attempt to start the docker container when needed
        using the sub-process standard library to execute docker commands.

        If this is the first time the database is being run it will initilize the needed docker
        container using the included docker_compose yml. This includes generating the desired
        mounting directory.

        -- PARAMS --
        - conn_params : Optional[PsyscaleConnectParams | str]
            - A string argument is interpreted as a formatted connection url.
            - When None is given the Client will initialize the database using environment variables

        - docker_compose_fpath: Optional[str]
            - Optional File path to point to a custom docker compose yml. Will only be used if the
            conn_params point to a local database *and* the client cannot immediately connect.
            In that case, this file path will be used when calling 'docker-compose up' using a
            sub-process.

        - down_on_del : Boolean Default = False
            - When True, on delete, this client will call 'docker compose down' using the stored
            docker_compose yml. While this does close out an unneeded docker container, this is not
            advised since it will blindly close the container even if another instance or program
            has a connection to the database.
            - Only useful when pointing to a local database.
            - Assumes that the given connection parameters point to the database that was started
            from the stored yml (either the default or the one passed as an argument)

        """
        self.down_on_del = down_on_del
        if isinstance(conn_params, str):
            conn_params = PsyscaleConnectParams.from_url(conn_params)
        if conn_params is None:
            conn_params = PsyscaleConnectParams.from_env()

        _timeout = LOCAL_POOL_GEN_TIMEOUT if conn_params.is_local else POOL_GEN_TIMEOUT

        try:
            self._pool = ConnectionPool(conn_params.url, open=False, timeout=_timeout)
            self._pool.open(timeout=_timeout)
            log.debug("Health_check: %s", "good" if self._health_check() else "bad")
        except PoolTimeout as e:
            if not (conn_params.is_local and conn_params.env_init):
                raise e

            # Try and start the local database, give extra buffer on the timeout.
            self._init_and_start_localdb(docker_compose_fpath, conn_params.volume_path)
            with self._pool.connection(timeout=2.5) as conn:
                conn._check_connection_ok()

        except OperationalError as e:
            raise e  # Give some more informative info here?

        self.cmds = Commands()
        self.db_cfg = conn_params
        self._ensure_std_schemas_exist()

    def __getitem__(self, args: Tuple[Op, StrEnum]) -> Callable[..., sql.Composed]:
        "Accessor forwarder for the self.cmds object"
        return self.cmds[args]

    def __del__(self):
        if self.down_on_del and self.db_cfg.is_local:
            subprocess.run(
                ["docker-compose", "-f", self.yml_path, "down"],
                capture_output=False,
                check=False,
            )

    def merge_operations(self, _map: OperationMap):
        "Merge additional operations into the Database's stored SQL Command Map"
        self.cmds.merge_operations(_map)

    # region ----------- Connection & Cursor Methods -----------

    # region -- Cursor & Execute Overloading ---

    @overload
    @contextmanager
    def _cursor(
        self,
        dict_cursor: Literal[True],
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[DictCursor]: ...
    @overload
    @contextmanager
    def _cursor(
        self,
        dict_cursor: Literal[False] = False,
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[TupleCursor]: ...
    @overload
    @contextmanager
    def _cursor(
        self,
        dict_cursor: bool = False,
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> Iterator[TupleCursor]: ...

    @overload
    def execute(
        self,
        cmd: sql.Composed,
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: Literal[False] = False,
    ) -> Tuple[List[Tuple], str | None]: ...
    @overload
    def execute(
        self,
        cmd: sql.Composed,
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: Literal[True] = True,
    ) -> Tuple[List[Dict], str | None]: ...

    # endregion

    @contextmanager
    def _cursor(
        self,
        dict_cursor: bool = False,
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
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

        raise_err = True Raises any database errors after rolling back the database.
        raise_err = False Rollsback changes, logs an error, then silences the error.
        """
        cursor_factory = pg_rows.dict_row if dict_cursor else pg_rows.tuple_row
        conn: pg.Connection = self._pool.getconn()

        if auto_commit:
            conn.set_autocommit(True)

        try:
            if pipeline:
                with (
                    conn.pipeline(),
                    conn.cursor(row_factory=cursor_factory, binary=binary) as cursor,
                ):
                    yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
            else:
                with (
                    conn,
                    conn.cursor(row_factory=cursor_factory, binary=binary) as cursor,
                ):
                    yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
        except pg.DatabaseError as e:
            conn.rollback()  # Reset Database, InFailedSqlTransaction Err thrown if not reset
            log.error("Caught Database Error: \n '%s' \n...Rolling back changes.", e)

            if raise_err:
                raise e  # log the message and continue to let the error bubble
        finally:
            if auto_commit:
                conn.set_autocommit(False)
            else:
                conn.commit()

            self._pool.putconn(conn)

    def execute(
        self,
        cmd: sql.Composed | sql.SQL,
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: bool = False,
    ) -> Tuple[List[Dict] | List[Tuple], str | None]:
        """
        Execution Method to manually invoke a command within the database.

        -- PARAMS --
        - cmd : sql.Composed
            - Composed SQL function using psycopg.sql sub-module
        - exec_args : Optional Mapping
            - When supplied, exec_args will be passed to the cursor and used to populate
            any placeholder args within the formatted command.
        - dict_cursor : boolean
            - When true will return any results as a list of dicts where each item in
            the list is a row of the returned table.
            - When false a list of Tuples per row is returned.

        -- RETURNS --
        Tuple[ List[] , str ] ==> Tuple of query results (if any) and the cursor status
        message returned as a string.
        """
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
                return [], cursor.statusmessage

            response = []
            pgr = cursor.pgresult
            # Really sad I have to dig to check if there is data available.
            if pgr is not None and pgr.status == ExecStatus.TUPLES_OK:
                response = cursor.fetchall()

            return response, cursor.statusmessage

    def _health_check(self) -> bool:
        "Simple Ping to the Database to ensure it is alive"
        with self._cursor() as cursor:
            cursor.execute("SELECT 1")
            rsp = cursor.fetchall()
            return rsp[0][0] == 1
        return False

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
                "Failed to Read Installed Docker Images, Ensure Docker Engine is Running"
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

    def _ensure_std_schemas_exist(self):
        with self._cursor() as cursor:
            cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA]())
            schemas: set[str] = {rsp[0] for rsp in cursor.fetchall()}

            for schema in {v for v in Schema}.difference(schemas):
                log.info("Creating Schema %s", schema)
                cursor.execute(self[Op.CREATE, GenericTbls.SCHEMA](schema))

    def _get_pkey(self, symbol: str | int) -> int | None:
        if isinstance(symbol, int):
            return symbol

        rsp, _ = self.execute(
            self[Op.SELECT, GenericTbls.TABLE](
                Schema.SECURITY,
                AssetTbls.SYMBOLS,
                [
                    "pkey",
                    "source",
                    "exchange",
                    "asset_class",
                ],
                filters=("symbol", "ILIKE", symbol),
                _limit=3,
            )
        )

        if len(rsp) > 1:
            log.warning(
                "Attempting to fetch or update symbol %s but database contains"
                " multiple of these symbols. Using first match %s",
                symbol,
                rsp[0],
            )

        return None if len(rsp) == 0 else rsp[0][0]

    # endregion
