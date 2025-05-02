"""An interface for reading and commiting data to a database"""

from asyncio import create_task
import asyncio
import logging
from inspect import stack
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import (
    AsyncIterator,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    TypeAlias,
    overload,
)
import psycopg as pg
import psycopg.rows as pg_rows
from psycopg import AsyncConnection, OperationalError, sql
from psycopg.pq._enums import ExecStatus
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from .core import (
    PsyscaleConnectParams,
    POOL_GEN_TIMEOUT,
    LOCAL_POOL_GEN_TIMEOUT,
    PsyscaleCore,
)

log = logging.getLogger("psyscale_log")

AsyncDictCursor: TypeAlias = pg.AsyncCursor[pg_rows.DictRow]
AsyncTupleCursor: TypeAlias = pg.AsyncCursor[pg_rows.TupleRow]


class ImproperInitilizationError(Exception):
    "Class not initialized through required create() class method."


class PsyscaleAsyncCore(PsyscaleCore):
    """
    Core Asynchronous client interface for connecting to a PostgreSQL Database.
    Extends the PsyscaleCore Client with an Async Connection Pool to allow for
    a Hybrid Requests.

    Hybrid approach is taken since some functions, when asynchronously awaited,
    will result in more overhead than what is gained by allowing other processes
    to run in the await time.
    """

    def __init__(
        self,
        conn_params: PsyscaleConnectParams | str | None = None,
        *,
        down_on_del: bool = False,
        docker_compose_fpath: str | None = None,
    ):
        super().__init__(
            conn_params,
            down_on_del=down_on_del,
            docker_compose_fpath=docker_compose_fpath,
        )
        self._async_pool: AsyncConnectionPool
        self._pool_task = create_task(self._open())
        asyncio.gather(create_task(self._async_health_check())).add_done_callback(
            lambda good: log.debug(
                "Async Pool Health_check: %s",
                "good" if good else "bad",
            )
        )

    @property
    async def _async_conn(self) -> AsyncConnection:
        if not self._pool_task.done():
            await asyncio.gather(self._pool_task)
        return await self._async_pool.getconn()

    async def close(self):
        "Close the active connection pool prior to __del__."
        await self._async_pool.close()

    async def _open(self):
        "Open the async connection pool. Task automatically created on init."
        _timeout = LOCAL_POOL_GEN_TIMEOUT if self.db_cfg.is_local else POOL_GEN_TIMEOUT

        try:
            self._async_pool = AsyncConnectionPool(
                self.db_cfg.url, open=False, timeout=_timeout
            )
            await self._async_pool.open(timeout=_timeout)
        except PoolTimeout as e:
            raise e  #
        except OperationalError as e:
            raise e  # Give some more informative info here?

    # region ----------- Async Connection & Cursor Methods -----------

    # region -- Cursor & Execute Overloading ---

    @overload
    def _acursor(
        self,
        dict_cursor: Literal[True],
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> AbstractAsyncContextManager[AsyncDictCursor]: ...
    @overload
    def _acursor(
        self,
        dict_cursor: Literal[False] = False,
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> AbstractAsyncContextManager[AsyncTupleCursor]: ...
    @overload
    def _acursor(
        self,
        dict_cursor: bool = False,
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> AbstractAsyncContextManager[AsyncTupleCursor]: ...

    @overload
    async def execute_async(
        self,
        cmd: sql.Composed,
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: Literal[False] = False,
    ) -> Tuple[List[Tuple], str | None]: ...
    @overload
    async def execute_async(
        self,
        cmd: sql.Composed,
        exec_args: Optional[Mapping[str, int | float | str | None]] = None,
        dict_cursor: Literal[True] = True,
    ) -> Tuple[List[Dict], str | None]: ...

    # endregion

    @asynccontextmanager
    async def _acursor(
        self,
        dict_cursor: bool = False,
        *,
        binary: bool = False,
        pipeline: bool = False,
        raise_err: bool = False,
        auto_commit: bool = False,
    ) -> AsyncIterator[AsyncTupleCursor | AsyncDictCursor]:
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
        conn: pg.AsyncConnection = await self._async_conn

        if auto_commit:
            await conn.set_autocommit(True)

        try:
            if pipeline:
                async with (
                    conn.pipeline(),
                    conn.cursor(row_factory=cursor_factory, binary=binary) as cursor,
                ):
                    yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
            else:
                async with (
                    conn,
                    conn.cursor(row_factory=cursor_factory, binary=binary) as cursor,
                ):
                    yield cursor  # type:ignore : Silence the Dict/Tuple overloading Error
        except pg.DatabaseError as e:
            await conn.rollback()  # Reset Database, InFailedSqlTransaction Err thrown if not reset
            log.error("Caught Database Error: \n '%s' \n...Rolling back changes.", e)

            if raise_err:
                raise e  # log the message and continue to let the error bubble
        finally:
            if auto_commit:
                await conn.set_autocommit(False)
            else:
                await conn.commit()

            # Pool is guaranteed to exist at this point
            await self._async_pool.putconn(conn)

    async def execute_async(
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
        async with self._acursor(dict_cursor) as cursor:
            try:
                log.debug("Executing PSQL Command: %s", cmd.as_string(cursor))
                await cursor.execute(cmd, exec_args)

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
                response = await cursor.fetchall()

            return response, cursor.statusmessage

    async def _async_health_check(self) -> bool:
        "Simple Ping to the Database to ensure it is alive"
        async with self._acursor() as cursor:
            await cursor.execute("SELECT 1")
            rsp = await cursor.fetchall()
            return rsp[0][0] == 1
        return False

    # endregion
