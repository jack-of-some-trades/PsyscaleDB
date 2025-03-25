"""Set of Functions that return SQL Composed Commands"""

from __future__ import annotations
from enum import Enum, StrEnum, auto
from typing import (
    Callable,
    Dict,
    Literal,
    Optional,
    Tuple,
    TypeAlias,
)

from pandas import Timedelta, Timestamp
from psycopg import sql

from .orm import AssetTable

# pylint: disable="missing-function-docstring",'protected-access','line-too-long'

# region -------- -------- -------- -------- PSQL CMDS -------- -------- -------- --------

# region --------   -------- Filter Composer   --------  --------
# Util functions to add 'WHERE', 'LIMIT', and 'ORDER' Statements to a SQL Statement


def limit() -> sql.Composable:
    return sql.SQL(" LIMIT %(limit)s ")


def order(arg: str, ascending: bool = True) -> sql.Composable:
    if ascending:
        return sql.SQL(" ORDER BY {arg} ASC").format(arg=sql.Identifier(arg))
    else:
        return sql.SQL(" ORDER BY {arg} DESC").format(arg=sql.Identifier(arg))


def where() -> sql.Composable:
    "SQL WHERE Clause to precede a set of filters"
    return sql.SQL(" WHERE ")


Comparators = Literal["=", "!=", ">", "<", ">=", "<=", "LIKE"]


def union_filter(
    filters: Dict[str, Comparators] | list[sql.Composable],
) -> sql.Composable:
    """
    Simple Union ('AND') Filter. Filters, when a dictionary, is of the format:
    {table_column_name:str, Comparator:Literal['=', '!=', '>', '<', '>=', '<=', "LIKE"]}
    """
    if len(filters) == 0:
        return sql.SQL("")

    if isinstance(filters, dict):
        filters = [_filter(k, v) for k, v in filters.items()]

    return sql.SQL(" AND ").join(filters)


def intersection_filter(
    filters: Dict[str, Comparators] | list[sql.Composable],
) -> sql.Composable:
    """
    Simple Intersection ('OR') Filter. Filters, when a dictionary, is of the format:
    {table_column_name:str, Comparator:Literal['=', '!=', '>', '<', '>=', '<=']}
    """
    if len(filters) == 0:
        return sql.SQL("")

    if isinstance(filters, dict):
        filters = [_filter(k, v) for k, v in filters.items()]

    return sql.SQL(" OR ").join(filters)


def _filter(arg: str, comparison: Comparators) -> sql.Composable:
    return sql.SQL("{arg} " + comparison + " {arg_ph}").format(
        arg=sql.Identifier(arg),
        arg_ph=sql.Placeholder(arg),
    )


# endregion


# region -------- -------- Generic PSQL Commands -------- --------


def list_schemas() -> sql.Composed:
    return sql.SQL("SELECT schema_name FROM information_schema.schemata;").format()


def create_schema(schema: str) -> sql.Composed:
    return sql.SQL("CREATE SCHEMA {schema_name};").format(
        schema_name=sql.Identifier(schema),
    )


def drop_schema(schema: str) -> sql.Composed:
    return sql.SQL("DROP SCHEMA IF EXISTS {schema_name} CASCADE;").format(
        schema_name=sql.Identifier(schema),
    )


def drop_table(schema: str, table: str) -> sql.Composed:
    return sql.SQL("DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;").format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
    )


def drop_materialized_view(schema: str, table: str) -> sql.Composed:
    return sql.SQL(
        "DROP MATERIALIZED VIEW IF EXISTS {schema_name}.{table_name} CASCADE;"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
    )


def list_tables(schema: str) -> sql.Composed:
    return sql.SQL(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = {schema_name};
    """
    ).format(
        schema_name=sql.Literal(schema),
    )


def delete_items(schema: str, table: str, sql_filter: sql.Composable) -> sql.Composed:
    return sql.SQL(
        """
        DELETE FROM {schema_name}.{table_name}
        {filter}
        CASCADE;
    """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
        filter=sql_filter,
    )


# endregion


# region -------- -------- Security Commands -------- --------


def create_symbol_table() -> sql.Composed:
    return sql.SQL(
        """
        CREATE TABLE {schema_name}.{table_name}(
            pkey SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            source TEXT NOT NULL,
            exchange TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            name TEXT NOT NULL,
            stored BOOLEAN NOT NULL DEFAULT False,
            attrs jsonb,
            CONSTRAINT unique_asset UNIQUE (symbol, source, exchange)
        );
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


def select_symbols(
    filters: dict[str, Comparators], include_attrs: bool = False
) -> sql.Composed:
    rtn_args = {"symbol", "exchange", "name", "asset_class", "pkey", "stored", "source"}
    if include_attrs:
        rtn_args |= {"attrs"}

    return sql.SQL(
        """
        SELECT symbol FROM {schema_name}.{table_name}
        {filter};
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
        rtn_args=sql.SQL(",").join([sql.Identifier(arg) for arg in rtn_args]),
        filter=union_filter(filters),
    )


def insert_symbols() -> sql.Composed:
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} (source, symbol, name, exchange, asset_class, attrs) 
        SELECT %(source)s, A, B, C, D, E FROM unnest(
            %(symbol)s::text[],
            %(name)s::text[],
            %(exchange)s::text[],
            %(asset_class)s::text[],
            %(attrs)s::jsonb[]
        ) AS t(A, B, C, D, E)
        ON CONFLICT (symbol, source, exchange) DO NOTHING
        RETURNING symbol;
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


def upsert_symbols() -> sql.Composed:
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} (source, symbol, name, exchange, asset_class, attrs) 
        SELECT %(source)s, A, B, C, D, E FROM unnest(
            %(symbol)s::text[],
            %(name)s::text[],
            %(exchange)s::text[],
            %(asset_class)s::text[],
            %(attrs)s::jsonb[]
        ) AS t(A, B, C, D, E)
        ON CONFLICT (symbol, source, exchange) DO UPDATE
        SET name = EXCLUDED.name, 
            asset_class = EXCLUDED.asset_class,
            attrs = EXCLUDED.attrs
        RETURNING symbol, xmax;
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


# endregion

# region -------- -------- Timeseries Metadata Table Commands -------- --------
# Table to store metadata on each asset that has stored data. specifically time
# that their data starts and ends for each timeframe


def create_timeseries_metadata_view(schema: str) -> sql.Composed:
    return sql.SQL(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS {schema_name}.{view_name} AS
        WITH all_tables AS ({_available_tables} UNION ALL {_available_aggregates}),
        _metadata AS (
            SELECT
                dt.pkey,
                t.table_name,
                t.timeframe,
                t.is_raw_data,
                t.trading_hours_type,
                dt.start_date,
                dt.end_date
            FROM all_tables t
            CROSS JOIN LATERAL get_timeseries_date_range({schema_arg}, t.table_name) AS dt
        )
        SELECT * FROM _metadata;
    """
    ).format(
        schema_arg=sql.Literal(schema),
        schema_name=sql.Identifier(schema),
        view_name=sql.Identifier(SeriesTbls._METADATA),
        _available_tables=_available_tables(schema),
        _available_aggregates=_available_aggregates(schema),
    )


def _available_tables(schema: str) -> sql.Composable:
    # Sub-query to get all the table names in a given timeseries schema
    # is_raw_data = True because it's a table. By nature is must be raw inserted data
    return sql.SQL(
        r"""
        SELECT 
            tablename AS table_name,
            substring(tablename FROM '_(\d+)(?:_raw)?(?:_(ext|rth|eth))?$')::integer AS timeframe,
            TRUE AS is_raw_data,
            CASE 
                WHEN tablename ~ '_ext$' THEN 'ext'
                WHEN tablename ~ '_rth$' THEN 'rth'
                WHEN tablename ~ '_eth$' THEN 'eth'
                ELSE 'none'
            END AS trading_hours_type
        FROM pg_catalog.pg_tables 
        """
        "WHERE schemaname = {schema_name} "
        r""" AND tablename ~ '^[\D]+_\d+(_raw)?(_(ext|rth|eth))?$'
        AND tablename NOT LIKE 'pg\_%'
    """
    ).format(schema_name=sql.Literal(schema))


def _available_aggregates(schema: str) -> sql.Composable:
    # Sub-query to get all the continuous aggregate names in a given timeseries schema
    # is_raw_data = False because it's a continuous agg. By nature is must be derived.
    return sql.SQL(
        r"""
        SELECT 
            user_view_name AS table_name,
            substring(user_view_name FROM '_(\d+)(?:_raw)?(?:_(ext|rth|eth))?$')::INTEGER AS timeframe,
            FALSE AS is_raw_data,
            CASE 
                WHEN user_view_name ~ '_ext$' THEN 'ext'
                WHEN user_view_name ~ '_rth$' THEN 'rth'
                WHEN user_view_name ~ '_eth$' THEN 'eth'
                ELSE 'none'
            END AS trading_hours_type
        FROM _timescaledb_catalog.continuous_agg 
        """
        "WHERE user_view_schema = {schema_name}"
        r""" AND user_view_name ~ '^[\D]+_\d+(_raw)?(_(ext|rth|eth))?$'
        AND user_view_name NOT LIKE 'pg\_%'
        """
    ).format(schema_name=sql.Literal(schema))


def create_timeseries_metadata_subfunction() -> sql.Composed:
    "SQL Dynamic Function to get the Stored Date-Range of all Assets in a Timeseries Table"
    return sql.SQL(
        """
        CREATE OR REPLACE FUNCTION get_timeseries_date_range(_schema TEXT, _table_name TEXT)
        RETURNS TABLE(pkey INT, start_date TIMESTAMPTZ, end_date TIMESTAMPTZ) AS
        $$
        BEGIN
            RETURN QUERY EXECUTE format(
                'SELECT pkey, MIN(dt), MAX(dt) FROM %I.%I GROUP BY pkey',
                _schema, _table_name
            );
        END;
        $$ LANGUAGE plpgsql;
    """
    ).format()


def refresh_timeseries_metadata_view(schema: str) -> sql.Composed:
    return sql.SQL("REFRESH MATERIALIZED VIEW {schema_name}.{view_name};").format(
        schema_name=sql.Identifier(schema),
        view_name=sql.Identifier(SeriesTbls._METADATA),
    )


# endregion


# region -------- -------- -------- Timeseries Bucket Origins Commands -------- -------- --------
# A Table of Timestamps that Store Time bucket origins for each given asset class.


def create_origin_table(schema: str) -> sql.Composed:
    "A Table of Timestamps that Store Time bucket origins for each given asset class."
    return sql.SQL(
        """
        CREATE TABLE {schema_name}.{table_name} (
            asset_class TEXT PRIMARY KEY,
            origin_rth TIMESTAMPTZ NOT NULL,
            origin_eth TIMESTAMPTZ NOT NULL,
            origin_htf TIMESTAMPTZ NOT NULL
        );
    """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
    )


def select_origin(
    schema: str,
    asset_class: str = "",
    rth: bool | None = None,
    period: Timedelta = Timedelta(-1),
    *,
    _all: bool = False,
) -> sql.Composed:
    if _all:
        return _select_all_origins(schema)
    origin = (
        "origin_htf"
        if period >= Timedelta("4W")
        else "origin_rth" if rth else "origin_eth"
    )
    return sql.SQL(
        "SELECT {origin} FROM {schema_name}.{table_name} WHERE asset_class = {asset_class};"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
        origin=sql.Literal(origin),
        asset_class=sql.Literal(asset_class),
    )


def _select_all_origins(schema: str) -> sql.Composed:
    return sql.SQL(
        "SELECT (asset_class, origin_rth, origin_eth, origin_htf) FROM {schema_name}.{table_name};"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
    )


def insert_origin(
    schema: str,
    asset_class: str,
    rth_origin: Timestamp,
    eth_origin: Timestamp,
    htf_origin: Timestamp,
) -> sql.Composed:
    return sql.SQL(
        "INSERT INTO {schema_name}.{table_name} (asset_class, origin_rth, origin_eth, origin_htf) "
        "VALUES ({asset_class}, {origin_rth}, {origin_eth}, {origin_htf});"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
        asset_class=sql.Literal(asset_class),
        origin_rth=sql.Literal(str(rth_origin)),
        origin_eth=sql.Literal(str(eth_origin)),
        origin_htf=sql.Literal(str(htf_origin)),
    )


def update_origin(
    schema: str,
    asset_class: str,
    rth_origin: Timestamp,
    eth_origin: Timestamp,
    htf_origin: Timestamp,
) -> sql.Composed:
    return sql.SQL(
        """
        UPDATE {schema_name}.{table_name} SET
            origin_rth = {origin_rth},
            origin_eth = {origin_eth},
            origin_htf = {origin_htf}
        WHERE asset_class = {asset_class};
        """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
        asset_class=sql.Literal(asset_class),
        origin_rth=sql.Literal(str(rth_origin)),
        origin_eth=sql.Literal(str(eth_origin)),
        origin_htf=sql.Literal(str(htf_origin)),
    )


def delete_origin(schema: str, asset_class: str) -> sql.Composed:
    return sql.SQL(
        "DELETE FROM {schema_name}.{table_name} WHERE asset_class = {asset_class};"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
        asset_class=sql.Literal(asset_class),
    )


# endregion


# region -------- -------- Tick Timeseries Commands -------- --------


def create_tick_table(schema: str, table: AssetTable) -> sql.Composed:
    "Create a Tick table and the initial aggregate needed for other aggregates"
    if table.period != Timedelta(0):
        raise ValueError("A Tick Table must have a Period of Timedelta(0).")
    return sql.SQL(
        """
        CREATE TABLE {schema_name}.{table_name} (
            pkey INTEGER NOT NULL,
            dt TIMESTAMPTZ NOT NULL,"""
        + (" rth BOOLEAN not NULL," if table.ext and table.rth is None else "")
        + """
            price DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION, 
            PRIMARY KEY (pkey, dt),
            CONSTRAINT fk_pkey FOREIGN KEY (pkey) REFERENCES {ref_schema_name}.{ref_table_name} (pkey)
        );
        SELECT create_hypertable({full_table_name}, by_range('dt'));
        """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(str(table)),
        full_table_name=sql.Literal(schema + "." + repr(table)),
        # There's no real good way to make the reference table dynamic
        # Leaving these as Identifies so its easier to see these variable names are hard-coded
        ref_schema_name=sql.Identifier(Schema.SECURITY),
        ref_table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


def create_continuous_tick_aggregate(
    schema: str, table: AssetTable, ref_table: AssetTable
):
    "Create the inital continuous aggregate from a tick table."
    _error_check_continuous_aggrigate(table, ref_table)
    return sql.SQL(
        """
        CREATE MATERIALIZED VIEW {schema_name}.{table_name} WITH (timescaledb.continuous) AS
        SELECT 
            time_bucket({interval}, dt, TIMESTAMPTZ {origin}) as dt,
            pkey,"""
        + ("    first(rth, dt) AS rth," if table.ext and table.rth is None else "")
        + """
            first(price, dt) AS open,
            last(price, dt) AS close,
            max(price) AS high,
            min(price) AS low,
            sum(volume) AS volume,
            sum(price * volume) / NULLIF(SUM(volume), 0) AS vwap,
            count(*) AS ticks
        FROM {schema_name}.{ref_table_name}"""
        # Where clause handles the case when going from a table with an rth column to no rth column
        + (
            " WHERE rth = TRUE "
            if ref_table.rth is None and table.ext and table.rth
            else ""
        )
        + """
        GROUP BY pkey, 1 ORDER BY 1;
        """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(repr(table)),
        ref_table_name=sql.Identifier(repr(ref_table)),
        origin=sql.Literal(table.origin),
        interval=sql.Literal(table.psql_interval),
    )


def _error_check_continuous_aggrigate(table: AssetTable, ref_table: AssetTable):
    if table.ext != ref_table.ext:
        raise AttributeError(
            "EXT Data mismatched between reference and continuous aggregate tables.\n"
            f"Desired Aggregate Table: {table}, Reference Table: {ref_table}"
        )
    if not ref_table.ext or ref_table.rth is None:
        return
    if table.rth is None:  # ref_table.rth == True or False
        raise AttributeError(
            "Cannot create an Aggregate table with an rth column from a table with an rth column."
            f"Desired Aggregate Table: {table}, Reference Table: {ref_table}"
        )
    if ref_table.rth != table.rth:
        raise AttributeError(
            "Cannot create an RTH Aggregate from an ETH aggregate and vise versa."
            f"Desired Aggregate Table: {table}, Reference Table: {ref_table}"
        )


# endregion


# region -------- -------- -------- Aggrigate Timeseries Commands -------- -------- --------


def create_raw_aggregate_table(schema: str, table: AssetTable) -> sql.Composed:
    "Aggregate Table that is filled with data from a source API, Should be maintained by the user."
    return sql.SQL(
        """
        CREATE TABLE {schema_name}.{table_name} (
            pkey INTEGER NOT NULL,
            dt TIMESTAMPTZ NOT NULL,"""
        + (" rth BOOLEAN NOT NULL," if table.ext and table.rth is None else "")
        + """
            close DOUBLE PRECISION NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION,
            vwap DOUBLE PRECISION,
            ticks INTEGER,
            PRIMARY KEY (pkey, dt),
            CONSTRAINT fk_pkey FOREIGN KEY (pkey) REFERENCES {ref_schema_name}.{ref_table_name} (pkey)
        );
        SELECT create_hypertable({full_name}, by_range('dt'));
    """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(repr(table)),
        full_name=sql.Literal(schema + "." + repr(table)),
        # No easy way to make the pkey reference variable
        ref_schema_name=sql.Identifier(Schema.SECURITY),
        ref_table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


def create_continuous_aggrigate(
    schema: str, table: AssetTable, ref_table: AssetTable
) -> sql.Composed:
    "Create a Higher-Timeframe Aggregate from an OHLC Dataset"
    _error_check_continuous_aggrigate(table, ref_table)
    return sql.SQL(
        """
        CREATE MATERIALIZED VIEW {schema_name}.{table_name} WITH (timescaledb.continuous) AS
        SELECT 
            time_bucket({interval}, dt, TIMESTAMPTZ {origin}) as dt,
            pkey,"""
        + ("    first(rth, dt) AS rth," if table.ext and table.rth is None else "")
        + """
            first(open, dt) AS open,
            last(close, dt) AS close,
            max(high) AS high,
            min(low) AS low,
            sum(volume) AS volume,
            sum(vwap * volume) / NULLIF(SUM(volume), 0) AS vwap,
            sum(ticks) AS ticks
        FROM {schema_name}.{ref_table_name}"""
        # Where clause handles the case when going from a table with an rth column to no rth column
        + (
            " WHERE rth = TRUE "
            if ref_table.rth is None and table.ext and table.rth
            else ""
        )
        # GROUP By 1 == Group by dt. Must use number since there is a name conflict on 'dt'
        # between source table and the selected table. Names must be Identical to chain aggregates.
        + """
        GROUP BY pkey, 1 ORDER BY 1
        WITH NO DATA;
    """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(repr(table)),
        origin=sql.Literal(table.origin),
        interval=sql.Literal(table.psql_interval),
        ref_table_name=sql.Identifier(repr(ref_table)),
    )


def insert_aggrigate_series(schema: str, table: str) -> sql.Composed:
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} (
            {insert_args}
        ) 
        VALUES (
            {kw_args}
        );
    """
    ).format(schema_name=sql.Identifier(schema), table_name=sql.Identifier(table))


def upsert_aggrigate_series(schema: str, table: str):
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} (
            {insert_args}
        ) 
        VALUES (
            {kw_args}
        ) ON CONFLICT UPDATE;
    """
    ).format(schema_name=sql.Identifier(schema), table_name=sql.Identifier(table))


def update_aggrigate(
    schema: str, table: str, sql_filter: sql.Composable, args: list[str]
) -> sql.Composed:
    return sql.SQL(
        """
        UPDATE {schema_name}.{table_name} SET (
            {update_args}
        ){filter};
    """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
        update_args=sql.SQL(", ").join([_filter(arg, "=") for arg in args]),
        filter=sql_filter,
    )


# endregion

# endregion


# region  -------- -------- -------- Command Accessor + Supporting Enums  -------- -------- --------


class Schema(StrEnum):
    "Schema Names available within the database"

    SECURITY = auto()
    USER_DATA = auto()
    TICK_DATA = auto()
    MINUTE_DATA = auto()
    AGGREGATE_DATA = auto()


class Generic(StrEnum):
    "Generic Cmds that may apply to multiple table types"

    SCHEMA = auto()
    TABLE = auto()
    VIEW = auto()


class SeriesTbls(StrEnum):
    "Raw & Aggregated Timeseries Data Tables"

    _ORIGIN = auto()  # Tables with a leading underscore are filtered out
    _METADATA = auto()  # When reconstructing a Config from table names
    _METADATA_FUNC = auto()
    TICK = auto()
    MINUTE = auto()
    AGGREGATE = auto()
    RAW_AGGREGATE = auto()
    TICK_AGGREGATE = auto()


class AssetTbls(StrEnum):
    "Security Information Tables"

    SYMBOLS = auto()
    # SPLITS = auto() # Currently not Implemented. All Data is assumed to be Adjusted.
    # DIVIDENDS = auto()
    # EARNINGS = auto()


class UserTbls(StrEnum):
    "TBD Tables for storing various user data"

    USER_TABLE = auto()
    WATCHLIST = auto()
    PRIMITIVES = auto()
    INDICATOR = auto()

    @classmethod
    def get(cls, dtype: Literal["INDICATOR", "PRIMITIVES", "WATCHLIST", "USER_TABLE"]):
        if dtype in cls.__members__:
            return cls.__members__[dtype]
        else:
            return cls.__members__["USER_TABLE"]


class Operation(Enum):
    "Postgres Operations"

    CREATE = auto()
    INSERT = auto()
    UPSERT = auto()
    UPDATE = auto()
    SELECT = auto()
    DROP = auto()
    DELETE = auto()
    REFRESH = auto()


SchemaMap: TypeAlias = Dict[StrEnum, Optional[Schema]]
OperationMap: TypeAlias = Dict[Operation, Dict[StrEnum, Callable[..., sql.Composed]]]

SCHEMA_MAP: SchemaMap = {
    # Mapping that defines what Schema Each Table belongs to. Effectively a Concise Hardcoding.
    AssetTbls.SYMBOLS: Schema.SECURITY,
    # AssetTbls.SPLITS: Schema.SECURITY,    # Not Implemented Yet
    # AssetTbls.EARNINGS: Schema.SECURITY,    # Not Implemented Yet
    # AssetTbls.DIVIDENDS: Schema.SECURITY,    # Not Implemented Yet
    SeriesTbls._ORIGIN: None,  # Table Applies to TICK, MINUTE and AGGREGATE
    SeriesTbls._METADATA: None,  # Table Applies to TICK, MINUTE and AGGREGATE
    SeriesTbls.AGGREGATE: None,  # Table Applies to TICK, MINUTE and AGGREGATE
    SeriesTbls.RAW_AGGREGATE: None,  # Table Applies to MINUTE and AGGREGATE
    SeriesTbls.TICK: Schema.TICK_DATA,
    SeriesTbls.MINUTE: Schema.MINUTE_DATA,
    SeriesTbls.TICK_AGGREGATE: Schema.TICK_DATA,
    # ANY: SCHEMA.USER_DATA
}


OPERATION_MAP: OperationMap = {
    # Mapping that defines the SQL Composing Function for each Operation and Table Combination
    Operation.CREATE: {
        SeriesTbls._ORIGIN: create_origin_table,
        SeriesTbls._METADATA: create_timeseries_metadata_view,
        SeriesTbls._METADATA_FUNC: create_timeseries_metadata_subfunction,
        SeriesTbls.TICK: create_tick_table,
        SeriesTbls.MINUTE: create_raw_aggregate_table,
        SeriesTbls.AGGREGATE: create_continuous_aggrigate,
        SeriesTbls.RAW_AGGREGATE: create_raw_aggregate_table,
        SeriesTbls.TICK_AGGREGATE: create_continuous_tick_aggregate,
        AssetTbls.SYMBOLS: create_symbol_table,
        # AssetTbls.SPLITS: ,
        # AssetTbls.EARNINGS: ,
        # AssetTbls.DIVIDENDS: ,
    },
    Operation.INSERT: {
        AssetTbls.SYMBOLS: insert_symbols,
        # AssetTbls.SPLITS: ,
        # AssetTbls.EARNINGS: ,
        # AssetTbls.DIVIDENDS: ,
        SeriesTbls._ORIGIN: insert_origin,
    },
    Operation.UPSERT: {AssetTbls.SYMBOLS: upsert_symbols},
    Operation.UPDATE: {
        SeriesTbls._ORIGIN: update_origin,
    },
    Operation.SELECT: {
        Generic.TABLE: list_tables,
        Generic.SCHEMA: list_schemas,
        SeriesTbls._ORIGIN: select_origin,
        AssetTbls.SYMBOLS: select_symbols,
        # AssetTbls.SPLITS: ,
        # AssetTbls.EARNINGS: ,
        # AssetTbls.DIVIDENDS: ,
    },
    Operation.DELETE: {
        SeriesTbls._ORIGIN: delete_origin,
    },
    Operation.DROP: {
        Generic.SCHEMA: drop_schema,
        Generic.TABLE: drop_table,
        Generic.VIEW: drop_materialized_view,
    },
    Operation.REFRESH: {SeriesTbls._METADATA: refresh_timeseries_metadata_view},
}


class Commands:
    """
    Class that stores formattable Postgres Commands based on operation and table type.

    Extendable with custom PostgreSQL functions. Given functions will override base functions
    in the event a new function is given for a given operation, table pair.

    Since the Table Key is a StrEnum, overriding will occur if the StrEnums have identical values,
    even if they are separately defined StrEnums.
    """

    def __init__(
        self,
        operation_map: Optional[OperationMap] = None,
        schema_map: Optional[SchemaMap] = None,
    ) -> None:
        self.schema_map = SCHEMA_MAP
        self.operation_map = OPERATION_MAP

        # Merge in Additional Operations Given
        if operation_map is not None:
            for operation, tbl_map in operation_map.items():
                self.operation_map[operation] |= tbl_map

        # Merge in additional Table:Schemas pairs
        if schema_map is not None:
            self.schema_map |= schema_map

    @property
    def all_schemas(self) -> set[Schema]:
        return set(self.schema_map.values()) - {None} | {val for val in Schema}  # type: ignore

    def get_schema(self, table: StrEnum) -> Schema:
        schema = self.schema_map.get(table, Schema.USER_DATA)
        return schema if schema is not None else Schema.USER_DATA

    def __getitem__(
        self, args: Tuple[Operation, StrEnum]
    ) -> Callable[..., sql.Composed]:
        """
        Accessor to retrieve sql commands. Does not type check function args.
        Call Signature is Obj[Operation, Table](*Function Specific args)
        """
        if args[1] not in self.operation_map[args[0]]:
            raise ValueError(
                f"Operation '{args[0].name}' not known for Postgres Table: {args[1].value}"
            )

        return self.operation_map[args[0]][args[1]]


# endregion
