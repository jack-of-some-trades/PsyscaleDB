"""Set of Functions that return SQL Composed Commands"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, StrEnum, auto
from functools import partial
from json import dumps
from typing import (
    Any,
    Callable,
    Dict,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
    get_args,
)

from pandas import Timedelta, Timestamp
from psycopg import sql

from .orm import AssetTable

# pylint: disable="missing-function-docstring",'protected-access','line-too-long'

# region -------- -------- -------- -------- PSQL CMDS -------- -------- -------- --------

# region --------   -------- Filter Composer   --------  --------
# Util functions to add 'WHERE', 'LIMIT', and 'ORDER' Statements to a SQL Statement

ArgumentLiteral: TypeAlias = Tuple[str, Any]
Argument: TypeAlias = ArgumentLiteral | str
Comparators: TypeAlias = Literal["=", "!=", ">", "<", ">=", "<=", "LIKE", "ILIKE"]
FilterLiteral: TypeAlias = Tuple[str, Comparators, Any]
FilterPlaceholder: TypeAlias = Tuple[str, Comparators]
Filter: TypeAlias = sql.Composable | FilterLiteral | FilterPlaceholder


def where(filters: Sequence[Filter] | Filter) -> sql.Composable:
    "SQL WHERE Clause to precede a set of filters"
    if isinstance(filters, (sql.Composable, Tuple)):
        return sql.SQL(" WHERE ") + filter_composer([filters])  # type:ignore
    else:
        return (
            sql.SQL("")
            if len(filters) == 0
            else sql.SQL(" WHERE ") + filter_composer(filters)
        )


def limit(val: Optional[str | int]) -> sql.Composable:
    if val is None:
        return sql.SQL("")
    if isinstance(val, int):
        return sql.SQL(" LIMIT {val} ").format(val=sql.Literal(val))
    else:
        return sql.SQL(" LIMIT {val_ph} ").format(val_ph=sql.Placeholder(val))


def order(arg: Optional[str], ascending: Optional[bool] = True) -> sql.Composable:
    if arg is None:
        return sql.SQL("")
    if ascending is False:
        return sql.SQL(" ORDER BY {arg} DESC").format(arg=sql.Identifier(arg))
    else:
        return sql.SQL(" ORDER BY {arg}").format(arg=sql.Identifier(arg))


def arg_list(args: Sequence[str], distinct: bool = False) -> sql.Composable:
    fmt_args = (
        sql.SQL("*")
        if len(args) == 0
        else sql.SQL(", ").join(map(sql.Identifier, args))
    )
    return sql.SQL("DISTINCT ") + fmt_args if distinct else fmt_args


def update_args(args: Sequence[Argument]) -> sql.Composable:
    if len(args) == 0:
        raise ValueError("Attempting to update arguments, but no values given to SET.")
    composables = [
        _arg_placeholder(v) if isinstance(v, str) else _arg_literal(*v) for v in args
    ]
    return sql.SQL(", ").join(composables)


def _arg_placeholder(arg: str) -> sql.Composable:
    return sql.SQL("{arg} " + "=" + " {arg_ph}").format(
        arg=sql.Identifier(arg),
        arg_ph=sql.Placeholder(arg),
    )


def _arg_literal(arg: str, value: Any) -> sql.Composable:
    return sql.SQL("{arg} " + "=" + " {arg_lit}").format(
        arg=sql.Identifier(arg),
        arg_lit=sql.Literal(value),
    )


def filter_composer(
    filters: Sequence[Filter], mode: Literal["AND", "OR"] = "AND"
) -> sql.Composable:
    composables = [
        (
            sql.SQL("(") + v + sql.SQL(")")
            if isinstance(v, sql.Composable)
            else _filter_literal(*v) if len(v) == 3 else _filter_placeholder(*v)
        )
        for v in filters
    ]
    if mode == "OR":
        return sql.SQL(" OR ").join(composables)
    else:
        return sql.SQL(" AND ").join(composables)


def _filter_literal(arg: str, comparison: Comparators, value: Any) -> sql.Composable:
    return sql.SQL("{arg}" + comparison + "{arg_val}").format(
        arg=sql.Identifier(arg),
        arg_val=sql.Literal(value),
    )


def _filter_placeholder(arg: str, comparison: Comparators) -> sql.Composable:
    return sql.SQL("{arg}" + comparison + "{arg_ph}").format(
        arg=sql.Identifier(arg),
        arg_ph=sql.Placeholder(arg),
    )


# endregion


# region -------- -------- Generic PSQL Commands -------- --------


def list_schemas() -> sql.Composed:
    return sql.SQL("SELECT schema_name FROM information_schema.schemata;").format()


def list_tables(schema: str) -> sql.Composed:
    return sql.SQL(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = {schema_name};"
    ).format(
        schema_name=sql.Literal(schema),
    )


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


def delete_items(schema: str, table: str, filters: list[Filter]) -> sql.Composed:
    return sql.SQL("DELETE FROM {schema_name}.{table_name} {filter} CASCADE;").format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
        filter=where(filters),
    )


def select(
    schema: str | StrEnum,
    table: str | StrEnum,
    arguments: Sequence[str] = [],
    filters: Sequence[Filter] = [],
    _limit: Optional[str | int] = None,
    _order: Tuple[str | None, bool | None] = (None, None),
    *,
    distinct: bool = False,
) -> sql.Composed:
    return sql.SQL(
        "SELECT {rtn_args} FROM {schema_name}.{table_name}{filter}{order}{limit};"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
        rtn_args=arg_list(arguments, distinct),
        filter=where(filters),
        order=order(*_order),
        limit=limit(_limit),
    )


def update(
    schema: str | StrEnum,
    table: str | StrEnum,
    assignments: list[Argument],
    filters: list[Filter],
) -> sql.Composed:
    return sql.SQL("UPDATE {schema_name}.{table_name} SET {args} {filter};").format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
        args=update_args(assignments),
        filter=where(filters),
    )


# endregion


# region -------- -------- Security Commands -------- --------

SymbolArgs = Literal[
    "pkey",
    "symbol",
    "name",
    "source",
    "exchange",
    "asset_class",
    "store",
    "store_tick",
    "store_minute",
    "store_aggregate",
]
SYMBOL_ARGS = set(v for v in get_args(SymbolArgs))

STRICT_SYMBOL_ARGS = {
    "pkey",
    "source",
    "exchange",
    "asset_class",
    "store",
    "store_tick",
    "store_minute",
    "store_aggregate",
}


def create_search_functions() -> sql.Composed:
    return sql.SQL("CREATE EXTENSION IF NOT EXISTS pg_trgm;").format()


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
            store_tick BOOLEAN NOT NULL DEFAULT False,
            store_minute BOOLEAN NOT NULL DEFAULT False,
            store_aggregate BOOLEAN NOT NULL DEFAULT False,
            store BOOLEAN GENERATED ALWAYS AS (store_tick OR store_minute OR store_aggregate) STORED,
            attrs jsonb,
            CONSTRAINT unique_asset UNIQUE (symbol, source, exchange),
            CONSTRAINT unique_data_schema CHECK (store_tick::int + store_minute::int + store_aggregate::int <= 1)
        );
        CREATE INDEX attrs_gin_idx ON {schema_name}.{table_name} USING gin (attrs);
        CREATE INDEX name_trgm_idx ON {schema_name}.{table_name} USING gin (name gin_trgm_ops);
        CREATE INDEX symbol_trgm_idx ON {schema_name}.{table_name} USING gin (symbol gin_trgm_ops);
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


def select_symbols(
    name: Optional[str],
    symbol: Optional[str],
    filters: list[Filter],
    include_attrs: bool = False,
    attrs: Optional[dict[str, Any]] = None,
    _limit: Optional[str | int] = None,
) -> sql.Composed:
    # Don't return the '_score' regardless if attrs is returned or not
    rtn_args = [v for v in get_args(SymbolArgs)]
    if include_attrs:
        rtn_args.append("attrs")
    return sql.SQL(
        """
        WITH _base_matches AS ( 
            SELECT * from {schema_name}.{table_name}{_filters}
        ),
        _graded_matches AS (
            SELECT *, {_score} AS _score FROM {_inner_select}
        )
        SELECT {_rtn_args} FROM _graded_matches WHERE _score > 0 ORDER BY _score DESC {_limit};
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
        _filters=where(filters),
        _inner_select=_inner_attrs_select(attrs),
        _rtn_args=arg_list(rtn_args),
        _score=_symbol_score(name, symbol),
        _limit=limit(_limit),
    )


def _symbol_score(name: Optional[str], symbol: Optional[str]) -> sql.Composable:
    # Utilizes similarity function from pg_trgm to rank matches by relevancy
    match name, symbol:
        case str(), str():
            stmt = sql.SQL(
                "(similarity(name, {_name}) + similarity(symbol, {_symbol}))"
            )
        case None, str():
            stmt = sql.SQL("similarity(symbol, {_symbol})")
        case str(), None:
            stmt = sql.SQL("similarity(name, {_name})")
        case _:
            stmt = sql.SQL("1")

    return stmt.format(_name=sql.Literal(name), _symbol=sql.Literal(symbol))


def _inner_attrs_select(attrs: Optional[dict[str, Any]] = None) -> sql.Composable:
    if attrs is None:
        # No Additonal Select Statement Necessary
        return sql.SQL("_base_matches")
    else:
        # Perform an Inner Select on _base_matches to test for the given Attrs.
        return sql.SQL("( SELECT * FROM _base_matches WHERE attrs @> {json} )").format(
            json=dumps(attrs)
        )


def create_symbol_buffer() -> sql.Composed:
    "Temp table in injest symbols from. The Source arg is not present since it is assumed constant."
    return sql.SQL(
        """
        CREATE TEMP TABLE _symbol_buffer (
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            name TEXT NOT NULL,
            attrs jsonb
        ) ON COMMIT DROP;
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
    )


def copy_symbols(args: list[str]) -> sql.Composed:
    return sql.SQL("COPY _symbol_buffer ({args}) FROM STDIN;").format(
        args=sql.SQL(",").join([sql.Identifier(arg) for arg in args]),
    )


def insert_copied_symbols(source: str) -> sql.Composed:
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} (source, symbol, name, exchange, asset_class, attrs) 
        SELECT {source}, symbol, name, exchange, asset_class, attrs FROM _symbol_buffer
        ON CONFLICT (symbol, source, exchange) DO NOTHING
        RETURNING symbol;
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
        source=sql.Literal(source),
    )


def upsert_copied_symbols(source: str) -> sql.Composed:
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} (source, symbol, name, exchange, asset_class, attrs) 
        SELECT {source}, symbol, name, exchange, asset_class, attrs FROM _symbol_buffer
        ON CONFLICT (symbol, source, exchange)  DO UPDATE
        SET name = EXCLUDED.name, 
            asset_class = EXCLUDED.asset_class,
            attrs = EXCLUDED.attrs
        RETURNING symbol, xmax;
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        table_name=sql.Identifier(AssetTbls.SYMBOLS),
        source=sql.Literal(source),
    )


# endregion

# region -------- -------- Timeseries Metadata Table Commands -------- --------
# Table to store metadata on each asset that has stored data. specifically time
# that their data starts and ends for each timeframe

MetadataArgs = Literal[
    "pkey",
    "table_name",
    "schema_name",
    "start_date",
    "end_date",
    "timeframe",
    "is_raw_data",
    "trading_hours_type",
]
METADATA_ARGS = set(v for v in get_args(MetadataArgs))


@dataclass
class MetadataInfo:
    """
    Construct for returning symbol metadata.

    Dataclass contains the earliest and latest recorded datapoint (start_date & end_date
    respectfully) for a given data table (schema_name & table_name)
    """

    table_name: str
    schema_name: str | StrEnum
    start_date: Timestamp
    end_date: Timestamp
    table: AssetTable | None = None
    timeframe: Timedelta = field(init=False)

    def __post_init__(self):
        # Allows str/int/datetime args to be passed to constructer and ensure
        # a pd.Timestamp is always stored.
        self.start_date = Timestamp(self.start_date)
        self.end_date = Timestamp(self.end_date)
        if self.table is None:
            self.table = AssetTable.from_table_name(self.table_name)
        self.timeframe = self.table.period


def create_timeseries_metadata_view() -> sql.Composed:
    return sql.SQL(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS {schema_name}.{view_name} AS
        WITH all_tables AS ({_available_tables} UNION ALL {_available_aggregates}),
        _metadata AS (
            SELECT
                dt.pkey,
                t.table_name,
                t.schema_name,
                t.timeframe,
                t.is_raw_data,
                t.trading_hours_type,
                dt.start_date,
                dt.end_date
            FROM all_tables t
            CROSS JOIN LATERAL get_timeseries_date_range(t.schema_name, t.table_name) AS dt
        )
        SELECT * FROM _metadata;
    """
    ).format(
        schema_name=sql.Identifier(Schema.SECURITY),
        view_name=sql.Identifier(AssetTbls._METADATA),
        _available_tables=_available_tables(),
        _available_aggregates=_available_aggregates(),
    )


def _available_tables() -> sql.Composable:
    # Sub-query to get all the table names
    # is_raw_data = True because it's a table. By nature is must be raw inserted data
    return sql.SQL(
        r"""
        SELECT 
            tablename AS table_name,
            schemaname AS schema_name,
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
        "WHERE schemaname IN ({schemas}) "
        r""" AND tablename ~ '^[\D]+_\d+(_raw)?(_(ext|rth|eth))?$'
        AND tablename NOT LIKE 'pg\_%'
    """
    ).format(
        schemas=sql.SQL(", ").join(
            sql.Literal(s)
            for s in [Schema.TICK_DATA, Schema.MINUTE_DATA, Schema.AGGREGATE_DATA]
        )
    )


def _available_aggregates() -> sql.Composable:
    # Sub-query to get all the continuous aggregate names
    # is_raw_data = False because it's a continuous agg. By nature is must be derived.
    return sql.SQL(
        r"""
        SELECT 
            user_view_name AS table_name,
            user_view_schema AS schema_name,
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
        "WHERE user_view_schema IN ({schemas}) "
        r""" AND user_view_name ~ '^[\D]+_\d+(_raw)?(_(ext|rth|eth))?$'
        AND user_view_name NOT LIKE 'pg\_%'
        """
    ).format(
        schemas=sql.SQL(", ").join(
            sql.Literal(s)
            for s in [Schema.TICK_DATA, Schema.MINUTE_DATA, Schema.AGGREGATE_DATA]
        )
    )


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


def refresh_timeseries_metadata_view() -> sql.Composed:
    return sql.SQL("REFRESH MATERIALIZED VIEW {schema_name}.{view_name};").format(
        schema_name=sql.Identifier(Schema.SECURITY),
        view_name=sql.Identifier(AssetTbls._METADATA),
    )


def refresh_continuous_aggregate(
    schema: Schema,
    table: AssetTable,
    start: Optional[Timestamp] = None,
    end: Optional[Timestamp] = None,
) -> sql.Composed:
    return sql.SQL(
        "CALL refresh_continuous_aggregate({full_table_name}, {start}, {end});"
    ).format(
        full_table_name=sql.Literal(schema + "." + table.table_name),
        start=sql.Literal(start),
        end=sql.Literal(end),
    )


def select_timeseries_metadata(
    filters: list[Filter] = [],
    rtn_args: list[str] = ["table_name", "schema_name", "start_date", "end_date"],
) -> sql.Composed:
    return select(Schema.SECURITY, AssetTbls._METADATA, rtn_args, filters)


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
    else:
        return _select_origin(schema, asset_class, rth, period) + sql.SQL(";")


def _select_all_origins(schema: str) -> sql.Composed:
    return sql.SQL(
        "SELECT (asset_class, origin_rth, origin_eth, origin_htf) FROM {schema_name}.{table_name};"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
    )


def _select_origin(
    schema: str,
    asset_class: str = "",
    rth: bool | None = None,
    period: Timedelta = Timedelta(-1),
) -> sql.Composed:
    origin = (
        "origin_htf"
        if period >= Timedelta("4W")
        else "origin_rth" if rth else "origin_eth"
    )
    return sql.SQL(
        "SELECT {origin} FROM {schema_name}.{table_name} WHERE asset_class = {asset_class}"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(SeriesTbls._ORIGIN),
        origin=sql.Identifier(origin),
        asset_class=sql.Literal(asset_class),
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

TickArgs = Literal[
    "dt",
    "rth",
    "price",
    "volume",
]
TICK_ARGS = set(v for v in get_args(TickArgs))


def create_tick_table(schema: str, table: AssetTable) -> sql.Composed:
    "Create a Tick table and the initial aggregate needed for other aggregates"
    if table.period != Timedelta(0):
        raise ValueError("A Tick Table must have a Period of Timedelta(0).")
    return sql.SQL(
        """
        CREATE TABLE {schema_name}.{table_name} (
            pkey INTEGER NOT NULL,
            dt TIMESTAMPTZ NOT NULL,"""
        + (" rth SMALLINT not NULL," if table.ext and table.rth is None else "")
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
            " WHERE rth = 0 "
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


def refresh_continous_aggrigate(
    schema: str, table: AssetTable, start: Optional[Timestamp], end: Optional[Timestamp]
) -> sql.Composed:
    if table.raw:
        raise AttributeError(
            f"Cannot Refresh Table {schema}.{table}. It is not a Continuous Aggregate."
        )
    return sql.SQL(
        "CALL refresh_continuous_aggregate({full_name}, {start}, {end});"
    ).format(
        full_name=sql.Literal(schema + "." + repr(table)),
        start=sql.Literal(start),  # Automatically handles type conversion & Null Case
        end=sql.Literal(end),
    )


def create_raw_tick_buffer(table: AssetTable) -> sql.Composed:
    return sql.SQL(
        """
        CREATE TEMP TABLE _tick_buffer (
            dt TIMESTAMPTZ NOT NULL,"""
        + (" rth SMALLINT not NULL," if table.ext and table.rth is None else "")
        + """
            price DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION DEFAULT NULL
        ) ON COMMIT DROP;
    """
    ).format()


def copy_ticks(args: list[str]) -> sql.Composed:
    return sql.SQL("COPY _tick_buffer ({args}) FROM STDIN;").format(
        args=sql.SQL(",").join([sql.Identifier(arg) for arg in args]),
    )


def insert_copied_ticks(schema: str, table: AssetTable, pkey: int) -> sql.Composed:
    "No Conflict Statement since Inserted Data Ideally should be only new data."
    if table.ext and table.rth is None:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, rth, price, volume) 
            SELECT {pkey}, dt, rth, price, volume FROM _tick_buffer;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )
    else:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, price, volume) 
            SELECT {pkey}, dt, price, volume FROM _tick_buffer;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )


def upsert_copied_ticks(schema: str, table: AssetTable, pkey: int) -> sql.Composed:
    "Not Intended to be used as often as INSERT Operation since this requires a CONTINUOUS AGG REFRESH"
    if table.ext and table.rth is None:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, rth, price, volume) 
            SELECT {pkey}, dt, rth, price, volume FROM _tick_buffer
            ON CONFLICT (pkey, dt) DO UPDATE
            SET price = EXCLUDED.price, volume = EXCLUDED.volume, rth = EXCLUDED.rth;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )
    else:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, price, volume) 
            SELECT {pkey}, dt, price, volume FROM _tick_buffer
            ON CONFLICT (pkey, dt) DO UPDATE
            SET price = EXCLUDED.price, volume = EXCLUDED.volume;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )


# endregion


# region -------- -------- -------- Aggrigate Timeseries Commands -------- -------- --------

AggregateArgs = Literal[
    "dt", "rth", "open", "high", "low", "close", "volume", "vwap", "ticks"
]
AGGREGATE_ARGS = set(v for v in get_args(AggregateArgs))


def create_raw_aggregate_table(schema: str, table: AssetTable) -> sql.Composed:
    "Aggregate Table that is filled with data from a source API, Should be maintained by the user."
    return sql.SQL(
        """
        CREATE TABLE {schema_name}.{table_name} (
            pkey INTEGER NOT NULL,
            dt TIMESTAMPTZ NOT NULL,"""
        + (" rth SMALLINT NOT NULL," if table.ext and table.rth is None else "")
        + """
            close DOUBLE PRECISION NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
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
            " WHERE rth = 0 "
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


def create_raw_agg_buffer(table: AssetTable) -> sql.Composed:
    return sql.SQL(
        """
        CREATE TEMP TABLE _aggregate_buffer (
            dt TIMESTAMPTZ NOT NULL,"""
        + (" rth SMALLINT NOT NULL," if table.ext and table.rth is None else "")
        + """
            close DOUBLE PRECISION NOT NULL,
            open DOUBLE PRECISION DEFAULT NULL,
            high DOUBLE PRECISION DEFAULT NULL,
            low DOUBLE PRECISION DEFAULT NULL,
            volume DOUBLE PRECISION DEFAULT NULL,
            vwap DOUBLE PRECISION DEFAULT NULL,
            ticks INTEGER DEFAULT NULL
        ) ON COMMIT DROP;
    """
    ).format()


def copy_aggregates(args: list[str]) -> sql.Composed:
    return sql.SQL("COPY _aggregate_buffer ({args}) FROM STDIN;").format(
        args=sql.SQL(",").join([sql.Identifier(arg) for arg in args]),
    )


def insert_copied_aggregates(schema: str, table: AssetTable, pkey: int) -> sql.Composed:
    if table.ext and table.rth is None:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, rth, close, open, high, low, volume, vwap, ticks) 
            SELECT {pkey}, dt, rth, close, open, high, low, volume, vwap, ticks FROM _aggregate_buffer;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )
    else:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, close, open, high, low, volume, vwap, ticks) 
            SELECT {pkey}, dt, close, open, high, low, volume, vwap, ticks FROM _aggregate_buffer;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )


def upsert_copied_aggregates(schema: str, table: AssetTable, pkey: int) -> sql.Composed:
    "Not Intended to be used as often as INSERT Operation since this requires a CONTINUOUS AGG REFRESH"
    if table.ext and table.rth is None:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, rth, close, open, high, low, volume, vwap, ticks) 
            SELECT {pkey}, dt, rth, close, open, high, low, volume, vwap, ticks FROM _aggregate_buffer
            ON CONFLICT (pkey, dt) DO UPDATE
            SET rth = EXCLUDED.rth,
                close = EXCLUDED.close,
                open = EXCLUDED.open, 
                high = EXCLUDED.high, 
                low = EXCLUDED.low, 
                volume = EXCLUDED.volume, 
                vwap = EXCLUDED.vwap, 
                ticks = EXCLUDED.ticks;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )
    else:
        return sql.SQL(
            """
            INSERT INTO {schema_name}.{table_name} (pkey, dt, close, open, high, low, volume, vwap, ticks) 
            SELECT {pkey}, dt, close, open, high, low, volume, vwap, ticks FROM _aggregate_buffer
            ON CONFLICT (pkey, dt) DO UPDATE
            SET close = EXCLUDED.close,
                open = EXCLUDED.open, 
                high = EXCLUDED.high, 
                low = EXCLUDED.low, 
                volume = EXCLUDED.volume, 
                vwap = EXCLUDED.vwap, 
                ticks = EXCLUDED.ticks;
        """
        ).format(
            schema_name=sql.Identifier(schema),
            table_name=sql.Identifier(str(table)),
            pkey=sql.Literal(pkey),
        )


def _array_select(column: str):
    return sql.SQL("ARRAY( SELECT {col} FROM inner_select) AS {col}").format(
        col=sql.Identifier(column)
    )


def select_aggregates(
    schema: Schema,
    table: AssetTable,
    pkey: int,
    rth: bool,
    start: Optional[Timestamp],
    end: Optional[Timestamp],
    _limit: Optional[int],
    rtn_args: set[str],
) -> sql.Composed:

    _filters: list[Filter] = [("pkey", "=", pkey)]
    if start is not None:
        _filters.append(("dt", ">=", start))
    if end is not None:
        _filters.append(("dt", "<", end))

    # Filter by rth if accessing a table that has both rth and eth
    if rth and table.ext and table.rth is None:
        _filters.append(("rth", "=", 0))

    if "rth" in rtn_args and table.ext and table.rth is None:
        # 'rth' Doesn't exist in the table we are selecting from.
        rtn_args.remove("rth")

    rtn_args |= {"dt"}  # Ensure dt is returned
    if table.period == Timedelta(0):
        _ordered_rtn_args = [v for v in get_args(TickArgs) if v in rtn_args]
    else:
        _ordered_rtn_args = [v for v in get_args(AggregateArgs) if v in rtn_args]

    # Select all the needed data, then reorient it so it is returned by column instead of by row
    return sql.SQL(
        """
        WITH inner_select AS (
            SELECT {rtn_args} FROM {schema_name}.{table_name}{filter}{order}{limit}
        )
        SELECT {rtn_arrays} ;
    """
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(str(table)),
        rtn_args=arg_list(_ordered_rtn_args),
        filter=where(_filters),
        order=order("dt", True),
        limit=limit(_limit),
        rtn_arrays=sql.SQL(",\n").join(
            [_array_select(col) for col in _ordered_rtn_args]
        ),
    )


def calculate_aggregates(
    schema: Schema,
    src_table: AssetTable,
    timeframe: Timedelta,
    pkey: int,
    rth: bool,
    start: Optional[Timestamp],
    end: Optional[Timestamp],
    _limit: Optional[int],
    rtn_args: set[str],
) -> sql.Composed:

    _filters: list[Filter] = [("pkey", "=", pkey)]
    if start is not None:
        _filters.append(("dt", ">=", start))
    if end is not None:
        _filters.append(("dt", "<", end))

    ext_tbl = src_table.ext and src_table.rth is None
    if ext_tbl and rth:
        # Filter by rth if accessing a table that has both rth and eth
        _filters.append(("rth", "=", 0))
    if not ext_tbl and "rth" in rtn_args:
        # 'rth' Doesn't exist in the table we are selecting from.
        rtn_args.remove("rth")

    if timeframe == Timedelta(0):
        _inner_sel_args = _tick_inner_select_args(rtn_args)
    else:
        _inner_sel_args = _agg_inner_select_args(rtn_args)

    rtn_args |= {"dt"}  # Ensure dt is returned
    _ordered_rtn_args = [v for v in get_args(AggregateArgs) if v in rtn_args]

    return sql.SQL(
        """
        WITH inner_select AS (
            SELECT 
                time_bucket({interval}, dt, ({origin_select})) as dt,
                {inner_select_args}
            FROM {schema}.{table_name}{filters} GROUP BY 1 ORDER BY 1{limit} 
        )
        SELECT {rtn_arrays} ;
        """
    ).format(
        schema=sql.Identifier(schema),
        table_name=sql.Identifier(repr(src_table)),
        origin_select=_select_origin(schema, src_table.asset_class, rth, timeframe),
        interval=sql.Literal(str(int(timeframe.total_seconds())) + " seconds"),
        inner_select_args=sql.SQL(", ").join(_inner_sel_args),
        filters=where(_filters),
        limit=limit(_limit),
        rtn_arrays=sql.SQL(",\n").join(
            [_array_select(col) for col in _ordered_rtn_args]
        ),
    )


def _agg_inner_select_args(args: set[str]) -> list[sql.Composable]:
    inner_select_args = []
    if "rth" in args:
        inner_select_args.append(sql.SQL("first(rth, dt) AS rth"))
    if "open" in args:
        inner_select_args.append(sql.SQL("first(open, dt) AS open"))
    if "high" in args:
        inner_select_args.append(sql.SQL("max(high) AS high"))
    if "low" in args:
        inner_select_args.append(sql.SQL("min(low) AS low"))
    if "close" in args:
        inner_select_args.append(sql.SQL("last(close, dt) AS close"))
    if "volume" in args:
        inner_select_args.append(sql.SQL("sum(volume) AS volume"))
    if "ticks" in args:
        inner_select_args.append(sql.SQL("sum(ticks) AS ticks"))
    if "vwap" in args:
        inner_select_args.append(
            sql.SQL("sum(vwap * volume) / NULLIF(SUM(volume), 0) AS vwap")
        )
    return inner_select_args


def _tick_inner_select_args(args: set[str]) -> list[sql.Composable]:
    inner_select_args = []
    if "rth" in args:
        inner_select_args.append(sql.SQL("first(rth, dt) AS rth"))
    if "open" in args:
        inner_select_args.append(sql.SQL("first(price, dt) AS open"))
    if "high" in args:
        inner_select_args.append(sql.SQL("max(price) AS high"))
    if "low" in args:
        inner_select_args.append(sql.SQL("min(price) AS low"))
    if "close" in args:
        inner_select_args.append(sql.SQL("last(price, dt) AS close"))
    if "volume" in args:
        inner_select_args.append(sql.SQL("sum(volume) AS volume"))
    if "ticks" in args:
        inner_select_args.append(sql.SQL("count(*) AS ticks"))
    if "vwap" in args:
        inner_select_args.append(
            sql.SQL("sum(price * volume) / NULLIF(SUM(volume), 0) AS vwap")
        )
    return inner_select_args


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
    SCHEMA_TABLES = auto()
    TABLE = auto()
    VIEW = auto()


class SeriesTbls(StrEnum):
    "Raw & Aggregated Timeseries Data Tables"

    _ORIGIN = auto()
    TICK = auto()
    TICK_BUFFER = auto()
    RAW_AGGREGATE = auto()
    RAW_AGG_BUFFER = auto()
    CONTINUOUS_AGG = auto()
    CONTINUOUS_TICK_AGG = auto()
    # Following is Not a stored_table, just references a specific select function
    CALCULATE_AGGREGATE = auto()


class AssetTbls(StrEnum):
    "Security Information Tables"

    _METADATA = auto()
    _METADATA_FUNC = auto()
    SYMBOLS = auto()
    SYMBOLS_BUFFER = auto()
    _SYMBOL_SEARCH_FUNCS = auto()
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
    COPY = auto()
    DROP = auto()
    DELETE = auto()
    REFRESH = auto()


SchemaMap: TypeAlias = Dict[StrEnum, Optional[Schema]]
OperationMap: TypeAlias = Dict[Operation, Dict[StrEnum, Callable[..., sql.Composed]]]

SCHEMA_MAP: SchemaMap = {
    # Mapping that defines what Schema Each Table belongs to. Effectively a Concise Hardcoding.
    AssetTbls.SYMBOLS: Schema.SECURITY,
    AssetTbls._METADATA: Schema.SECURITY,
    # AssetTbls.SPLITS: Schema.SECURITY,    # Not Implemented Yet
    # AssetTbls.EARNINGS: Schema.SECURITY,    # Not Implemented Yet
    # AssetTbls.DIVIDENDS: Schema.SECURITY,    # Not Implemented Yet
    SeriesTbls._ORIGIN: None,  # Table Applies to TICK, MINUTE and AGGREGATE
    SeriesTbls.CONTINUOUS_AGG: None,  # Table Applies to TICK, MINUTE and AGGREGATE
    SeriesTbls.RAW_AGGREGATE: None,  # Table Applies to MINUTE and AGGREGATE
    SeriesTbls.TICK: Schema.TICK_DATA,
    SeriesTbls.CONTINUOUS_TICK_AGG: Schema.TICK_DATA,
    # ANY: SCHEMA.USER_DATA
}


OPERATION_MAP: OperationMap = {
    # Mapping that defines the SQL Composing Function for each Operation and Table Combination
    Operation.CREATE: {
        SeriesTbls._ORIGIN: create_origin_table,
        SeriesTbls.TICK: create_tick_table,
        SeriesTbls.TICK_BUFFER: create_raw_tick_buffer,
        SeriesTbls.CONTINUOUS_AGG: create_continuous_aggrigate,
        SeriesTbls.RAW_AGGREGATE: create_raw_aggregate_table,
        SeriesTbls.RAW_AGG_BUFFER: create_raw_agg_buffer,
        SeriesTbls.CONTINUOUS_TICK_AGG: create_continuous_tick_aggregate,
        AssetTbls._SYMBOL_SEARCH_FUNCS: create_search_functions,
        AssetTbls.SYMBOLS: create_symbol_table,
        AssetTbls.SYMBOLS_BUFFER: create_symbol_buffer,
        AssetTbls._METADATA: create_timeseries_metadata_view,
        AssetTbls._METADATA_FUNC: create_timeseries_metadata_subfunction,
        # AssetTbls.SPLITS: ,
        # AssetTbls.EARNINGS: ,
        # AssetTbls.DIVIDENDS: ,
    },
    Operation.INSERT: {
        SeriesTbls.TICK_BUFFER: insert_copied_ticks,
        SeriesTbls.RAW_AGG_BUFFER: insert_copied_aggregates,
        AssetTbls.SYMBOLS_BUFFER: insert_copied_symbols,
        # AssetTbls.SPLITS: ,
        # AssetTbls.EARNINGS: ,
        # AssetTbls.DIVIDENDS: ,
        SeriesTbls._ORIGIN: insert_origin,
    },
    Operation.UPSERT: {
        SeriesTbls.TICK_BUFFER: upsert_copied_ticks,
        SeriesTbls.RAW_AGG_BUFFER: upsert_copied_aggregates,
        AssetTbls.SYMBOLS_BUFFER: upsert_copied_symbols,
    },
    Operation.UPDATE: {
        Generic.TABLE: update,
        SeriesTbls._ORIGIN: update_origin,
        AssetTbls.SYMBOLS: partial(update, Schema.SECURITY, AssetTbls.SYMBOLS),
    },
    Operation.COPY: {
        SeriesTbls.TICK_BUFFER: copy_ticks,
        SeriesTbls.RAW_AGG_BUFFER: copy_aggregates,
        AssetTbls.SYMBOLS_BUFFER: copy_symbols,
    },
    Operation.SELECT: {
        Generic.TABLE: select,
        Generic.SCHEMA_TABLES: list_tables,
        Generic.SCHEMA: list_schemas,
        SeriesTbls._ORIGIN: select_origin,
        SeriesTbls.RAW_AGGREGATE: select_aggregates,
        SeriesTbls.CALCULATE_AGGREGATE: calculate_aggregates,
        AssetTbls.SYMBOLS: select_symbols,
        AssetTbls._METADATA: select_timeseries_metadata,
        # AssetTbls.SPLITS: ,
        # AssetTbls.EARNINGS: ,
        # AssetTbls.DIVIDENDS: ,
    },
    Operation.DELETE: {
        Generic.TABLE: delete_items,
        SeriesTbls._ORIGIN: delete_origin,
    },
    Operation.DROP: {
        Generic.SCHEMA: drop_schema,
        Generic.TABLE: drop_table,
        Generic.VIEW: drop_materialized_view,
    },
    Operation.REFRESH: {
        AssetTbls._METADATA: refresh_timeseries_metadata_view,
        SeriesTbls.CONTINUOUS_AGG: refresh_continuous_aggregate,
        SeriesTbls.CONTINUOUS_TICK_AGG: refresh_continuous_aggregate,
    },
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

        if operation_map is not None:
            self.merge_operations(operation_map)
        if schema_map is not None:
            self.merge_schema_map(schema_map)

    def merge_operations(self, operation_map: OperationMap):
        for operation, tbl_map in operation_map.items():
            self.operation_map[operation] |= tbl_map

    def merge_schema_map(self, schema_map: SchemaMap):
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
