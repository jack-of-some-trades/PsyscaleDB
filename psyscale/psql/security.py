"""Formatted SQL Commands for all Tables & Views Under the 'Security' Schema"""

from json import dumps
from typing import (
    Any,
    Literal,
    Optional,
    get_args,
)

from psycopg import sql

from .enum import Schema, AssetTbls
from .generic import Filter, where, arg_list, limit, select

# pylint: disable=missing-function-docstring, protected-access

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
            substring(tablename FROM '_(\d+)(?:_raw)?(?:_(ext|rth|eth))?$')::INT AS timeframe,
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
            substring(user_view_name FROM '_(\d+)(?:_raw)?(?:_(ext|rth|eth))?$')::INT AS timeframe,
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


def select_timeseries_metadata(
    filters: list[Filter] = [],
    rtn_args: list[str] = [
        "pkey",
        "table_name",
        "schema_name",
        "start_date",
        "end_date",
    ],
) -> sql.Composed:
    return select(Schema.SECURITY, AssetTbls._METADATA, rtn_args, filters)


# endregion
