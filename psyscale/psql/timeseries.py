"""Formatted SQL Commands for all Tables & Views Under the Timeseries Schemas"""

from typing import Literal, Optional, get_args

from pandas import Timedelta, Timestamp
from psycopg import sql

from .orm import AssetTable
from .enum import SeriesTbls, AssetTbls, Schema
from .generic import Filter, where, limit, arg_list, order

# pylint: disable=missing-function-docstring, protected-access, line-too-long


# region -------- -------- Timeseries Bucket Origins Commands -------- --------
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
        + (" rth SMALLINT not NULL," if table.has_rth else "")
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
        + ("    first(rth, dt) AS rth," if table.has_rth else "")
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


def _error_check_continuous_aggrigate(table: AssetTable, src_table: AssetTable):
    if table.ext != src_table.ext:
        # One table has extended hours information, the other doesn't
        raise AttributeError(
            "EXT Data mismatched between reference and continuous aggregate tables.\n"
            f"Desired Aggregate Table: {table}, Reference Table: {src_table}"
        )
    if not src_table.ext or src_table.rth is None:
        return  # either ext = None for both, or src_table has all ext information needed
    if table.rth is None:  # ref_table.rth == True or False
        # Trying to get more refined ext information out of src_table than what is possible
        raise AttributeError(
            "Cannot create an Aggregate table with an rth column from a table with an rth column."
            f"Desired Aggregate Table: {table}, Reference Table: {src_table}"
        )
    if src_table.rth != table.rth:
        raise AttributeError(
            "Cannot create an RTH Aggregate from an ETH aggregate and vise versa."
            f"Desired Aggregate Table: {table}, Reference Table: {src_table}"
        )


def refresh_continuous_aggregate(
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
        + (" rth SMALLINT not NULL," if table.has_rth else "")
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
    if table.has_rth:
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
    if table.has_rth:
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
        + (" rth SMALLINT NOT NULL," if table.has_rth else "")
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
        + ("    first(rth, dt) AS rth," if table.has_rth else "")
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
        + (" rth SMALLINT NOT NULL," if table.has_rth else "")
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
    if table.has_rth:
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
    if table.has_rth:
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

    if src_table.has_rth and rth:
        # Filter by rth if accessing a table that has both rth and eth
        _filters.append(("rth", "=", 0))
    if not src_table.has_rth and "rth" in rtn_args:
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
