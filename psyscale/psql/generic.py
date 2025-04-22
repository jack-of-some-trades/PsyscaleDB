"""Generic PostgreSQL Commands & Util Functions to Compose Filters"""

from enum import StrEnum
from typing import (
    Any,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
)

from psycopg import sql

ArgumentLiteral: TypeAlias = Tuple[str, Any]
Argument: TypeAlias = ArgumentLiteral | str
Comparators: TypeAlias = Literal["=", "!=", ">", "<", ">=", "<=", "LIKE", "ILIKE"]
FilterLiteral: TypeAlias = Tuple[str, Comparators, Any]
FilterPlaceholder: TypeAlias = Tuple[str, Comparators]
Filter: TypeAlias = sql.Composable | FilterLiteral | FilterPlaceholder

# region --------   -------- Filter Composer Utils  --------  --------
# pylint: disable=missing-function-docstring
# Util functions to add 'WHERE', 'LIMIT', and 'ORDER' Statements to a SQL Statement


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
        return sql.SQL(" LIMIT {val}").format(val=sql.Literal(val))
    else:
        return sql.SQL(" LIMIT {val_ph}").format(val_ph=sql.Placeholder(val))


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


def drop_schema(schema: str, cascade: bool = True) -> sql.Composed:
    return sql.SQL(
        "DROP SCHEMA IF EXISTS {schema_name}" + " CASCADE" if cascade else "" + ";"
    ).format(
        schema_name=sql.Identifier(schema),
    )


def drop_table(schema: str, table: str, cascade: bool = True) -> sql.Composed:
    return sql.SQL(
        "DROP TABLE IF EXISTS {schema_name}.{table_name}" + " CASCADE"
        if cascade
        else "" + ";"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
    )


def drop_materialized_view(
    schema: str, table: str, cascade: bool = True
) -> sql.Composed:
    return sql.SQL(
        "DROP MATERIALIZED VIEW IF EXISTS {schema_name}.{table_name}" + " CASCADE"
        if cascade
        else "" + ";"
    ).format(
        schema_name=sql.Identifier(schema),
        table_name=sql.Identifier(table),
    )


def delete(
    schema: str, table: str, filters: list[Filter], cascade: bool = True
) -> sql.Composed:
    return sql.SQL(
        "DELETE FROM {schema_name}.{table_name} {filter}" + " CASCADE"
        if cascade
        else "" + ";"
    ).format(
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
