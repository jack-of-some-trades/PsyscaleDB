"""SQL Operation Map Construction and Commands Accessor"""

from __future__ import annotations
from copy import deepcopy
from enum import StrEnum
from logging import getLogger
from functools import partial
from typing import Optional, Tuple, TypeAlias, Callable

from psycopg import sql

from . import generic as gen
from . import security as sec
from . import timeseries as ts
from .enum import Operation, Schema, GenericTbls, AssetTbls, SeriesTbls

log = getLogger("psyscale_log")


# pylint: disable=protected-access
class Commands:
    """
    Class that stores formattable Postgres Commands based on operation and table type.

    Extendable with custom PostgreSQL functions. Given functions will override base functions
    in the event a new function is given for a given operation, table pair.

    Since the Table Key is a StrEnum, overriding will occur if the StrEnums have identical values,
    even if they are separately defined StrEnums.
    """

    def __init__(self, operation_map: Optional[OperationMap] = None) -> None:
        self.operation_map = deepcopy(OPERATION_MAP)

        if operation_map is not None:
            self.merge_operations(operation_map)

    def merge_operations(self, operation_map: OperationMap):
        """
        Merge additional operations into the existing suite of pre-formatted SQL Commands.
        Merging prefers the operations given over the operations present.

        Commands are access via Operation & strEnums. strEnums are keyed as strings, not object ids.
        Therefore, overwriting not only occurs when a command is passed with an already existing
        [Operation, Table_type] but also when a command is passed as [Operation, str(table_type)]

        e.g.
        [Operation.Create, AssetTbls.Symbol] = sql.SQL() & [Operation.Create, 'symbol'] = sql.SQL()
        will both overwrite the creation of the symbols table.
        """
        for operation, tbl_map in operation_map.items():
            known_keys = set(self.operation_map[operation].keys())
            if len(overlap := known_keys.intersection(tbl_map.keys())) > 0:
                log.warning(
                    "Overriding psyscale default %s for tables: %s",
                    operation,
                    overlap,
                )
            self.operation_map[operation] |= tbl_map

    def __getitem__(self, args: Tuple[Operation, StrEnum | str]) -> Callable[..., sql.Composed]:
        """
        Accessor to retrieve sql commands. Does not type check function args.
        Call Signature is Obj[Operation, Table](*Function Specific args)
        """
        if args[1] not in self.operation_map[args[0]]:
            tbl = args[1] if isinstance(args[1], str) else args[1].value
            raise ValueError(f"Operation '{args[0].name}' not known for Postgres Table: {tbl}")

        return self.operation_map[args[0]][args[1]]


OperationMap: TypeAlias = dict[Operation, dict[StrEnum | str, Callable[..., sql.Composed]]]

OPERATION_MAP: OperationMap = {
    # Mapping that defines the SQL Composing Function for each Operation and Table Combination
    Operation.CREATE: {
        GenericTbls.SCHEMA: gen.create_schema,
        SeriesTbls._ORIGIN: ts.create_origin_table,
        SeriesTbls.TICK: ts.create_tick_table,
        SeriesTbls.TICK_BUFFER: ts.create_raw_tick_buffer,
        SeriesTbls.RAW_AGGREGATE: ts.create_raw_aggregate_table,
        SeriesTbls.RAW_AGG_BUFFER: ts.create_raw_agg_buffer,
        SeriesTbls.CONTINUOUS_AGG: ts.create_continuous_aggrigate,
        SeriesTbls.CONTINUOUS_TICK_AGG: ts.create_continuous_tick_aggregate,
        AssetTbls.SYMBOLS: sec.create_symbol_table,
        AssetTbls.SYMBOLS_BUFFER: sec.create_symbol_buffer,
        AssetTbls._SYMBOL_SEARCH_FUNCS: sec.create_search_functions,
        AssetTbls._METADATA: sec.create_timeseries_metadata_view,
        AssetTbls._METADATA_FUNC: sec.create_timeseries_metadata_subfunction,
    },
    Operation.INSERT: {
        SeriesTbls.TICK_BUFFER: ts.insert_copied_ticks,
        SeriesTbls.RAW_AGG_BUFFER: ts.insert_copied_aggregates,
        AssetTbls.SYMBOLS_BUFFER: sec.insert_copied_symbols,
        SeriesTbls._ORIGIN: ts.insert_origin,
    },
    Operation.UPSERT: {
        SeriesTbls.TICK_BUFFER: ts.upsert_copied_ticks,
        SeriesTbls.RAW_AGG_BUFFER: ts.upsert_copied_aggregates,
        AssetTbls.SYMBOLS_BUFFER: sec.upsert_copied_symbols,
    },
    Operation.UPDATE: {
        GenericTbls.TABLE: gen.update,
        SeriesTbls._ORIGIN: ts.update_origin,
        AssetTbls.SYMBOLS: partial(gen.update, Schema.SECURITY, AssetTbls.SYMBOLS),
    },
    Operation.COPY: {
        AssetTbls.SYMBOLS_BUFFER: sec.copy_symbols,
        SeriesTbls.TICK_BUFFER: ts.copy_ticks,
        SeriesTbls.RAW_AGG_BUFFER: ts.copy_aggregates,
        SeriesTbls.RAW_AGGREGATE: ts.select_aggregates_copy,
        SeriesTbls.CALCULATE_AGGREGATE: ts.calculate_aggregates_copy,
    },
    Operation.SELECT: {
        GenericTbls.TABLE: gen.select,
        GenericTbls.VIEW: gen.list_mat_views,
        GenericTbls.SCHEMA: gen.list_schemas,
        GenericTbls.SCHEMA_TABLES: gen.list_tables,
        SeriesTbls._ORIGIN: ts.select_origin,
        SeriesTbls.RAW_AGGREGATE: ts.select_aggregates,
        SeriesTbls.CALCULATE_AGGREGATE: ts.calculate_aggregates,
        AssetTbls.SYMBOLS: sec.select_symbols,
        AssetTbls._METADATA: sec.select_timeseries_metadata,
    },
    Operation.DELETE: {
        GenericTbls.TABLE: gen.delete,
        SeriesTbls._ORIGIN: ts.delete_origin,
    },
    Operation.DROP: {
        GenericTbls.SCHEMA: gen.drop_schema,
        GenericTbls.TABLE: gen.drop_table,
        GenericTbls.VIEW: gen.drop_materialized_view,
    },
    Operation.REFRESH: {
        AssetTbls._METADATA: sec.refresh_timeseries_metadata_view,
        SeriesTbls.CONTINUOUS_AGG: ts.refresh_continuous_aggregate,
        SeriesTbls.CONTINUOUS_TICK_AGG: ts.refresh_continuous_aggregate,
    },
}
