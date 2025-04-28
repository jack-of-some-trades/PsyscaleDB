import logging
import pytest
from psycopg import sql
from psyscale.psql import Commands, Operation, SeriesTbls
from psyscale.psql.enum import AssetTbls

# pylint: disable=missing-function-docstring, protected-access
# --- Mocks ---


def dummy_sql_function(*args, **kwargs):
    return sql.SQL("DUMMY SQL")


@pytest.fixture()
def mock_operation_map():
    return {
        Operation.CREATE: {
            SeriesTbls.TICK: dummy_sql_function,
        },
        Operation.SELECT: {
            SeriesTbls._ORIGIN: dummy_sql_function,
        },
    }


# --- Tests ---


def test_initializes_with_default_map():
    cmds = Commands()
    assert isinstance(cmds.operation_map, dict)
    assert Operation.CREATE in cmds.operation_map

    # Assert access works for both str & strEnum
    assert (
        cmds[Operation.CREATE, AssetTbls.SYMBOLS.value]
        == cmds[Operation.CREATE, AssetTbls.SYMBOLS]
    )


def test_merge_operations_overrides_existing_strenum(mock_operation_map, caplog):
    caplog.set_level(logging.WARNING)

    cmds = Commands()
    # Copy existing func to compare
    create_tick_original_func = cmds[Operation.CREATE, SeriesTbls.TICK]
    tick_buffer_original_func = cmds[Operation.CREATE, SeriesTbls.TICK_BUFFER]

    # Merge mock which overrides CREATE/TICK
    cmds.merge_operations(mock_operation_map)

    # The original remains unless it was overridden
    assert cmds[Operation.CREATE, SeriesTbls.TICK_BUFFER] == tick_buffer_original_func
    assert cmds[Operation.CREATE, SeriesTbls.TICK] == dummy_sql_function
    assert cmds[Operation.CREATE, SeriesTbls.TICK] != create_tick_original_func

    # Assert Warning Message was Thrown
    assert any(
        f"Overriding psyscale default Operation.CREATE for tables: {set([SeriesTbls.TICK])}"
        in record.message
        for record in caplog.records
    ), "Expected a warning about overriding CREATE operations"
    assert any(
        f"Overriding psyscale default Operation.SELECT for tables: {set([SeriesTbls._ORIGIN])}"
        in record.message
        for record in caplog.records
    ), "Expected a warning about overriding SELECT operations"


def test_merge_operations_overrides_existing_str(mock_operation_map, caplog):
    caplog.set_level(logging.WARNING)
    cmds = Commands()
    # Copy existing func to compare
    create_tick_original_func = cmds[Operation.CREATE, "tick"]
    tick_buffer_original_func = cmds[Operation.CREATE, "tick_buffer"]

    # Merge mock which overrides CREATE/TICK
    cmds.merge_operations(mock_operation_map)

    # The original remains unless it was overridden
    assert cmds[Operation.CREATE, "tick_buffer"] == tick_buffer_original_func
    assert cmds[Operation.CREATE, "tick"] == dummy_sql_function
    assert cmds[Operation.CREATE, "tick"] != create_tick_original_func

    # Assert Warning Message was Thrown
    assert any(
        f"Overriding psyscale default Operation.CREATE for tables: {set([SeriesTbls.TICK])}"
        in record.message
        for record in caplog.records
    ), "Expected a warning about overriding CREATE operations"
    assert any(
        f"Overriding psyscale default Operation.SELECT for tables: {set([SeriesTbls._ORIGIN])}"
        in record.message
        for record in caplog.records
    ), "Expected a warning about overriding SELECT operations"


def test_custom_map_on_init(mock_operation_map):
    cmds = Commands(operation_map=mock_operation_map)
    assert cmds[Operation.CREATE, SeriesTbls.TICK] == dummy_sql_function


def test_all_return_sql_func():
    cmds = Commands()
    for op, func_pairs in cmds.operation_map.items():
        for tbl in func_pairs.keys():
            func = cmds[op, tbl]
            assert callable(func)


def test_getitem_raises_on_missing_entry():
    cmds = Commands()
    with pytest.raises(ValueError) as excinfo:
        _ = cmds[Operation.CREATE, "value_not_in_op_map"]
    assert "Operation" in str(excinfo.value)
