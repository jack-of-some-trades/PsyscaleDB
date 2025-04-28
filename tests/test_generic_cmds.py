import pytest
from psycopg import sql

from psyscale.psql.generic import (
    where,
    limit,
    order,
    arg_list,
    update_args,
    filter_composer,
)


def test_where():
    # Filter Literal
    stmt = where(("id", "=", 5))
    assert stmt.as_string() == ' WHERE "id"=5'
    # Filter Placeholder
    stmt = where(("name", "ILIKE"))
    assert stmt.as_string() == ' WHERE "name"ILIKE%(name)s'
    # Empty list
    assert where([]).as_string() == ""


def test_limit_with_str():
    assert limit(10).as_string() == " LIMIT 10"
    assert limit("param").as_string() == " LIMIT %(param)s"
    assert limit(None).as_string() == ""


def test_order_default():
    stmt = order("created_at")
    assert stmt.as_string() == ' ORDER BY "created_at"'
    stmt = order("created_at", ascending=False)
    assert stmt.as_string() == ' ORDER BY "created_at" DESC'
    stmt = order(None)
    assert stmt.as_string() == ""


def test_arg_list():
    result = arg_list(["a", "b"]).as_string()
    assert result == '"a", "b"'
    result = arg_list(["a", "b"], distinct=True).as_string()
    assert result == 'DISTINCT "a", "b"'
    assert arg_list([]).as_string() == "*"
    assert arg_list([], distinct=True).as_string() == "DISTINCT *"


def test_update_args():
    args = [("a", 1), ("b", True)]  # Literals
    result = update_args(args).as_string()
    assert result == '"a" = 1, "b" = true'

    args = ["a", "b"]  # Placeholders
    result = update_args(args).as_string()
    assert result == '"a" = %(a)s, "b" = %(b)s'

    args = [("a", 1), "b"]  # Literals & Placeholders
    result = update_args(args).as_string()
    assert result == '"a" = 1, "b" = %(b)s'

    with pytest.raises(ValueError):
        update_args([])


def test_filter_composer_literals():
    filters = [("a", "=", 1), ("b", "LIKE", "%abc%")]  # Literals
    result = filter_composer(filters).as_string()
    assert result == """"a"=1 AND "b"LIKE'%abc%'"""

    filters = [("a", ">"), ("b", "ILIKE")]  # Placeholders
    # Following line can't recognize the Comparator Literals, apparently thinks they're strings
    result = filter_composer(filters).as_string()  # type: ignore
    assert result == '"a">%(a)s AND "b"ILIKE%(b)s'

    filters = [sql.SQL("EXISTS(SELECT 1)")]  # Inner SQL
    result = filter_composer(filters).as_string()
    assert result == "(EXISTS(SELECT 1))"

    filters = [("a", "!=", 1), ("b", "=", 2)]
    result = filter_composer(filters, mode="OR").as_string()  # type: ignore
    assert result == '"a"!=1 OR "b"=2'
