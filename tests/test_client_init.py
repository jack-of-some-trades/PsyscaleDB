import os
import pytest
from psycopg import connect
from psycopg.rows import tuple_row
from testcontainers.postgres import PostgresContainer

from psycopg import sql
from psyscale.dev import *
from psyscale import PsyscaleConnectParams, PsyscaleDB
from psyscale.psql.enum import GenericTbls, Schema
from psyscale.psql.generic import list_schemas


# Test naming convention done to ensure test orders
def test_00_initlization_state(module_test_url):
    # Check to make sure there is nothing in the database already
    with (
        connect(module_test_url) as _conn,
        _conn.cursor(row_factory=tuple_row) as _cursor,
    ):
        _cursor.execute(list_schemas())
        rsp = {v[0] for v in _cursor.fetchall()}
        schemas_present = rsp.intersection(v for v in Schema)
        # None of the schemas we create should be present yet.
        assert len(schemas_present) == 0


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_01_client_initilization_from_env(module_test_container: PostgresContainer):
    os.environ["PSYSCALE_HOST"] = module_test_container.get_container_host_ip()
    os.environ["PSYSCALE_PORT"] = str(
        module_test_container.get_exposed_port(module_test_container.port)
    )
    os.environ["PSYSCALE_USER"] = module_test_container.username
    os.environ["PSYSCALE_PASSWORD"] = module_test_container.password
    os.environ["PSYSCALE_DB_NAME"] = module_test_container.dbname

    db = PsyscaleDB()
    rtn, status = db.execute(sql.SQL("SELECT 1").format())

    assert rtn[0][0] == 1
    assert status == "SELECT 1"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_02_client_initilization_from_params(module_test_container: PostgresContainer):
    conn_params = PsyscaleConnectParams(
        module_test_container.get_container_host_ip(),
        module_test_container.get_exposed_port(module_test_container.port),
        module_test_container.username,
        module_test_container.password,
        module_test_container.dbname,
    )

    db = PsyscaleDB(conn_params)
    rtn, status = db.execute(sql.SQL("SELECT 1").format())

    assert rtn[0][0] == 1
    assert status == "SELECT 1"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_03_client_initilization_from_url(module_test_container: PostgresContainer):
    conn_params = PsyscaleConnectParams.from_url(
        module_test_container.get_connection_url()
    )
    db = PsyscaleDB(conn_params)
    rtn, status = db.execute(sql.SQL("SELECT 1").format())

    assert rtn[0][0] == 1
    assert status == "SELECT 1"


@pytest.fixture(scope="module")
def db(module_test_container: PostgresContainer):
    conn_params = PsyscaleConnectParams.from_url(
        module_test_container.get_connection_url()
    )
    yield PsyscaleDB(conn_params)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_04_schema_initilization(db: PsyscaleDB):
    rsp, status = db.execute(db[Op.SELECT, GenericTbls.SCHEMA]())
    schemas = {v[0] for v in rsp}
    assert schemas.issuperset(v for v in Schema)
    assert str(status).startswith("SELECT")


def test_05_security_schema_generation(db: PsyscaleDB):
    rsp, status = db.execute(db[Op.SELECT, GenericTbls.SCHEMA_TABLES](Schema.SECURITY))
    tables: set[str] = {v[0] for v in rsp}
    assert tables.issuperset({AssetTbls.SYMBOLS})
    assert str(status).startswith("SELECT")

    rsp, status = db.execute(db[Op.SELECT, GenericTbls.VIEW](Schema.SECURITY))
    tables: set[str] = {v[0] for v in rsp}
    assert tables.issuperset({AssetTbls._METADATA})
    assert str(status).startswith("SELECT")

    # Check _metadata sub-function exists
    rsp, status = db.execute(
        sql.SQL(
            """SELECT EXISTS (
                SELECT 1 FROM pg_proc
                WHERE proname = 'get_timeseries_date_range'
            );"""
        ).format()
    )

    assert rsp[0][0] is True
    assert str(status).startswith("SELECT")

    # Check trigram symbol search sub-function exists
    rsp, status = db.execute(
        sql.SQL(
            """SELECT EXISTS (
                SELECT 1 FROM pg_extension
                WHERE extname = 'pg_trgm'
            );"""
        ).format()
    )

    assert rsp[0][0] is True
    assert str(status).startswith("SELECT")
