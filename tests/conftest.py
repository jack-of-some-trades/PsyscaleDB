import pytest
from testcontainers.postgres import PostgresContainer

from psycopg.rows import dict_row, tuple_row
from psyscale.client import TIMESCALE_IMAGE, PsyscaleConnectParams, PsyscaleDB
from psyscale.manager import PsyscaleMod


# ---- ---- Module Level Test Container ---- ----


@pytest.fixture(scope="module")
def module_test_container():
    "Create a Test Container that will one be used for a single module"
    with PostgresContainer(TIMESCALE_IMAGE, driver=None) as pg_container:
        yield pg_container


@pytest.fixture(scope="module")
def module_test_url(module_test_container):
    "URL to a test container that is generated per module"
    yield module_test_container.get_connection_url()


# ---- ---- Session Level Test Container and Client Instance ---- ----


@pytest.fixture(scope="session")
def test_container():
    "Spawn a Reusable Test container"
    with PostgresContainer(TIMESCALE_IMAGE, driver=None) as pg_container:
        yield pg_container


@pytest.fixture(scope="session")
def psyscalemod_inst(test_container):
    "PsyscaleDB Instance that can be reused for the entire session"
    yield PsyscaleMod(PsyscaleConnectParams.from_url(test_container.get_url()))


@pytest.fixture(scope="session")
def psyscaledb_inst(test_container):
    "PsyscaleDB Instance that can be reused for the entire session"
    yield PsyscaleDB(PsyscaleConnectParams.from_url(test_container.get_url()))


@pytest.fixture()
def tuple_cursor(psyscalemod_inst: PsyscaleMod):
    "Return a tuple cursor that will have all modifications rolledback"
    with psyscalemod_inst._pool.connection() as conn:
        conn.set_autocommit(False)
        with conn.cursor(row_factory=tuple_row) as cursor:
            yield cursor
        conn.rollback()
        conn.set_autocommit(True)


@pytest.fixture()
def dict_cursor(psyscalemod_inst: PsyscaleMod):
    "Return a dict cursor that will have all modifications rolledback"
    with psyscalemod_inst._pool.connection() as conn:
        conn.set_autocommit(False)
        with conn.cursor(row_factory=dict_row) as cursor:
            yield cursor
        conn.rollback()
        conn.set_autocommit(True)
