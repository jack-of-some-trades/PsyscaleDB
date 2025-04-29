import pytest
from testcontainers.postgres import PostgresContainer

from psyscale import PsyscaleDB
from psyscale.core import TIMESCALE_IMAGE, PsyscaleConnectParams

# ---- ---- Module Level Test Container ---- ----


@pytest.fixture(scope="module")
def test_container():
    "Create a Test Container that will one be used for a single module"
    with PostgresContainer(TIMESCALE_IMAGE, driver=None) as pg_container:
        yield pg_container


@pytest.fixture(scope="module")
def test_url(test_container):
    "URL to a test container that is generated per module"
    yield test_container.get_connection_url()


@pytest.fixture(scope="module")
def psyscale_db(test_url):
    "PsyscaleDB Instance that can be reused for the entire session"
    yield PsyscaleDB(PsyscaleConnectParams.from_url(test_url))
