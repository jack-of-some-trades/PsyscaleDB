import pytest
from testcontainers.postgres import PostgresContainer

from psyscale.client import TIMESCALE_IMAGE, PsyscaleConnectParams
from psyscale.manager import PsyscaleMod


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
    yield PsyscaleMod(PsyscaleConnectParams.from_url(test_url))
