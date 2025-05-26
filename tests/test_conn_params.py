import os
from unittest.mock import patch
from urllib.parse import quote_plus

import pytest
from psyscale import PsyscaleConnectParams


def test_direct_instantiation_sets_correct_url():
    params = PsyscaleConnectParams(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_pass",
        database="test_db",
        sslmode="require",
        application_name="psyscale_test",
    )
    encoded_user = quote_plus("test_user")
    encoded_pass = quote_plus("test_pass")
    expected_url = (
        f"postgresql://{encoded_user}:{encoded_pass}@localhost:5432/test_db"
        f"?sslmode=require&application_name=psyscale_test"
    )
    assert params.url == expected_url
    assert params.is_local


def test_direct_instantiation_without_sslmode_or_app_name():
    params = PsyscaleConnectParams(host="127.0.0.1", port=5432, user="admin", password="admin123", database="db")
    assert "sslmode" not in params.url
    assert "application_name" not in params.url
    assert params.is_local


def test_from_url_parses_correctly():
    url = "postgresql://user:pass@myhost:5432/mydb?sslmode=disable&application_name=test"
    params = PsyscaleConnectParams.from_url(url)

    assert params.host == "myhost"
    assert params.port == 5432
    assert params.user == "user"
    assert params.password == "pass"
    assert params.database == "mydb"
    assert params.sslmode == "disable"
    assert params.application_name == "test"
    assert not params.is_local


def test_from_url_invalid_scheme_raises():
    with pytest.raises(ValueError, match="Invalid URL scheme"):
        PsyscaleConnectParams.from_url("mysql://user:pass@localhost:3306/db")


@patch.dict(
    os.environ,
    {
        "PSYSCALE_HOST": "localhost",
        "PSYSCALE_PORT": "5432",
        "PSYSCALE_USER": "testuser",
        "PSYSCALE_PASSWORD": "testpw",
        "PSYSCALE_DB_NAME": "testdb",
        "PSYSCALE_SSLMODE": "verify-full",
        "PSYSCALE_APP_NAME": "env_app",
        "PSYSCALE_VOLUME_PATH": "/data/db",
    },
    clear=True,
)
def test_from_env_without_url_uses_individual_vars():
    params = PsyscaleConnectParams.from_env()

    assert params.host == "localhost"
    assert params.port == 5432
    assert params.user == "testuser"
    assert params.password == "testpw"
    assert params.database == "testdb"
    assert params.sslmode == "verify-full"
    assert params.application_name == "env_app"
    assert params.volume_path == "/data/db"
    assert params.is_local


@patch.dict(
    os.environ,
    {"PSYSCALE_URL": "postgresql://u:p@host:1234/db?sslmode=require&application_name=env_url"},
    clear=True,
)
def test_from_env_with_url_parses_correctly():
    params = PsyscaleConnectParams.from_env()

    assert params.host == "host"
    assert params.port == 1234
    assert params.user == "u"
    assert params.password == "p"
    assert params.database == "db"
    assert params.sslmode == "require"
    assert params.application_name == "env_url"
    assert not params.volume_path
    assert not params.is_local


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_is_local_true_for_local_hosts(host):
    params = PsyscaleConnectParams(host=host, port=5432, user="u", password="p", database="db")
    assert params.is_local


def test_is_local_false_for_remote_host():
    params = PsyscaleConnectParams(host="remote-db.example.com", port=5432, user="u", password="p", database="db")
    assert not params.is_local
