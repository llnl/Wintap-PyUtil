from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.storage import S3Settings, open_s3


@pytest.fixture
def backend(monkeypatch):
    for name in (
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(name, raising=False)
    identity = SimpleNamespace(
        access_key="access'key", secret_key="private'key", token="session'token"
    )
    credentials = Mock()
    credentials.get_frozen_credentials.return_value = identity
    client = Mock()
    client.meta.endpoint_url = "https://s3.us-west-2.amazonaws.com"
    session = Mock(region_name="us-west-2")
    session.get_credentials.return_value = credentials
    session.client.return_value = client
    factory = Mock(return_value=session)
    monkeypatch.setattr("raw_sensor_compact.storage.boto3.Session", factory)
    return SimpleNamespace(
        conn=Mock(),
        session=session,
        client=client,
        factory=factory,
        credentials=credentials,
        identity=identity,
    )


def test_same_frozen_credentials_endpoint_and_profile(backend):
    b = backend
    assert open_s3(b.conn, S3Settings(profile="research")) is b.client
    b.factory.assert_called_once_with(profile_name="research")
    b.credentials.get_frozen_credentials.assert_called_once_with()
    kwargs = b.session.client.call_args.kwargs
    assert kwargs["aws_access_key_id"] == b.identity.access_key
    assert kwargs["aws_secret_access_key"] == b.identity.secret_key
    assert kwargs["aws_session_token"] == b.identity.token
    assert kwargs["config"].s3 == {"addressing_style": "path"}
    sql = b.conn.execute.call_args.args[0]
    assert "CREATE TEMPORARY SECRET" in sql
    assert "KEY_ID 'access''key'" in sql
    assert "SECRET 'private''key'" in sql
    assert "SESSION_TOKEN 'session''token'" in sql
    assert "ENDPOINT 's3.us-west-2.amazonaws.com'" in sql
    assert "USE_SSL true" in sql


@pytest.mark.parametrize("scheme", ["http", "https"])
def test_custom_endpoint_tls_region_and_extension_cache(backend, scheme):
    b = backend
    endpoint = f"{scheme}://localhost:9000"
    b.client.meta.endpoint_url = endpoint
    b.conn.load_extension.side_effect = [RuntimeError("not cached"), None]
    open_s3(
        b.conn,
        S3Settings(
            region="test-region",
            endpoint_url=endpoint,
            extension_directory="/private/cache",
        ),
    )
    assert b.session.client.call_args.kwargs["endpoint_url"] == endpoint
    assert b.session.client.call_args.kwargs["region_name"] == "test-region"
    b.conn.install_extension.assert_called_once_with("httpfs")
    assert b.conn.load_extension.call_count == 2
    assert b.conn.execute.call_args_list[0].args == (
        "SET extension_directory = ?",
        ["/private/cache"],
    )
    sql = b.conn.execute.call_args.args[0]
    assert "ENDPOINT 'localhost:9000'" in sql
    assert f"USE_SSL {'true' if scheme == 'https' else 'false'}" in sql


def test_service_endpoint_environment_precedence(backend, monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://global:9000")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://service:9000")
    backend.client.meta.endpoint_url = "https://service:9000"
    open_s3(backend.conn, S3Settings())
    assert backend.session.client.call_args.kwargs["endpoint_url"] == (
        "https://service:9000"
    )
    backend.identity.token = None
    open_s3(backend.conn, S3Settings(endpoint_url="https://explicit:9000"))
    assert backend.session.client.call_args.kwargs["endpoint_url"] == (
        "https://explicit:9000"
    )
    assert "SESSION_TOKEN" not in backend.conn.execute.call_args.args[0]


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://host",
        "https://user:private@host",
        "https://host/path",
        "https://host/?secret=private",
        "https://host/#private",
    ],
)
def test_invalid_origin_never_creates_session(backend, endpoint):
    with pytest.raises(CompactionError):
        open_s3(backend.conn, S3Settings(endpoint_url=endpoint))
    backend.factory.assert_not_called()


def test_setup_errors_suppress_secrets(backend):
    backend.conn.execute.side_effect = RuntimeError("private'key session'token")
    with pytest.raises(CompactionError, match="suppressed") as error:
        open_s3(backend.conn, S3Settings())
    assert "private'key" not in str(error.value)
    assert "session'token" not in str(error.value)


def test_no_credentials_fails_before_extensions(backend):
    backend.session.get_credentials.return_value = None
    with pytest.raises(CompactionError, match="No AWS credentials"):
        open_s3(backend.conn, S3Settings())
    backend.conn.load_extension.assert_not_called()
