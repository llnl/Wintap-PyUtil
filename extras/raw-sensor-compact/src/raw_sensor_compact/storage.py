"""One credential identity shared by boto3 listing and temporary DuckDB secrets."""

import os
from dataclasses import dataclass
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

from .config import CompactionError


@dataclass(frozen=True)
class S3Settings:
    profile: str | None = None
    region: str | None = None
    endpoint_url: str | None = None
    extension_directory: str | None = None


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_s3(conn, settings: S3Settings):
    endpoint_url = (
        settings.endpoint_url
        or os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
    )
    endpoint = urlsplit(endpoint_url) if endpoint_url else None
    if endpoint and (
        endpoint.scheme not in {"http", "https"}
        or not endpoint.hostname
        or endpoint.username
        or endpoint.password
        or endpoint.path not in {"", "/"}
        or endpoint.query
        or endpoint.fragment
    ):
        raise CompactionError(
            "S3 endpoint must be an HTTP(S) origin without credentials"
        )
    try:
        session = boto3.Session(profile_name=settings.profile)
        credentials = session.get_credentials()
        if credentials is None:
            raise CompactionError("No AWS credentials resolved")
        credentials = credentials.get_frozen_credentials()
        region = (
            settings.region
            or os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or session.region_name
            or "us-east-1"
        )
        client = session.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            aws_session_token=credentials.token,
            config=Config(
                s3={"addressing_style": "path"},
                connect_timeout=10,
                read_timeout=30,
                retries={"mode": "standard", "total_max_attempts": 3},
            ),
        )
        # Include endpoints supplied by profile configuration; both engines use
        # the SDK's resolved origin, path-style addressing and frozen identity.
        endpoint = urlsplit(client.meta.endpoint_url)
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.path not in {"", "/"}
            or endpoint.query
            or endpoint.fragment
        ):
            raise CompactionError("Resolved S3 endpoint is not an HTTP(S) origin")
        _ = endpoint.port  # Validate malformed/out-of-range ports before creating SQL.
        if settings.extension_directory:
            conn.execute(
                "SET extension_directory = ?", [settings.extension_directory]
            ).fetchall()
        try:
            conn.load_extension("httpfs")
        except Exception:
            conn.install_extension("httpfs")
            conn.load_extension("httpfs")
        fields = [
            "TYPE S3",
            "KEY_ID " + quote(credentials.access_key),
            "SECRET " + quote(credentials.secret_key),
            "REGION " + quote(region),
        ]
        if credentials.token:
            fields.append("SESSION_TOKEN " + quote(credentials.token))
        if endpoint:
            fields.extend(
                [
                    "ENDPOINT " + quote(endpoint.netloc),
                    "URL_STYLE 'path'",
                    "USE_SSL " + ("true" if endpoint.scheme == "https" else "false"),
                ]
            )
        conn.execute(
            "CREATE TEMPORARY SECRET compact_s3 (" + ", ".join(fields) + ")"
        ).fetchall()
        return client
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "S3 setup failed; check profile, credentials, endpoint, TLS "
            "and httpfs cache. "
            "Details suppressed to protect credentials"
        ) from None
