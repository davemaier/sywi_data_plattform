"""Connection wrapper for SYWI DuckLake."""

from __future__ import annotations

import os

import duckdb
from dotenv import find_dotenv, load_dotenv


class ConnectionError(Exception):
    """Raised when connection to DuckLake fails."""

    pass


def _get_required_env(name: str) -> str:
    """Get required environment variable or raise."""
    value = os.getenv(name)
    if not value:
        raise ConnectionError(f"Required environment variable {name} is not set")
    return value


def _get_env(name: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.getenv(name, default)


def connect() -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with SYWI DuckLake databases attached.

    Automatically loads environment from .env.local and attaches:
    - `local`: Local DuckLake database
    - `remote`: Remote DuckLake database (read-only)

    Returns:
        DuckDB connection with local and remote databases attached.

    Raises:
        ConnectionError: If required environment variables are missing
            or connection to remote fails.

    Example:
        >>> import sywi_duckdb as duckdb
        >>> conn = duckdb.connect()
        >>> conn.execute("SELECT * FROM local.my_table").fetchdf()
        >>> conn.execute("SELECT * FROM remote.my_table").fetchdf()
    """
    load_dotenv(find_dotenv(".env"))
    load_dotenv(find_dotenv(".env.local"), override=True)

    local_catalog_dsn = _get_required_env("DUCKLAKE_CATALOG_DSN")
    local_data_path = _get_required_env("DUCKLAKE_DATA_PATH")
    remote_catalog_dsn = _get_required_env("DUCKLAKE_REMOTE_CATALOG_DSN")
    remote_data_path = _get_required_env("DUCKLAKE_REMOTE_DATA_PATH")

    s3_region = _get_env("DUCKLAKE_REMOTE_S3_REGION", "us-east-1")
    s3_endpoint = _get_required_env("DUCKLAKE_REMOTE_S3_ENDPOINT")
    s3_access_key_id = _get_required_env("DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID")
    s3_secret_access_key = _get_required_env("DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY")
    s3_url_style = _get_env("DUCKLAKE_REMOTE_S3_URL_STYLE", "path")
    s3_use_ssl = _get_env("DUCKLAKE_REMOTE_S3_USE_SSL", "false").lower() == "true"

    conn = duckdb.connect()

    try:
        conn.execute("INSTALL ducklake; INSTALL postgres; INSTALL httpfs;")
        conn.execute("LOAD ducklake; LOAD postgres; LOAD httpfs;")

        conn.execute(f"SET s3_region = '{s3_region}';")
        conn.execute(f"SET s3_endpoint = '{s3_endpoint}';")
        conn.execute(f"SET s3_url_style = '{s3_url_style}';")
        conn.execute(f"SET s3_access_key_id = '{s3_access_key_id}';")
        conn.execute(f"SET s3_secret_access_key = '{s3_secret_access_key}';")
        conn.execute(f"SET s3_use_ssl = {str(s3_use_ssl).lower()};")

        conn.execute(
            f"ATTACH 'ducklake:{local_catalog_dsn}' AS local "
            f"(DATA_PATH '{local_data_path}');"
        )

        conn.execute(
            f"ATTACH 'ducklake:postgres:{remote_catalog_dsn}' AS remote "
            f"(DATA_PATH '{remote_data_path}', READ_ONLY);"
        )

    except duckdb.Error as e:
        conn.close()
        raise ConnectionError(f"Failed to connect to DuckLake: {e}") from e

    return conn
