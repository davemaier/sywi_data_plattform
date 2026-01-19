"""__NAME__ project definitions - self-contained Dagster definitions."""

import os

from dagster import Definitions, with_source_code_references

from sywi_core import ducklake_io_manager, DuckLakeResource
from __name__.assets import example_asset


def _get_ducklake_config() -> dict:
    """Build DuckLake configuration from environment variables."""
    config = {
        "duckdb_database": os.environ.get("DUCKLAKE_DUCKDB_DATABASE", ":memory:"),
        "ducklake_catalog_dsn": os.environ["DUCKLAKE_CATALOG_DSN"],
        "ducklake_data_path": os.environ["DUCKLAKE_DATA_PATH"],
        "ducklake_schema": os.environ.get("DUCKLAKE_SCHEMA", "my_ducklake"),
    }

    if config["ducklake_data_path"].startswith("s3://"):
        config.update(
            {
                "s3_region": os.environ.get("DUCKLAKE_S3_REGION"),
                "s3_endpoint": os.environ.get("DUCKLAKE_S3_ENDPOINT"),
                "s3_access_key_id": os.environ.get("DUCKLAKE_S3_ACCESS_KEY_ID"),
                "s3_secret_access_key": os.environ.get("DUCKLAKE_S3_SECRET_ACCESS_KEY"),
                "s3_url_style": os.environ.get("DUCKLAKE_S3_URL_STYLE", "path"),
                "s3_use_ssl": os.environ.get("DUCKLAKE_S3_USE_SSL", "false").lower()
                == "true",
            }
        )

    return config


def _get_ducklake_resource() -> DuckLakeResource:
    """Build DuckLakeResource from environment variables."""
    kwargs = {
        "duckdb_database": os.environ.get("DUCKLAKE_DUCKDB_DATABASE", ":memory:"),
        "ducklake_catalog_dsn": os.environ["DUCKLAKE_CATALOG_DSN"],
        "ducklake_data_path": os.environ["DUCKLAKE_DATA_PATH"],
        "ducklake_schema": os.environ.get("DUCKLAKE_SCHEMA", "my_ducklake"),
    }

    if kwargs["ducklake_data_path"].startswith("s3://"):
        kwargs.update(
            {
                "s3_region": os.environ.get("DUCKLAKE_S3_REGION"),
                "s3_endpoint": os.environ.get("DUCKLAKE_S3_ENDPOINT"),
                "s3_access_key_id": os.environ.get("DUCKLAKE_S3_ACCESS_KEY_ID"),
                "s3_secret_access_key": os.environ.get("DUCKLAKE_S3_SECRET_ACCESS_KEY"),
                "s3_url_style": os.environ.get("DUCKLAKE_S3_URL_STYLE", "path"),
                "s3_use_ssl": os.environ.get("DUCKLAKE_S3_USE_SSL", "false").lower()
                == "true",
            }
        )

    return DuckLakeResource(**kwargs)


defs = Definitions(
    assets=with_source_code_references(
        [
            example_asset,
        ]
    ),
    resources={
        "io_manager": ducklake_io_manager.configured(_get_ducklake_config()),
        "ducklake": _get_ducklake_resource(),
    },
)
