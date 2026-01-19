"""DuckLake resource for direct table access and updates."""

from contextlib import contextmanager
from typing import Iterator

import duckdb
from dagster import ConfigurableResource
from pydantic import Field


def _is_postgres_mode(dsn: str) -> bool:
    """Detect if DSN is for Postgres mode (vs local file mode)."""
    return "host=" in dsn


class DuckLakeResource(ConfigurableResource):
    """Resource for connecting to DuckLake with full configuration.

    Supports two modes:
    - Local mode: DSN is a file path (e.g., './data/ducklake.ducklake')
    - Postgres mode: DSN contains 'host=' (e.g., 'dbname=... host=localhost ...')

    In local mode, S3 configuration is not needed.
    In Postgres mode, S3 configuration is required for remote storage.
    """

    duckdb_database: str = Field(
        default=":memory:", description="Path to DuckDB database file or ':memory:'"
    )
    ducklake_catalog_dsn: str = Field(
        description="DuckLake catalog: file path for local mode, or Postgres DSN for remote mode"
    )
    ducklake_data_path: str = Field(
        description="Data path: local directory (e.g., './data/') or S3 path (e.g., 's3://bucket/path/')"
    )
    ducklake_schema: str = Field(
        default="my_ducklake", description="Schema name for DuckLake tables"
    )

    # S3 configuration - only required for Postgres mode with S3 storage
    s3_region: str | None = Field(default=None)
    s3_endpoint: str | None = Field(default=None)
    s3_access_key_id: str | None = Field(default=None)
    s3_secret_access_key: str | None = Field(default=None)
    s3_url_style: str | None = Field(default=None)
    s3_use_ssl: bool | None = Field(default=None)

    def _is_s3_storage(self) -> bool:
        """Check if data path is S3."""
        return self.ducklake_data_path.startswith("s3://")

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Get a configured DuckDB connection with DuckLake attached.

        Usage:
            with ducklake.get_connection() as conn:
                df = conn.execute("SELECT * FROM my_ducklake.my_table").fetchdf()
        """
        conn = duckdb.connect(self.duckdb_database)
        is_postgres = _is_postgres_mode(self.ducklake_catalog_dsn)

        try:
            # Install and load ducklake (always needed)
            conn.execute("INSTALL ducklake;")
            conn.execute("LOAD ducklake;")

            if is_postgres:
                # Postgres mode - need postgres extension
                conn.execute("INSTALL postgres;")
                conn.execute("LOAD postgres;")

            if self._is_s3_storage():
                # S3 storage - need httpfs and S3 config
                conn.execute("INSTALL httpfs;")
                conn.execute("LOAD httpfs;")

                conn.execute(f"SET s3_region = '{self.s3_region}';")
                conn.execute(f"SET s3_endpoint = '{self.s3_endpoint}';")
                conn.execute(f"SET s3_url_style = '{self.s3_url_style}';")
                conn.execute(f"SET s3_access_key_id = '{self.s3_access_key_id}';")
                conn.execute(
                    f"SET s3_secret_access_key = '{self.s3_secret_access_key}';"
                )
                conn.execute(f"SET s3_use_ssl = {str(self.s3_use_ssl).lower()};")

            # Attach DuckLake
            if is_postgres:
                conn.execute(
                    f"""
                    ATTACH 'ducklake:postgres:{self.ducklake_catalog_dsn}'
                    AS {self.ducklake_schema};
                    """
                )
            else:
                conn.execute(
                    f"""
                    ATTACH 'ducklake:{self.ducklake_catalog_dsn}'
                    AS {self.ducklake_schema};
                    """
                )

            yield conn

        finally:
            conn.close()

    def get_table_columns(self, table_name: str) -> list[str]:
        """Get list of column names for a table.

        Args:
            table_name: Name of the table (without schema prefix)

        Returns:
            List of column names
        """
        with self.get_connection() as conn:
            full_table_name = f"{self.ducklake_schema}.{table_name}"
            result = conn.execute(f"DESCRIBE {full_table_name}").fetchall()
            return [row[0] for row in result]

    def add_column_if_not_exists(
        self, table_name: str, column_name: str, column_type: str = "VARCHAR"
    ) -> bool:
        """Add a column to a table if it doesn't already exist.

        Args:
            table_name: Name of the table (without schema prefix)
            column_name: Name of the column to add
            column_type: SQL type for the column (default: VARCHAR)

        Returns:
            True if column was added, False if it already existed
        """
        with self.get_connection() as conn:
            full_table_name = f"{self.ducklake_schema}.{table_name}"

            # Check if column exists
            existing_columns = self.get_table_columns(table_name)
            if column_name in existing_columns:
                return False

            # Add the column
            conn.execute(
                f"ALTER TABLE {full_table_name} ADD COLUMN {column_name} {column_type}"
            )
            return True
