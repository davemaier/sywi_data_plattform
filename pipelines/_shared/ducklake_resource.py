"""DuckLake resource for direct table access and updates."""

from contextlib import contextmanager
from typing import Iterator

import duckdb
from dagster import ConfigurableResource
from pydantic import Field


class DuckLakeResource(ConfigurableResource):
    """Resource for connecting to DuckLake with full configuration.

    Provides direct access to DuckLake tables for reading and updating,
    bypassing the IOManager when you need more control.
    """

    duckdb_database: str = Field(
        default=":memory:", description="Path to DuckDB database file or ':memory:'"
    )
    ducklake_catalog_dsn: str = Field(
        description="DuckLake catalog connection string (e.g., 'dbname=ducklake_catalog host=localhost user=traindata password=traindata port=5433')"
    )
    ducklake_data_path: str = Field(
        description="S3 data path for DuckLake (e.g., 's3://ducklake/data/')"
    )
    s3_region: str = Field(default="us-east-1")
    s3_endpoint: str = Field(description="S3/MinIO endpoint (e.g., '127.0.0.1:9000')")
    s3_access_key_id: str = Field(description="S3 access key")
    s3_secret_access_key: str = Field(description="S3 secret key")
    s3_url_style: str = Field(default="path")
    s3_use_ssl: bool = Field(default=False)
    ducklake_schema: str = Field(
        default="my_ducklake", description="Schema name for DuckLake tables"
    )

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Get a configured DuckDB connection with DuckLake attached.

        Usage:
            with ducklake.get_connection() as conn:
                df = conn.execute("SELECT * FROM my_ducklake.my_table").fetchdf()
        """
        conn = duckdb.connect(self.duckdb_database)

        try:
            # Install and load extensions
            conn.execute("INSTALL ducklake;")
            conn.execute("INSTALL postgres;")
            conn.execute("INSTALL httpfs;")
            conn.execute("INSTALL vss;")
            conn.execute("LOAD ducklake;")
            conn.execute("LOAD postgres;")
            conn.execute("LOAD httpfs;")
            conn.execute("LOAD vss;")

            # Configure S3/MinIO
            conn.execute(f"SET s3_region = '{self.s3_region}';")
            conn.execute(f"SET s3_endpoint = '{self.s3_endpoint}';")
            conn.execute(f"SET s3_url_style = '{self.s3_url_style}';")
            conn.execute(f"SET s3_access_key_id = '{self.s3_access_key_id}';")
            conn.execute(f"SET s3_secret_access_key = '{self.s3_secret_access_key}';")
            conn.execute(f"SET s3_use_ssl = {str(self.s3_use_ssl).lower()};")

            # Attach DuckLake
            conn.execute(
                f"""
                ATTACH 'ducklake:postgres:{self.ducklake_catalog_dsn}'
                AS {self.ducklake_schema}
                (DATA_PATH '{self.ducklake_data_path}');
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
