from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd
from dagster import (
    Field as DagsterField,
    IOManager,
    InputContext,
    OutputContext,
    InitResourceContext,
    io_manager,
)


def _is_postgres_mode(dsn: str) -> bool:
    """Detect if DSN is for Postgres mode (vs local file mode)."""
    return "host=" in dsn


class DuckLakeIOManager(IOManager):
    def __init__(
        self,
        duckdb_database: str,
        ducklake_catalog_dsn: str,
        ducklake_data_path: str,
        ducklake_schema: str = "my_ducklake",
        s3_region: str | None = None,
        s3_endpoint: str | None = None,
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_url_style: str | None = None,
        s3_use_ssl: bool | None = None,
    ) -> None:
        """
        IOManager that stores assets as tables in DuckLake.

        Supports two modes:
        - Local mode: DSN is a file path (e.g., './data/ducklake.ducklake')
        - Postgres mode: DSN contains 'host=' (e.g., 'dbname=... host=localhost ...')

        Parameters
        ----------
        duckdb_database:
            Path to DuckDB database file or ':memory:'.
        ducklake_catalog_dsn:
            File path for local mode, or Postgres DSN for remote mode.
        ducklake_data_path:
            Local directory (e.g., './data/') or S3 path (e.g., 's3://ducklake/data/').
        ducklake_schema:
            Name of the attached DuckLake schema in DuckDB (AS <schema>).
        s3_*:
            S3/MinIO configuration - only required when ducklake_data_path is S3.
        """
        self._duckdb_database = duckdb_database
        self._ducklake_catalog_dsn = ducklake_catalog_dsn
        self._ducklake_data_path = ducklake_data_path
        self._ducklake_schema = ducklake_schema
        self._s3_region = s3_region
        self._s3_endpoint = s3_endpoint
        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_access_key = s3_secret_access_key
        self._s3_url_style = s3_url_style
        self._s3_use_ssl = s3_use_ssl

    def _is_s3_storage(self) -> bool:
        """Check if data path is S3."""
        return self._ducklake_data_path.startswith("s3://")

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        """
        Return a DuckDB connection with DuckLake attached.
        """
        conn = duckdb.connect(self._duckdb_database)
        is_postgres = _is_postgres_mode(self._ducklake_catalog_dsn)

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

            conn.execute(f"SET s3_region = '{self._s3_region}';")
            conn.execute(f"SET s3_endpoint = '{self._s3_endpoint}';")
            conn.execute(f"SET s3_url_style = '{self._s3_url_style}';")
            conn.execute(f"SET s3_access_key_id = '{self._s3_access_key_id}';")
            conn.execute(f"SET s3_secret_access_key = '{self._s3_secret_access_key}';")
            conn.execute(f"SET s3_use_ssl = {str(self._s3_use_ssl).lower()};")

        # Attach DuckLake
        if is_postgres:
            conn.execute(
                f"""
                ATTACH 'ducklake:postgres:{self._ducklake_catalog_dsn}'
                AS {self._ducklake_schema}
                (DATA_PATH '{self._ducklake_data_path}');
                """
            )
        else:
            conn.execute(
                f"""
                ATTACH 'ducklake:{self._ducklake_catalog_dsn}'
                AS {self._ducklake_schema}
                (DATA_PATH '{self._ducklake_data_path}');
                """
            )

        return conn

    def _table_name_for_context(self, context: OutputContext | InputContext) -> str:
        # Simple mapping: asset key -> table name
        # e.g. asset key ["hackernews", "save_to_ducklake"] -> "save_to_ducklake"
        return context.asset_key.path[-1]

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """
        Store the asset output in DuckLake as a table.
        Currently expects a Pandas DataFrame-like object.
        """
        table_name = self._table_name_for_context(context)
        full_table_name = f"{self._ducklake_schema}.{table_name}"

        context.log.info(f"[DuckLakeIOManager] Writing table {full_table_name}")

        conn = self._get_conn()
        try:
            # Normalize obj to a DataFrame
            if isinstance(obj, pd.DataFrame):
                df = obj
            else:
                # Fallback: try to construct a DataFrame from generic sequences / records.
                df = pd.DataFrame(obj)

            # Register temp view for the DataFrame
            conn.register("asset_df", df)

            # Check if table exists by trying to query it
            table_exists = False
            try:
                conn.execute(f"SELECT 1 FROM {full_table_name} LIMIT 1")
                table_exists = True
                context.log.info(f"Table {full_table_name} exists, replacing data")
            except Exception:
                context.log.info(f"Table {full_table_name} does not exist, creating it")

            if table_exists:
                # Add any new columns that exist in DataFrame but not in table
                # Get existing table columns
                existing_cols = conn.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
                ).fetchall()
                existing_col_names = {row[0] for row in existing_cols}

                # Add missing columns
                for col in df.columns:
                    if col not in existing_col_names:
                        # Infer DuckDB type from pandas dtype
                        dtype = df[col].dtype
                        if dtype == "bool":
                            duck_type = "BOOLEAN"
                        elif dtype == "int64":
                            duck_type = "BIGINT"
                        elif dtype == "float64":
                            duck_type = "DOUBLE"
                        else:
                            duck_type = "VARCHAR"
                        context.log.info(
                            f"Adding new column {col} ({duck_type}) to {full_table_name}"
                        )
                        conn.execute(
                            f"ALTER TABLE {full_table_name} ADD COLUMN {col} {duck_type}"
                        )

                # Delete all rows and insert new data
                conn.execute(f"DELETE FROM {full_table_name};")
                conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM asset_df")
            else:
                # Create new table from DataFrame
                conn.execute(
                    f"""
                    CREATE TABLE {full_table_name} AS
                    SELECT * FROM asset_df
                    """
                )

            # Get row count for metadata
            row_count_result = conn.execute(
                f"SELECT COUNT(*) FROM {full_table_name}"
            ).fetchone()
            row_count = row_count_result[0] if row_count_result else 0

            context.add_output_metadata(
                {
                    "ducklake_table": full_table_name,
                    "row_count": row_count,
                    "data_path": self._ducklake_data_path,
                }
            )

        finally:
            conn.close()

    def load_input(self, context: InputContext) -> Any:
        """
        Load an upstream asset from DuckLake as a Pandas DataFrame.
        """
        if context.upstream_output is None:
            raise ValueError("No upstream output found")

        upstream_key = context.upstream_output.asset_key
        table_name = upstream_key.path[-1]
        full_table_name = f"{self._ducklake_schema}.{table_name}"

        context.log.info(f"[DuckLakeIOManager] Reading table {full_table_name}")

        conn = self._get_conn()
        try:
            df = conn.execute(f"SELECT * FROM {full_table_name}").fetchdf()
            return df
        finally:
            conn.close()


@io_manager(
    config_schema={
        "duckdb_database": str,
        "ducklake_catalog_dsn": str,
        "ducklake_data_path": str,
        "ducklake_schema": str,
        # S3 config - optional, only needed for S3 storage
        "s3_region": DagsterField(str, is_required=False),
        "s3_endpoint": DagsterField(str, is_required=False),
        "s3_access_key_id": DagsterField(str, is_required=False),
        "s3_secret_access_key": DagsterField(str, is_required=False),
        "s3_url_style": DagsterField(str, is_required=False),
        "s3_use_ssl": DagsterField(bool, is_required=False),
    }
)
def ducklake_io_manager(init_context: InitResourceContext) -> DuckLakeIOManager:
    cfg = init_context.resource_config
    return DuckLakeIOManager(
        duckdb_database=cfg["duckdb_database"],
        ducklake_catalog_dsn=cfg["ducklake_catalog_dsn"],
        ducklake_data_path=cfg["ducklake_data_path"],
        ducklake_schema=cfg["ducklake_schema"],
        s3_region=cfg.get("s3_region"),
        s3_endpoint=cfg.get("s3_endpoint"),
        s3_access_key_id=cfg.get("s3_access_key_id"),
        s3_secret_access_key=cfg.get("s3_secret_access_key"),
        s3_url_style=cfg.get("s3_url_style"),
        s3_use_ssl=cfg.get("s3_use_ssl"),
    )
