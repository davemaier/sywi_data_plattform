from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd
from dagster import (
    IOManager,
    InputContext,
    OutputContext,
    InitResourceContext,
    io_manager,
)


class DuckLakeIOManager(IOManager):
    def __init__(
        self,
        duckdb_database: str,
        ducklake_catalog_dsn: str,
        ducklake_data_path: str,
        s3_region: str,
        s3_endpoint: str,
        s3_access_key_id: str,
        s3_secret_access_key: str,
        s3_url_style: str = "path",
        s3_use_ssl: bool = False,
        ducklake_schema: str = "my_ducklake",
    ) -> None:
        """
        IOManager that stores assets as tables in DuckLake.

        Parameters
        ----------
        duckdb_database:
            Path to DuckDB database file or ':memory:'.
        ducklake_catalog_dsn:
            Connection string part for the DuckLake catalog, e.g.
            'dbname=ducklake_catalog host=localhost user=traindata password=traindata port=5433'.
        ducklake_data_path:
            Data path for DuckLake, e.g. 's3://ducklake/data/'.
        s3_*:
            MinIO / S3 configuration used by DuckDB httpfs.
        ducklake_schema:
            Name of the attached DuckLake schema in DuckDB (AS <schema>).
        """
        self._duckdb_database = duckdb_database
        self._ducklake_catalog_dsn = ducklake_catalog_dsn
        self._ducklake_data_path = ducklake_data_path
        self._s3_region = s3_region
        self._s3_endpoint = s3_endpoint
        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_access_key = s3_secret_access_key
        self._s3_url_style = s3_url_style
        self._s3_use_ssl = s3_use_ssl
        self._ducklake_schema = ducklake_schema

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        """
        Return a DuckDB connection with DuckLake attached and S3 configured.
        """
        conn = duckdb.connect(self._duckdb_database)

        # Install + load required extensions
        conn.execute("INSTALL ducklake;")
        conn.execute("INSTALL postgres;")
        conn.execute("INSTALL httpfs;")
        conn.execute("INSTALL vss;")
        conn.execute("LOAD ducklake;")
        conn.execute("LOAD postgres;")
        conn.execute("LOAD httpfs;")
        conn.execute("LOAD vss;")

        # S3 / MinIO config
        conn.execute(f"SET s3_region = '{self._s3_region}';")
        conn.execute(f"SET s3_endpoint = '{self._s3_endpoint}';")
        conn.execute(f"SET s3_url_style = '{self._s3_url_style}';")
        conn.execute(f"SET s3_access_key_id = '{self._s3_access_key_id}';")
        conn.execute(f"SET s3_secret_access_key = '{self._s3_secret_access_key}';")
        conn.execute(f"SET s3_use_ssl = {str(self._s3_use_ssl).lower()};")

        # Attach DuckLake
        conn.execute(
            f"""
            ATTACH 'ducklake:postgres:{self._ducklake_catalog_dsn}'
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
                    "s3_data_path": self._ducklake_data_path,
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
        "s3_region": str,
        "s3_endpoint": str,
        "s3_access_key_id": str,
        "s3_secret_access_key": str,
        "s3_url_style": str,
        "s3_use_ssl": bool,
        "ducklake_schema": str,
    }
)
def ducklake_io_manager(init_context: InitResourceContext) -> DuckLakeIOManager:
    cfg = init_context.resource_config
    return DuckLakeIOManager(
        duckdb_database=cfg["duckdb_database"],
        ducklake_catalog_dsn=cfg["ducklake_catalog_dsn"],
        ducklake_data_path=cfg["ducklake_data_path"],
        s3_region=cfg["s3_region"],
        s3_endpoint=cfg["s3_endpoint"],
        s3_access_key_id=cfg["s3_access_key_id"],
        s3_secret_access_key=cfg["s3_secret_access_key"],
        s3_url_style=cfg["s3_url_style"],
        s3_use_ssl=cfg["s3_use_ssl"],
        ducklake_schema=cfg["ducklake_schema"],
    )
