import os
import sys

# Add _shared to path for imports
sys.path.insert(0, "/opt/dagster/app")

from dagster import Definitions

from _shared.ducklake_io_manager import ducklake_io_manager
from _shared.ducklake_resource import DuckLakeResource

from assets.ingestion import hackernews_top_stories, hackernews_titles
from assets.storage import hackernews_stories
from assets.export_csv import save_to_csv

defs = Definitions(
    assets=[
        hackernews_top_stories,
        hackernews_titles,
        hackernews_stories,
        save_to_csv,
    ],
    resources={
        "io_manager": ducklake_io_manager.configured(
            {
                "duckdb_database": os.environ.get(
                    "DUCKLAKE_DUCKDB_DATABASE", ":memory:"
                ),
                "ducklake_catalog_dsn": os.environ["DUCKLAKE_CATALOG_DSN"],
                "ducklake_data_path": os.environ["DUCKLAKE_DATA_PATH"],
                "s3_region": os.environ["DUCKLAKE_S3_REGION"],
                "s3_endpoint": os.environ["DUCKLAKE_S3_ENDPOINT"],
                "s3_access_key_id": os.environ["DUCKLAKE_S3_ACCESS_KEY_ID"],
                "s3_secret_access_key": os.environ["DUCKLAKE_S3_SECRET_ACCESS_KEY"],
                "s3_url_style": os.environ.get("DUCKLAKE_S3_URL_STYLE", "path"),
                "s3_use_ssl": os.environ.get("DUCKLAKE_S3_USE_SSL", "false").lower()
                == "true",
                "ducklake_schema": os.environ.get("DUCKLAKE_SCHEMA", "my_ducklake"),
            }
        ),
        "ducklake": DuckLakeResource(
            duckdb_database=os.environ.get("DUCKLAKE_DUCKDB_DATABASE", ":memory:"),
            ducklake_catalog_dsn=os.environ["DUCKLAKE_CATALOG_DSN"],
            ducklake_data_path=os.environ["DUCKLAKE_DATA_PATH"],
            s3_region=os.environ["DUCKLAKE_S3_REGION"],
            s3_endpoint=os.environ["DUCKLAKE_S3_ENDPOINT"],
            s3_access_key_id=os.environ["DUCKLAKE_S3_ACCESS_KEY_ID"],
            s3_secret_access_key=os.environ["DUCKLAKE_S3_SECRET_ACCESS_KEY"],
            s3_url_style=os.environ.get("DUCKLAKE_S3_URL_STYLE", "path"),
            s3_use_ssl=os.environ.get("DUCKLAKE_S3_USE_SSL", "false").lower() == "true",
            ducklake_schema=os.environ.get("DUCKLAKE_SCHEMA", "my_ducklake"),
        ),
    },
)
