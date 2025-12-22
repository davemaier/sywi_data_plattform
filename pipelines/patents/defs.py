"""Patents pipeline definitions - ingestion, extraction, and knowledge graph."""

import os

from dagster import Definitions, with_source_code_references

from _shared import ducklake_io_manager, DuckLakeResource
from resources import FalkorDBResource, Neo4jResource
from assets import (
    # Ingestion
    evaluation_patents,
    raw_patents_data,
    cleaned_patents_data,
    # Extraction
    patent_problems_solutions,
    patent_embeddings,
    # Metadata
    extraction_generation_metadata,
    embeddings_generation_metadata,
    # Knowledge Graph
    patent_knowledge_graph,
    patent_knowledge_graph_dense,
    patent_graph_neo4j_replica,
    patent_similarity_distribution,
)


def _get_ducklake_config() -> dict:
    """Build DuckLake configuration from environment variables.

    Supports two modes:
    - Local mode: DUCKLAKE_CATALOG_DSN is a file path, S3 vars not needed
    - Postgres mode: DUCKLAKE_CATALOG_DSN contains 'host=', S3 vars required
    """
    config = {
        "duckdb_database": os.environ.get("DUCKLAKE_DUCKDB_DATABASE", ":memory:"),
        "ducklake_catalog_dsn": os.environ["DUCKLAKE_CATALOG_DSN"],
        "ducklake_data_path": os.environ["DUCKLAKE_DATA_PATH"],
        "ducklake_schema": os.environ.get("DUCKLAKE_SCHEMA", "my_ducklake"),
    }

    # S3 config - only add if data path is S3
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

    # S3 config - only add if data path is S3
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


def _get_falkordb_resource() -> FalkorDBResource:
    """Build FalkorDBResource from environment variables."""
    return FalkorDBResource(
        host=os.environ.get("FALKORDB_HOST", "localhost"),
        port=int(os.environ.get("FALKORDB_PORT", "6379")),
        password=os.environ.get("FALKORDB_PASSWORD") or None,
        graph_name=os.environ.get("FALKORDB_GRAPH_NAME", "patents"),
    )


def _get_neo4j_resource() -> Neo4jResource:
    """Build Neo4jResource from environment variables."""
    return Neo4jResource(
        uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        user=os.environ.get("NEO4J_USER", "neo4j"),
        password=os.environ.get("NEO4J_PASSWORD", "neo4jpassword"),
        database=os.environ.get("NEO4J_DATABASE", "neo4j"),
    )


defs = Definitions(
    assets=with_source_code_references(
        [
            # Ingestion (group: patent_ingestion)
            evaluation_patents,
            raw_patents_data,
            cleaned_patents_data,
            # Extraction (group: patent_extraction)
            patent_problems_solutions,
            patent_embeddings,
            extraction_generation_metadata,
            embeddings_generation_metadata,
            # Knowledge Graph (group: patent_graph)
            patent_knowledge_graph,
            patent_knowledge_graph_dense,
            patent_graph_neo4j_replica,
            patent_similarity_distribution,
        ]
    ),
    resources={
        "io_manager": ducklake_io_manager.configured(_get_ducklake_config()),
        "ducklake": _get_ducklake_resource(),
        "falkordb": _get_falkordb_resource(),
        "neo4j": _get_neo4j_resource(),
    },
)
