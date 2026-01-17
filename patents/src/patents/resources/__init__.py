"""Resources for the patents pipeline."""

from .falkordb_resource import FalkorDBResource
from .neo4j_resource import Neo4jResource

__all__ = ["FalkorDBResource", "Neo4jResource"]
