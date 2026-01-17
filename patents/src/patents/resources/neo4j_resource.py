"""Neo4j resource for Dagster."""

from contextlib import contextmanager
from typing import Generator

from dagster import ConfigurableResource
from neo4j import GraphDatabase, Driver, Session


class Neo4jResource(ConfigurableResource):
    """Dagster resource for Neo4j graph database.

    Provides a context manager for accessing Neo4j sessions with
    connection pooling and automatic cleanup.

    Example usage in an asset:
        @asset
        def my_asset(neo4j: Neo4jResource):
            with neo4j.get_session() as session:
                result = session.run("MATCH (n) RETURN n LIMIT 10")
                for record in result:
                    print(record)
    """

    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "neo4jpassword"
    database: str = "neo4j"

    def get_driver(self) -> Driver:
        """Get a Neo4j driver instance.

        Returns:
            Driver: Neo4j driver object for creating sessions.
        """
        return GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    @contextmanager
    def get_session(
        self, database: str | None = None
    ) -> Generator[Session, None, None]:
        """Get a Neo4j session.

        Args:
            database: Optional database name override. If not provided, uses configured default.

        Yields:
            Session: Neo4j session object for executing Cypher queries.
        """
        driver = self.get_driver()
        try:
            session = driver.session(database=database or self.database)
            try:
                yield session
            finally:
                session.close()
        finally:
            driver.close()
