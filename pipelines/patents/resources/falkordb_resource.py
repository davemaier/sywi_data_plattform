"""FalkorDB resource for Dagster."""

from contextlib import contextmanager
from typing import Generator

from dagster import ConfigurableResource
from falkordb import FalkorDB, Graph


class FalkorDBResource(ConfigurableResource):
    """Dagster resource for FalkorDB graph database.

    Provides a context manager for accessing FalkorDB graphs with
    connection pooling and automatic cleanup.

    Example usage in an asset:
        @asset
        def my_asset(falkordb: FalkorDBResource):
            with falkordb.get_graph() as graph:
                result = graph.query("MATCH (n) RETURN n LIMIT 10")
                for row in result.result_set:
                    print(row)
    """

    host: str = "localhost"
    port: int = 6379
    password: str | None = None
    graph_name: str = "patents"

    @contextmanager
    def get_graph(self, graph_name: str | None = None) -> Generator[Graph, None, None]:
        """Get a FalkorDB graph connection.

        Args:
            graph_name: Optional graph name override. If not provided, uses configured default.

        Yields:
            Graph: FalkorDB graph object for executing Cypher queries.
        """
        db = FalkorDB(host=self.host, port=self.port, password=self.password)
        graph = db.select_graph(graph_name or self.graph_name)
        yield graph
