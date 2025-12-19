"""Build knowledge graph from patent embeddings using FalkorDB.

This module creates a knowledge graph where:
- Similar problems are clustered (merged if >=90% similarity, linked if >=80%)
- Similar solutions are clustered using the same logic
- Patents are connected to their problem and solution nodes
- Solutions are linked to the problems they solve

The graph uses vector indexes for efficient similarity search on embeddings.
"""

import numpy as np
import matplotlib.pyplot as plt
from dagster import AssetExecutionContext, MetadataValue, asset

from _shared import DuckLakeResource
from resources import FalkorDBResource, Neo4jResource

# Similarity thresholds (cosine similarity)
MERGE_THRESHOLD = 0.90  # >= 90% similarity: merge into existing node
LINK_THRESHOLD = 0.80  # >= 80% similarity: create SIMILAR_TO relationship

# Batch size for resumability
BATCH_SIZE = 10

# Embedding dimension (Qwen3-Embedding-8B)
EMBEDDING_DIM = 4096

# Dense graph configuration
SIMILARITY_THRESHOLD = 0.60  # 60% cosine similarity minimum for edges
GRAPH_NAME = "patent_similarity_network"
VECTOR_SEARCH_K = 200  # Number of candidates to retrieve in vector search

# Sample size for pairwise comparison (full comparison is O(n²))
SAMPLE_SIZE = 500


def normalize_embedding(embedding: list[float]) -> list[float]:
    """Normalize embedding vector for cosine similarity."""
    arr = np.array(embedding)
    norm = np.linalg.norm(arr)
    if norm > 0:
        arr = arr / norm
    return arr.tolist()


def compute_centroid(
    existing_embedding: list[float], new_embedding: list[float], merge_count: int
) -> list[float]:
    """Compute new centroid embedding after merge.

    Uses weighted average where existing embedding has weight = merge_count
    and new embedding has weight = 1. Result is normalized for cosine similarity.

    Args:
        existing_embedding: Current centroid embedding
        new_embedding: New embedding to merge in
        merge_count: Number of embeddings already merged into existing

    Returns:
        Normalized centroid embedding
    """
    existing = np.array(existing_embedding)
    new = np.array(new_embedding)
    centroid = (existing * merge_count + new) / (merge_count + 1)
    return normalize_embedding(centroid.tolist())


def ensure_vector_indexes(graph, context: AssetExecutionContext) -> None:
    """Create vector indexes if they don't exist.

    FalkorDB requires vector indexes for efficient similarity search.
    We create indexes on both Problem and Solution embeddings.
    """
    # Check if indexes exist by trying to query them
    # If they don't exist, create them
    try:
        # Try to create Problem index
        graph.query(
            f"""
            CREATE VECTOR INDEX FOR (p:Problem) ON (p.embedding) 
            OPTIONS {{dimension:{EMBEDDING_DIM}, similarityFunction:'cosine'}}
        """
        )
        context.log.info("Created vector index for Problem nodes")
    except Exception as e:
        if "already exists" in str(e).lower() or "already indexed" in str(e).lower():
            context.log.debug("Problem vector index already exists")
        else:
            context.log.warning(f"Could not create Problem vector index: {e}")

    try:
        # Try to create Solution index
        graph.query(
            f"""
            CREATE VECTOR INDEX FOR (s:Solution) ON (s.embedding) 
            OPTIONS {{dimension:{EMBEDDING_DIM}, similarityFunction:'cosine'}}
        """
        )
        context.log.info("Created vector index for Solution nodes")
    except Exception as e:
        if "already exists" in str(e).lower() or "already indexed" in str(e).lower():
            context.log.debug("Solution vector index already exists")
        else:
            context.log.warning(f"Could not create Solution vector index: {e}")


def find_or_create_problem(
    graph,
    problem_description: str,
    problem_embedding: list[float],
    context: AssetExecutionContext,
) -> tuple[int, list[tuple[int, float]], bool]:
    """Find existing problem node or create new one.

    Searches for similar problems using vector similarity. If a problem
    with >=90% similarity exists, merges into it. Otherwise creates a new node.

    Args:
        graph: FalkorDB graph connection
        problem_description: Text description of the problem
        problem_embedding: 4096-dim embedding vector
        context: Dagster execution context for logging

    Returns:
        Tuple of (problem_node_id, list of (similar_id, score) for linking, was_merged)
    """
    # Search for similar problems using vector index
    result = graph.query(
        """
        CALL db.idx.vector.queryNodes('Problem', 'embedding', 10, vecf32($embedding)) 
        YIELD node, score 
        WHERE score >= $link_threshold
        RETURN id(node) as node_id, node.descriptions as descriptions, 
               node.embedding as embedding, node.merge_count as merge_count, score
        ORDER BY score DESC
    """,
        {"embedding": problem_embedding, "link_threshold": LINK_THRESHOLD},
    )

    similar_to_link: list[tuple[int, float]] = []

    for row in result.result_set:
        node_id, descriptions, existing_embedding, merge_count, score = row

        if score >= MERGE_THRESHOLD:
            # Merge into existing node
            new_embedding = compute_centroid(
                existing_embedding, problem_embedding, merge_count
            )

            graph.query(
                """
                MATCH (p:Problem) WHERE id(p) = $node_id
                SET p.descriptions = p.descriptions + [$description],
                    p.embedding = vecf32($new_embedding),
                    p.merge_count = p.merge_count + 1
            """,
                {
                    "node_id": node_id,
                    "description": problem_description,
                    "new_embedding": new_embedding,
                },
            )

            context.log.debug(
                f"Merged problem into existing node {node_id} (similarity: {score:.3f})"
            )

            # Collect other similar nodes for SIMILAR_TO links (excluding the one we merged into)
            for other_row in result.result_set:
                other_id = other_row[0]
                other_score = other_row[4]
                if (
                    other_id != node_id
                    and LINK_THRESHOLD <= other_score < MERGE_THRESHOLD
                ):
                    similar_to_link.append((other_id, other_score))

            return node_id, similar_to_link, True

        else:
            # This is a similar but not merge-worthy node
            similar_to_link.append((node_id, score))

    # No match found >= MERGE_THRESHOLD, create new node
    create_result = graph.query(
        """
        CREATE (p:Problem {
            descriptions: [$description],
            embedding: vecf32($embedding),
            merge_count: 1
        })
        RETURN id(p) as node_id
    """,
        {"description": problem_description, "embedding": problem_embedding},
    )

    new_problem_id = create_result.result_set[0][0]
    context.log.debug(f"Created new problem node {new_problem_id}")

    return new_problem_id, similar_to_link, False


def find_or_create_solution(
    graph,
    solution_description: str,
    solution_embedding: list[float],
    context: AssetExecutionContext,
) -> tuple[int, list[tuple[int, float]], bool]:
    """Find existing solution node or create new one.

    Same logic as find_or_create_problem but for Solution nodes.

    Args:
        graph: FalkorDB graph connection
        solution_description: Text description of the solution
        solution_embedding: 4096-dim embedding vector
        context: Dagster execution context for logging

    Returns:
        Tuple of (solution_node_id, list of (similar_id, score) for linking, was_merged)
    """
    # Search for similar solutions using vector index
    result = graph.query(
        """
        CALL db.idx.vector.queryNodes('Solution', 'embedding', 10, vecf32($embedding)) 
        YIELD node, score 
        WHERE score >= $link_threshold
        RETURN id(node) as node_id, node.descriptions as descriptions, 
               node.embedding as embedding, node.merge_count as merge_count, score
        ORDER BY score DESC
    """,
        {"embedding": solution_embedding, "link_threshold": LINK_THRESHOLD},
    )

    similar_to_link: list[tuple[int, float]] = []

    for row in result.result_set:
        node_id, descriptions, existing_embedding, merge_count, score = row

        if score >= MERGE_THRESHOLD:
            # Merge into existing node
            new_embedding = compute_centroid(
                existing_embedding, solution_embedding, merge_count
            )

            graph.query(
                """
                MATCH (s:Solution) WHERE id(s) = $node_id
                SET s.descriptions = s.descriptions + [$description],
                    s.embedding = vecf32($new_embedding),
                    s.merge_count = s.merge_count + 1
            """,
                {
                    "node_id": node_id,
                    "description": solution_description,
                    "new_embedding": new_embedding,
                },
            )

            context.log.debug(
                f"Merged solution into existing node {node_id} (similarity: {score:.3f})"
            )

            # Collect other similar nodes for SIMILAR_TO links
            for other_row in result.result_set:
                other_id = other_row[0]
                other_score = other_row[4]
                if (
                    other_id != node_id
                    and LINK_THRESHOLD <= other_score < MERGE_THRESHOLD
                ):
                    similar_to_link.append((other_id, other_score))

            return node_id, similar_to_link, True

        else:
            similar_to_link.append((node_id, score))

    # Create new solution node
    create_result = graph.query(
        """
        CREATE (s:Solution {
            descriptions: [$description],
            embedding: vecf32($embedding),
            merge_count: 1
        })
        RETURN id(s) as node_id
    """,
        {"description": solution_description, "embedding": solution_embedding},
    )

    new_solution_id = create_result.result_set[0][0]
    context.log.debug(f"Created new solution node {new_solution_id}")

    return new_solution_id, similar_to_link, False


def create_similar_to_links(
    graph, node_type: str, node_id: int, similar_nodes: list[tuple[int, float]]
) -> None:
    """Create SIMILAR_TO relationships between nodes.

    Creates unidirectional similarity links from the given node to
    all nodes in the similar_nodes list.

    Args:
        graph: FalkorDB graph connection
        node_type: 'Problem' or 'Solution'
        node_id: ID of the source node
        similar_nodes: List of (target_node_id, similarity_score) tuples
    """
    for similar_id, similarity in similar_nodes:
        if similar_id != node_id:  # Don't link to self
            graph.query(
                f"""
                MATCH (a:{node_type}), (b:{node_type}) 
                WHERE id(a) = $node_id AND id(b) = $similar_id
                MERGE (a)-[r:SIMILAR_TO]->(b)
                SET r.similarity = $similarity
            """,
                {
                    "node_id": node_id,
                    "similar_id": similar_id,
                    "similarity": similarity,
                },
            )


def create_similarity_edges(
    graph,
    node_type: str,
    node_id: int,
    embedding: list[float],
    edge_type: str,
    threshold: float,
    context: AssetExecutionContext,
) -> int:
    """Find similar nodes and create weighted edges.

    Uses consistent edge direction (lower_id -> higher_id) to avoid duplicates.

    Args:
        graph: FalkorDB graph
        node_type: 'Problem' or 'Solution'
        node_id: ID of the newly created node
        embedding: Normalized embedding vector
        edge_type: 'SIMILAR_PROBLEM' or 'SIMILAR_SOLUTION'
        threshold: Minimum similarity (0.0-1.0)
        context: Dagster context for logging

    Returns:
        Number of edges created
    """
    # Query for similar nodes above threshold
    # Use VECTOR_SEARCH_K to get enough candidates so we don't miss matches above threshold
    result = graph.query(
        f"""
        CALL db.idx.vector.queryNodes('{node_type}', 'embedding', {VECTOR_SEARCH_K}, vecf32($embedding))
        YIELD node, score
        WHERE score >= $threshold AND id(node) <> $node_id
        RETURN id(node) as similar_id, score
        ORDER BY score DESC
        """,
        {"embedding": embedding, "threshold": threshold, "node_id": node_id},
    )

    edges_created = 0
    for row in result.result_set:
        similar_id, similarity = row

        # Consistent direction: lower ID -> higher ID (avoids duplicate edges)
        if node_id < similar_id:
            from_id, to_id = node_id, similar_id
        else:
            from_id, to_id = similar_id, node_id

        # Create edge with MERGE to avoid duplicates
        graph.query(
            f"""
            MATCH (a:{node_type}), (b:{node_type})
            WHERE id(a) = $from_id AND id(b) = $to_id
            MERGE (a)-[r:{edge_type}]->(b)
            SET r.similarity = $similarity
            """,
            {"from_id": from_id, "to_id": to_id, "similarity": float(similarity)},
        )
        edges_created += 1

    return edges_created


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


@asset(
    group_name="patent_graph",
    description=(
        "Builds a knowledge graph from patent embeddings using FalkorDB. "
        "Clusters similar problems and solutions using cosine similarity. "
        "Nodes with >=90% similarity are merged (centroid embedding); "
        ">=80% similarity creates SIMILAR_TO links. "
        "Resumable - skips patents already in graph."
    ),
    kinds={"python", "falkordb"},
    metadata={
        "merge_threshold": MERGE_THRESHOLD,
        "link_threshold": LINK_THRESHOLD,
        "batch_size": BATCH_SIZE,
        "embedding_dimension": EMBEDDING_DIM,
        "resumable": True,
    },
    deps=["patent_embeddings"],
    code_version="1.0.0",
)
def patent_knowledge_graph(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
    falkordb: FalkorDBResource,
) -> None:
    """Build knowledge graph from patent problem/solution embeddings.

    This asset:
    1. Loads patents with embeddings from DuckLake
    2. For each patent, finds or creates Problem and Solution nodes
    3. Merges nodes with >=90% similarity (updates centroid embedding)
    4. Creates SIMILAR_TO links for nodes with 80-90% similarity
    5. Creates Patent nodes linked to their Problem and Solution

    The result is a graph that clusters semantically similar problems
    and solutions, enabling discovery of related patents and cross-domain
    solution transfer.
    """
    TABLE_NAME = "patent_problems_solutions"

    with falkordb.get_graph() as graph:
        # Ensure vector indexes exist
        context.log.info("Ensuring vector indexes exist...")
        ensure_vector_indexes(graph, context)

        # Get patents already in graph (for resumability)
        existing_result = graph.query(
            """
            MATCH (patent:Patent)
            RETURN patent.publication_number
        """
        )
        existing_patents = {row[0] for row in existing_result.result_set}
        context.log.info(f"Found {len(existing_patents)} patents already in graph")

        # Query patents with embeddings from DuckLake
        with ducklake.get_connection() as conn:
            patents_df = conn.execute(
                f"""
                SELECT 
                    publication_number,
                    abstract_text,
                    problem,
                    solution,
                    problem_embedding,
                    solution_embedding
                FROM {ducklake.ducklake_schema}.{TABLE_NAME}
                WHERE problem_embedding IS NOT NULL 
                  AND solution_embedding IS NOT NULL
            """
            ).fetchdf()

        # Filter out already processed patents
        patents_to_process = patents_df[
            ~patents_df["publication_number"].isin(list(existing_patents))
        ]

        total_patents = len(patents_to_process)
        context.log.info(
            f"Processing {total_patents} new patents "
            f"({len(existing_patents)} already in graph)"
        )

        if total_patents == 0:
            context.log.info("No new patents to process!")
            context.add_output_metadata(
                {
                    "status": "no_new_patents",
                    "existing_patents": len(existing_patents),
                    "processed_count": 0,
                }
            )
            return

        # Processing counters
        processed_count = 0
        failed_count = 0
        problems_created = 0
        problems_merged = 0
        solutions_created = 0
        solutions_merged = 0

        for idx, row in patents_to_process.iterrows():
            try:
                pub_number = str(row["publication_number"])
                abstract_val = row["abstract_text"]
                abstract = str(abstract_val) if abstract_val is not None else ""
                problem_desc = str(row["problem"])
                solution_desc = str(row["solution"])
                problem_emb = list(row["problem_embedding"])
                solution_emb = list(row["solution_embedding"])

                # 1. Find or create Problem node
                problem_id, problem_similar, problem_was_merged = (
                    find_or_create_problem(graph, problem_desc, problem_emb, context)
                )
                if problem_was_merged:
                    problems_merged += 1
                else:
                    problems_created += 1

                # 2. Find or create Solution node
                solution_id, solution_similar, solution_was_merged = (
                    find_or_create_solution(graph, solution_desc, solution_emb, context)
                )
                if solution_was_merged:
                    solutions_merged += 1
                else:
                    solutions_created += 1

                # 3. Create SIMILAR_TO links
                create_similar_to_links(graph, "Problem", problem_id, problem_similar)
                create_similar_to_links(
                    graph, "Solution", solution_id, solution_similar
                )

                # 4. Create Patent node and relationships
                graph.query(
                    """
                    CREATE (patent:Patent {
                        publication_number: $pub_number,
                        abstract: $abstract
                    })
                    WITH patent
                    MATCH (problem:Problem), (solution:Solution)
                    WHERE id(problem) = $problem_id AND id(solution) = $solution_id
                    CREATE (patent)-[:HAS_PROBLEM]->(problem)
                    CREATE (patent)-[:HAS_SOLUTION]->(solution)
                    CREATE (solution)-[:SOLVES]->(problem)
                """,
                    {
                        "pub_number": pub_number,
                        "abstract": abstract,
                        "problem_id": problem_id,
                        "solution_id": solution_id,
                    },
                )

                processed_count += 1

                # Progress logging every batch
                if processed_count % BATCH_SIZE == 0:
                    progress_pct = round(processed_count / total_patents * 100, 1)
                    context.log.info(
                        f"Progress: {processed_count}/{total_patents} ({progress_pct}%) | "
                        f"Problems: {problems_created} new, {problems_merged} merged | "
                        f"Solutions: {solutions_created} new, {solutions_merged} merged"
                    )

            except Exception as e:
                context.log.warning(
                    f"Failed to process patent {row.get('publication_number', 'unknown')}: {e}"
                )
                failed_count += 1
                continue

        # Get final graph stats
        try:
            stats_result = graph.query(
                """
                MATCH (p:Problem) WITH count(p) as problems
                MATCH (s:Solution) WITH problems, count(s) as solutions
                MATCH (pat:Patent) WITH problems, solutions, count(pat) as patents
                MATCH ()-[r:SIMILAR_TO]->() 
                RETURN problems, solutions, patents, count(r) as similar_links
            """
            )
            final_stats = (
                stats_result.result_set[0] if stats_result.result_set else [0, 0, 0, 0]
            )
        except Exception:
            final_stats = [0, 0, 0, 0]

        context.log.info(
            f"Completed! Processed {processed_count} patents, {failed_count} failed"
        )

        context.add_output_metadata(
            {
                "processed_count": processed_count,
                "failed_count": failed_count,
                "problems_created": problems_created,
                "problems_merged": problems_merged,
                "solutions_created": solutions_created,
                "solutions_merged": solutions_merged,
                "total_problem_nodes": int(final_stats[0]),
                "total_solution_nodes": int(final_stats[1]),
                "total_patent_nodes": int(final_stats[2]),
                "total_similar_to_links": int(final_stats[3]),
                "merge_threshold": MERGE_THRESHOLD,
                "link_threshold": LINK_THRESHOLD,
            }
        )


@asset(
    group_name="patent_graph",
    description=(
        "Dense patent similarity network with individual nodes for every problem/solution. "
        "Creates Patent, Problem, and Solution nodes linked together. "
        "Problems and Solutions are connected to similar nodes (>=60% cosine similarity) "
        "with weighted SIMILAR_PROBLEM and SIMILAR_SOLUTION edges. "
        "Graph name: patent_similarity_network. Resumable."
    ),
    kinds={"python", "falkordb"},
    metadata={
        "similarity_threshold": SIMILARITY_THRESHOLD,
        "batch_size": BATCH_SIZE,
        "embedding_dimension": EMBEDDING_DIM,
        "graph_name": GRAPH_NAME,
        "vector_search_k": VECTOR_SEARCH_K,
        "resumable": True,
    },
    deps=["patent_embeddings"],
    code_version="1.1.0",
)
def patent_knowledge_graph_dense(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
    falkordb: FalkorDBResource,
) -> None:
    """Build dense similarity network from patent embeddings."""
    TABLE_NAME = "patent_problems_solutions"

    with falkordb.get_graph(graph_name=GRAPH_NAME) as graph:
        # Ensure vector indexes exist
        context.log.info(f"Building graph: {GRAPH_NAME}")
        ensure_vector_indexes(graph, context)

        # Get patents already in graph (for resumability)
        existing_result = graph.query(
            """
            MATCH (patent:Patent)
            RETURN patent.publication_number
        """
        )
        existing_patents = {row[0] for row in existing_result.result_set}
        context.log.info(f"Found {len(existing_patents)} patents already in graph")

        # Query patents with embeddings from DuckLake
        with ducklake.get_connection() as conn:
            patents_df = conn.execute(
                f"""
                SELECT 
                    publication_number,
                    title_text,
                    abstract_text,
                    problem,
                    solution,
                    problem_embedding,
                    solution_embedding
                FROM {ducklake.ducklake_schema}.{TABLE_NAME}
                WHERE problem_embedding IS NOT NULL 
                  AND solution_embedding IS NOT NULL
            """
            ).fetchdf()

        # Filter out already processed patents
        patents_to_process = patents_df[
            ~patents_df["publication_number"].isin(list(existing_patents))
        ]

        total_patents = len(patents_to_process)
        context.log.info(
            f"Processing {total_patents} new patents "
            f"({len(existing_patents)} already in graph)"
        )

        if total_patents == 0:
            context.log.info("No new patents to process!")
            context.add_output_metadata(
                {
                    "status": "no_new_patents",
                    "existing_patents": len(existing_patents),
                    "processed_count": 0,
                }
            )
            return

        # Processing counters
        processed_count = 0
        failed_count = 0
        problem_edges_created = 0
        solution_edges_created = 0

        for idx, row in patents_to_process.iterrows():
            try:
                pub_number = str(row["publication_number"])
                title_val = row["title_text"]
                title = str(title_val) if title_val is not None else ""
                abstract_val = row["abstract_text"]
                abstract = str(abstract_val) if abstract_val is not None else ""
                problem_desc = str(row["problem"])
                solution_desc = str(row["solution"])
                problem_emb = normalize_embedding(list(row["problem_embedding"]))
                solution_emb = normalize_embedding(list(row["solution_embedding"]))

                # 1. Create Problem node
                problem_result = graph.query(
                    """
                    CREATE (p:Problem {
                        publication_number: $pub_number,
                        description: $description,
                        embedding: vecf32($embedding)
                    })
                    RETURN id(p) as node_id
                    """,
                    {
                        "pub_number": pub_number,
                        "description": problem_desc,
                        "embedding": problem_emb,
                    },
                )
                problem_id = problem_result.result_set[0][0]

                # 2. Create Solution node
                solution_result = graph.query(
                    """
                    CREATE (s:Solution {
                        publication_number: $pub_number,
                        description: $description,
                        embedding: vecf32($embedding)
                    })
                    RETURN id(s) as node_id
                    """,
                    {
                        "pub_number": pub_number,
                        "description": solution_desc,
                        "embedding": solution_emb,
                    },
                )
                solution_id = solution_result.result_set[0][0]

                # 3. Create Patent node and connect to Problem/Solution
                graph.query(
                    """
                    CREATE (patent:Patent {
                        publication_number: $pub_number,
                        title: $title,
                        abstract: $abstract
                    })
                    WITH patent
                    MATCH (problem:Problem), (solution:Solution)
                    WHERE id(problem) = $problem_id AND id(solution) = $solution_id
                    CREATE (patent)-[:HAS_PROBLEM]->(problem)
                    CREATE (patent)-[:HAS_SOLUTION]->(solution)
                    CREATE (solution)-[:SOLVES]->(problem)
                    """,
                    {
                        "pub_number": pub_number,
                        "title": title,
                        "abstract": abstract,
                        "problem_id": problem_id,
                        "solution_id": solution_id,
                    },
                )

                # 4. Create SIMILAR_PROBLEM edges
                problem_edges = create_similarity_edges(
                    graph,
                    "Problem",
                    problem_id,
                    problem_emb,
                    "SIMILAR_PROBLEM",
                    SIMILARITY_THRESHOLD,
                    context,
                )
                problem_edges_created += problem_edges

                # 5. Create SIMILAR_SOLUTION edges
                solution_edges = create_similarity_edges(
                    graph,
                    "Solution",
                    solution_id,
                    solution_emb,
                    "SIMILAR_SOLUTION",
                    SIMILARITY_THRESHOLD,
                    context,
                )
                solution_edges_created += solution_edges

                processed_count += 1

                # Progress logging every batch
                if processed_count % BATCH_SIZE == 0:
                    progress_pct = round(processed_count / total_patents * 100, 1)
                    context.log.info(
                        f"Progress: {processed_count}/{total_patents} ({progress_pct}%) | "
                        f"Problem edges: {problem_edges_created} | "
                        f"Solution edges: {solution_edges_created}"
                    )

            except Exception as e:
                context.log.warning(
                    f"Failed to process patent {row.get('publication_number', 'unknown')}: {e}"
                )
                failed_count += 1
                continue

        # Get final graph stats
        try:
            stats_result = graph.query(
                """
                MATCH (p:Problem) WITH count(p) as problems
                MATCH (s:Solution) WITH problems, count(s) as solutions
                MATCH (pat:Patent) WITH problems, solutions, count(pat) as patents
                MATCH ()-[r:SIMILAR_PROBLEM]->() WITH problems, solutions, patents, count(r) as prob_edges
                MATCH ()-[r:SIMILAR_SOLUTION]->() 
                RETURN problems, solutions, patents, prob_edges, count(r) as sol_edges
            """
            )
            final_stats = (
                stats_result.result_set[0]
                if stats_result.result_set
                else [0, 0, 0, 0, 0]
            )
        except Exception:
            final_stats = [0, 0, 0, 0, 0]

        context.log.info(
            f"Completed! Processed {processed_count} patents, {failed_count} failed"
        )

        context.add_output_metadata(
            {
                "graph_name": GRAPH_NAME,
                "processed_count": processed_count,
                "failed_count": failed_count,
                "problem_similarity_edges_created": problem_edges_created,
                "solution_similarity_edges_created": solution_edges_created,
                "total_patent_nodes": int(final_stats[2]),
                "total_problem_nodes": int(final_stats[0]),
                "total_solution_nodes": int(final_stats[1]),
                "total_similar_problem_edges": int(final_stats[3]),
                "total_similar_solution_edges": int(final_stats[4]),
                "similarity_threshold": SIMILARITY_THRESHOLD,
            }
        )


# Graph names for replication
FALKORDB_GRAPH = "patent_similarity_network"
NEO4J_DATABASE = "neo4j"
REPLICATION_BATCH_SIZE = 100


@asset(
    group_name="patent_graph",
    description=(
        "Replicates the patent_similarity_network graph from FalkorDB to Neo4j "
        "for visualization using Neo4j Browser. Nodes are copied without embeddings "
        "to save space. All relationships and their properties are preserved."
    ),
    kinds={"python", "neo4j"},
    metadata={
        "source_graph": FALKORDB_GRAPH,
        "target_database": NEO4J_DATABASE,
        "batch_size": REPLICATION_BATCH_SIZE,
    },
    deps=["patent_knowledge_graph_dense"],
    code_version="1.0.0",
)
def patent_graph_neo4j_replica(
    context: AssetExecutionContext,
    falkordb: FalkorDBResource,
    neo4j: Neo4jResource,
) -> None:
    """Replicate FalkorDB graph to Neo4j for visualization."""

    # Get graph stats from FalkorDB first
    with falkordb.get_graph(graph_name=FALKORDB_GRAPH) as graph:
        try:
            stats_result = graph.query(
                """
                MATCH (p:Problem) WITH count(p) as problems
                MATCH (s:Solution) WITH problems, count(s) as solutions
                MATCH (pat:Patent) 
                RETURN problems, solutions, count(pat) as patents
            """
            )
            if stats_result.result_set:
                problems_count, solutions_count, patents_count = (
                    stats_result.result_set[0]
                )
            else:
                context.log.warning("No data found in FalkorDB graph!")
                context.add_output_metadata({"status": "no_data_in_source"})
                return
        except Exception as e:
            context.log.error(f"Failed to query FalkorDB: {e}")
            raise

    context.log.info(
        f"Found {patents_count} patents, {problems_count} problems, "
        f"{solutions_count} solutions in FalkorDB"
    )

    with neo4j.get_session(database=NEO4J_DATABASE) as session:
        # Step 1: Clear existing data
        context.log.info("Clearing existing Neo4j data...")
        session.run("MATCH (n) DETACH DELETE n")
        context.log.info("Neo4j database cleared")

        # Step 2: Create indexes for performance
        context.log.info("Creating indexes...")
        session.run(
            "CREATE INDEX patent_pub_number IF NOT EXISTS FOR (p:Patent) ON (p.publication_number)"
        )
        session.run(
            "CREATE INDEX problem_pub_number IF NOT EXISTS FOR (p:Problem) ON (p.publication_number)"
        )
        session.run(
            "CREATE INDEX solution_pub_number IF NOT EXISTS FOR (s:Solution) ON (s.publication_number)"
        )
        context.log.info("Indexes created")

    # Step 3: Replicate Patents
    context.log.info("Replicating Patent nodes...")
    with falkordb.get_graph(graph_name=FALKORDB_GRAPH) as graph:
        result = graph.query(
            """
            MATCH (patent:Patent)
            RETURN patent.publication_number as pub_number,
                   patent.title as title,
                   patent.abstract as abstract
        """
        )

        patents_replicated = 0
        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                pub_number, title, abstract = row
                session.run(
                    """
                    CREATE (p:Patent {
                        publication_number: $pub_number,
                        title: $title,
                        abstract: $abstract
                    })
                    """,
                    pub_number=pub_number,
                    title=title or "",
                    abstract=abstract or "",
                )
                patents_replicated += 1

                if patents_replicated % REPLICATION_BATCH_SIZE == 0:
                    context.log.info(f"Replicated {patents_replicated} patents...")

    context.log.info(f"Replicated {patents_replicated} Patent nodes")

    # Step 4: Replicate Problems (without embeddings)
    context.log.info("Replicating Problem nodes...")
    with falkordb.get_graph(graph_name=FALKORDB_GRAPH) as graph:
        result = graph.query(
            """
            MATCH (problem:Problem)
            RETURN problem.publication_number as pub_number,
                   problem.description as description,
                   id(problem) as falkor_id
        """
        )

        problems_replicated = 0
        # Map FalkorDB node IDs to Neo4j node IDs for relationship creation
        falkor_to_neo4j_problem = {}

        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                pub_number, description, falkor_id = row
                result_neo4j = session.run(
                    """
                    CREATE (p:Problem {
                        publication_number: $pub_number,
                        description: $description,
                        falkor_id: $falkor_id
                    })
                    RETURN id(p) as neo4j_id
                    """,
                    pub_number=pub_number,
                    description=description or "",
                    falkor_id=falkor_id,
                )
                neo4j_id = result_neo4j.single()["neo4j_id"]
                falkor_to_neo4j_problem[falkor_id] = neo4j_id
                problems_replicated += 1

                if problems_replicated % REPLICATION_BATCH_SIZE == 0:
                    context.log.info(f"Replicated {problems_replicated} problems...")

    context.log.info(f"Replicated {problems_replicated} Problem nodes")

    # Step 5: Replicate Solutions (without embeddings)
    context.log.info("Replicating Solution nodes...")
    with falkordb.get_graph(graph_name=FALKORDB_GRAPH) as graph:
        result = graph.query(
            """
            MATCH (solution:Solution)
            RETURN solution.publication_number as pub_number,
                   solution.description as description,
                   id(solution) as falkor_id
        """
        )

        solutions_replicated = 0
        falkor_to_neo4j_solution = {}

        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                pub_number, description, falkor_id = row
                result_neo4j = session.run(
                    """
                    CREATE (s:Solution {
                        publication_number: $pub_number,
                        description: $description,
                        falkor_id: $falkor_id
                    })
                    RETURN id(s) as neo4j_id
                    """,
                    pub_number=pub_number,
                    description=description or "",
                    falkor_id=falkor_id,
                )
                neo4j_id = result_neo4j.single()["neo4j_id"]
                falkor_to_neo4j_solution[falkor_id] = neo4j_id
                solutions_replicated += 1

                if solutions_replicated % REPLICATION_BATCH_SIZE == 0:
                    context.log.info(f"Replicated {solutions_replicated} solutions...")

    context.log.info(f"Replicated {solutions_replicated} Solution nodes")

    # Step 6: Replicate relationships
    context.log.info("Replicating relationships...")
    relationships_replicated = 0

    with falkordb.get_graph(graph_name=FALKORDB_GRAPH) as graph:
        # HAS_PROBLEM relationships
        context.log.info("Replicating HAS_PROBLEM relationships...")
        result = graph.query(
            """
            MATCH (patent:Patent)-[:HAS_PROBLEM]->(problem:Problem)
            RETURN patent.publication_number as pub_number,
                   id(problem) as problem_falkor_id
        """
        )

        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                pub_number, problem_falkor_id = row
                session.run(
                    """
                    MATCH (patent:Patent {publication_number: $pub_number})
                    MATCH (problem:Problem {falkor_id: $problem_falkor_id})
                    CREATE (patent)-[:HAS_PROBLEM]->(problem)
                    """,
                    pub_number=pub_number,
                    problem_falkor_id=problem_falkor_id,
                )
                relationships_replicated += 1

        # HAS_SOLUTION relationships
        context.log.info("Replicating HAS_SOLUTION relationships...")
        result = graph.query(
            """
            MATCH (patent:Patent)-[:HAS_SOLUTION]->(solution:Solution)
            RETURN patent.publication_number as pub_number,
                   id(solution) as solution_falkor_id
        """
        )

        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                pub_number, solution_falkor_id = row
                session.run(
                    """
                    MATCH (patent:Patent {publication_number: $pub_number})
                    MATCH (solution:Solution {falkor_id: $solution_falkor_id})
                    CREATE (patent)-[:HAS_SOLUTION]->(solution)
                    """,
                    pub_number=pub_number,
                    solution_falkor_id=solution_falkor_id,
                )
                relationships_replicated += 1

        # SOLVES relationships
        context.log.info("Replicating SOLVES relationships...")
        result = graph.query(
            """
            MATCH (solution:Solution)-[:SOLVES]->(problem:Problem)
            RETURN id(solution) as solution_falkor_id,
                   id(problem) as problem_falkor_id
        """
        )

        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                solution_falkor_id, problem_falkor_id = row
                session.run(
                    """
                    MATCH (solution:Solution {falkor_id: $solution_falkor_id})
                    MATCH (problem:Problem {falkor_id: $problem_falkor_id})
                    CREATE (solution)-[:SOLVES]->(problem)
                    """,
                    solution_falkor_id=solution_falkor_id,
                    problem_falkor_id=problem_falkor_id,
                )
                relationships_replicated += 1

        # SIMILAR_PROBLEM relationships
        context.log.info("Replicating SIMILAR_PROBLEM relationships...")
        result = graph.query(
            """
            MATCH (p1:Problem)-[r:SIMILAR_PROBLEM]->(p2:Problem)
            RETURN id(p1) as from_id, id(p2) as to_id, r.similarity as similarity
        """
        )

        similar_problem_count = 0
        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                from_id, to_id, similarity = row
                session.run(
                    """
                    MATCH (p1:Problem {falkor_id: $from_id})
                    MATCH (p2:Problem {falkor_id: $to_id})
                    CREATE (p1)-[:SIMILAR_PROBLEM {similarity: $similarity}]->(p2)
                    """,
                    from_id=from_id,
                    to_id=to_id,
                    similarity=float(similarity),
                )
                relationships_replicated += 1
                similar_problem_count += 1

                if similar_problem_count % REPLICATION_BATCH_SIZE == 0:
                    context.log.info(
                        f"Replicated {similar_problem_count} SIMILAR_PROBLEM edges..."
                    )

        context.log.info(f"Replicated {similar_problem_count} SIMILAR_PROBLEM edges")

        # SIMILAR_SOLUTION relationships
        context.log.info("Replicating SIMILAR_SOLUTION relationships...")
        result = graph.query(
            """
            MATCH (s1:Solution)-[r:SIMILAR_SOLUTION]->(s2:Solution)
            RETURN id(s1) as from_id, id(s2) as to_id, r.similarity as similarity
        """
        )

        similar_solution_count = 0
        with neo4j.get_session(database=NEO4J_DATABASE) as session:
            for row in result.result_set:
                from_id, to_id, similarity = row
                session.run(
                    """
                    MATCH (s1:Solution {falkor_id: $from_id})
                    MATCH (s2:Solution {falkor_id: $to_id})
                    CREATE (s1)-[:SIMILAR_SOLUTION {similarity: $similarity}]->(s2)
                    """,
                    from_id=from_id,
                    to_id=to_id,
                    similarity=float(similarity),
                )
                relationships_replicated += 1
                similar_solution_count += 1

                if similar_solution_count % REPLICATION_BATCH_SIZE == 0:
                    context.log.info(
                        f"Replicated {similar_solution_count} SIMILAR_SOLUTION edges..."
                    )

        context.log.info(f"Replicated {similar_solution_count} SIMILAR_SOLUTION edges")

    context.log.info(f"Total relationships replicated: {relationships_replicated}")

    # Get final Neo4j stats
    with neo4j.get_session(database=NEO4J_DATABASE) as session:
        result = session.run(
            """
            MATCH (n) 
            WITH count(n) as node_count
            MATCH ()-[r]->() 
            RETURN node_count, count(r) as rel_count
        """
        )
        record = result.single()
        neo4j_nodes = record["node_count"] if record else 0
        neo4j_rels = record["rel_count"] if record else 0

    context.log.info(
        f"Replication complete! Neo4j now has {neo4j_nodes} nodes and {neo4j_rels} relationships"
    )

    context.add_output_metadata(
        {
            "source_graph": FALKORDB_GRAPH,
            "target_database": NEO4J_DATABASE,
            "patents_replicated": patents_replicated,
            "problems_replicated": problems_replicated,
            "solutions_replicated": solutions_replicated,
            "relationships_replicated": relationships_replicated,
            "neo4j_total_nodes": neo4j_nodes,
            "neo4j_total_relationships": neo4j_rels,
            "embeddings_excluded": True,
        }
    )


@asset(
    group_name="patent_graph",
    description=(
        "Analyzes pairwise similarity distributions for patent problems and solutions. "
        "Creates histograms showing how similarities are distributed, which helps "
        "understand why edge creation may stall at certain thresholds."
    ),
    kinds={"python", "matplotlib"},
    deps=["patent_embeddings"],
    code_version="1.0.0",
)
def patent_similarity_distribution(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> None:
    """Compute and visualize similarity score distributions."""
    TABLE_NAME = "patent_problems_solutions"

    # Load embeddings from DuckLake
    with ducklake.get_connection() as conn:
        df = conn.execute(
            f"""
            SELECT 
                publication_number,
                problem_embedding,
                solution_embedding
            FROM {ducklake.ducklake_schema}.{TABLE_NAME}
            WHERE problem_embedding IS NOT NULL 
              AND solution_embedding IS NOT NULL
        """
        ).fetchdf()

    total_patents = len(df)
    context.log.info(f"Loaded {total_patents} patents with embeddings")

    # Sample if too many patents (pairwise comparison is O(n²))
    if total_patents > SAMPLE_SIZE:
        df = df.sample(n=SAMPLE_SIZE, random_state=42)
        context.log.info(f"Sampled {SAMPLE_SIZE} patents for analysis")

    n = len(df)

    # Extract embeddings as numpy arrays
    problem_embeddings = np.array([np.array(e) for e in df["problem_embedding"].values])
    solution_embeddings = np.array(
        [np.array(e) for e in df["solution_embedding"].values]
    )

    # Normalize for cosine similarity
    problem_norms = np.linalg.norm(problem_embeddings, axis=1, keepdims=True)
    problem_norms[problem_norms == 0] = 1  # Avoid division by zero
    problem_embeddings_norm = problem_embeddings / problem_norms

    solution_norms = np.linalg.norm(solution_embeddings, axis=1, keepdims=True)
    solution_norms[solution_norms == 0] = 1
    solution_embeddings_norm = solution_embeddings / solution_norms

    # Compute pairwise cosine similarities (using matrix multiplication for efficiency)
    context.log.info("Computing pairwise problem similarities...")
    problem_sim_matrix = problem_embeddings_norm @ problem_embeddings_norm.T

    context.log.info("Computing pairwise solution similarities...")
    solution_sim_matrix = solution_embeddings_norm @ solution_embeddings_norm.T

    # Extract upper triangle (excluding diagonal) for unique pairs
    triu_indices = np.triu_indices(n, k=1)
    problem_similarities = problem_sim_matrix[triu_indices]
    solution_similarities = solution_sim_matrix[triu_indices]

    num_pairs = len(problem_similarities)
    context.log.info(f"Analyzed {num_pairs} unique pairs")

    # Compute statistics
    problem_stats = {
        "mean": float(np.mean(problem_similarities)),
        "std": float(np.std(problem_similarities)),
        "min": float(np.min(problem_similarities)),
        "max": float(np.max(problem_similarities)),
        "median": float(np.median(problem_similarities)),
        "pct_above_50": float(np.mean(problem_similarities >= 0.50) * 100),
        "pct_above_60": float(np.mean(problem_similarities >= 0.60) * 100),
        "pct_above_70": float(np.mean(problem_similarities >= 0.70) * 100),
        "pct_above_80": float(np.mean(problem_similarities >= 0.80) * 100),
    }

    solution_stats = {
        "mean": float(np.mean(solution_similarities)),
        "std": float(np.std(solution_similarities)),
        "min": float(np.min(solution_similarities)),
        "max": float(np.max(solution_similarities)),
        "median": float(np.median(solution_similarities)),
        "pct_above_50": float(np.mean(solution_similarities >= 0.50) * 100),
        "pct_above_60": float(np.mean(solution_similarities >= 0.60) * 100),
        "pct_above_70": float(np.mean(solution_similarities >= 0.70) * 100),
        "pct_above_80": float(np.mean(solution_similarities >= 0.80) * 100),
    }

    context.log.info(
        f"Problem similarities: mean={problem_stats['mean']:.3f}, "
        f"std={problem_stats['std']:.3f}, >=60%: {problem_stats['pct_above_60']:.1f}%"
    )
    context.log.info(
        f"Solution similarities: mean={solution_stats['mean']:.3f}, "
        f"std={solution_stats['std']:.3f}, >=60%: {solution_stats['pct_above_60']:.1f}%"
    )

    # Create visualization
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Problem similarity histogram
    ax1 = axes[0, 0]
    ax1.hist(
        problem_similarities, bins=50, edgecolor="black", alpha=0.7, color="steelblue"
    )
    ax1.axvline(x=0.60, color="red", linestyle="--", linewidth=2, label="60% threshold")
    ax1.axvline(
        x=problem_stats["mean"],
        color="orange",
        linestyle="-",
        linewidth=2,
        label=f"Mean: {problem_stats['mean']:.3f}",
    )
    ax1.set_xlabel("Cosine Similarity")
    ax1.set_ylabel("Number of Pairs")
    ax1.set_title("Problem Similarity Distribution")
    ax1.legend()
    ax1.set_xlim(0, 1)

    # Solution similarity histogram
    ax2 = axes[0, 1]
    ax2.hist(
        solution_similarities, bins=50, edgecolor="black", alpha=0.7, color="seagreen"
    )
    ax2.axvline(x=0.60, color="red", linestyle="--", linewidth=2, label="60% threshold")
    ax2.axvline(
        x=solution_stats["mean"],
        color="orange",
        linestyle="-",
        linewidth=2,
        label=f"Mean: {solution_stats['mean']:.3f}",
    )
    ax2.set_xlabel("Cosine Similarity")
    ax2.set_ylabel("Number of Pairs")
    ax2.set_title("Solution Similarity Distribution")
    ax2.legend()
    ax2.set_xlim(0, 1)

    # Cumulative distribution (percentage above threshold)
    ax3 = axes[1, 0]
    thresholds = np.linspace(0, 1, 100)
    problem_cumulative = [np.mean(problem_similarities >= t) * 100 for t in thresholds]
    solution_cumulative = [
        np.mean(solution_similarities >= t) * 100 for t in thresholds
    ]
    ax3.plot(
        thresholds, problem_cumulative, label="Problems", color="steelblue", linewidth=2
    )
    ax3.plot(
        thresholds,
        solution_cumulative,
        label="Solutions",
        color="seagreen",
        linewidth=2,
    )
    ax3.axvline(x=0.60, color="red", linestyle="--", linewidth=1, alpha=0.7)
    ax3.set_xlabel("Similarity Threshold")
    ax3.set_ylabel("% of Pairs Above Threshold")
    ax3.set_title("Cumulative Distribution: % Pairs Above Threshold")
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.set_xlim(0, 1)

    # Comparison box plot
    ax4 = axes[1, 1]
    box_data = [problem_similarities, solution_similarities]
    bp = ax4.boxplot(box_data, labels=["Problems", "Solutions"], patch_artist=True)
    bp["boxes"][0].set_facecolor("steelblue")
    bp["boxes"][1].set_facecolor("seagreen")
    ax4.axhline(y=0.60, color="red", linestyle="--", linewidth=2, label="60% threshold")
    ax4.set_ylabel("Cosine Similarity")
    ax4.set_title("Similarity Comparison: Problems vs Solutions")
    ax4.legend()
    ax4.set_ylim(0, 1)

    plt.tight_layout()

    # Save the figure
    output_path = "/tmp/patent_similarity_distribution.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()

    context.log.info(f"Saved visualization to {output_path}")

    # Add metadata
    context.add_output_metadata(
        {
            "sample_size": n,
            "total_patents": total_patents,
            "num_pairs_analyzed": num_pairs,
            "problem_mean_similarity": round(problem_stats["mean"], 4),
            "problem_std_similarity": round(problem_stats["std"], 4),
            "problem_pct_above_60": round(problem_stats["pct_above_60"], 2),
            "solution_mean_similarity": round(solution_stats["mean"], 4),
            "solution_std_similarity": round(solution_stats["std"], 4),
            "solution_pct_above_60": round(solution_stats["pct_above_60"], 2),
            "visualization": MetadataValue.path(output_path),
        }
    )
