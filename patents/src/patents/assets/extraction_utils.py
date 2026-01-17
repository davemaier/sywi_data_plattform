"""Utility functions for patent data extraction with versioning and generation tracking."""

import hashlib
import time
from typing import Any

import pandas as pd

from sywi_core import DuckLakeResource


def parse_model_info(model_string: str) -> dict:
    """Parse model string to extract provider and model name.

    Args:
        model_string: Full model string (e.g., "openrouter/meta-llama/llama-4-maverick")

    Returns:
        Dict with 'provider', 'model_name', 'full_string'

    Examples:
        >>> parse_model_info("openrouter/meta-llama/llama-4-maverick")
        {
            'provider': 'openrouter',
            'model_name': 'meta-llama/llama-4-maverick',
            'full_string': 'openrouter/meta-llama/llama-4-maverick'
        }
    """
    parts = model_string.split("/", 1)

    if len(parts) > 1:
        return {
            "provider": parts[0],
            "model_name": parts[1],
            "full_string": model_string,
        }
    else:
        return {
            "provider": "unknown",
            "model_name": model_string,
            "full_string": model_string,
        }


def get_prompt_version(signature_class) -> str:
    """Generate a version hash from a DSPy signature class.

    The version is based on the signature's class name, docstring,
    and field descriptions. Any change to these will produce a different hash.

    Args:
        signature_class: DSPy Signature class

    Returns:
        Version string in format "v_<10_char_hash>"

    Examples:
        >>> get_prompt_version(PatentProblemSolutionExtraction)
        'v_0dd391589e'
    """
    # Build string representation from class metadata
    parts = [
        signature_class.__name__,
        signature_class.__doc__ or "",
    ]

    # Add field descriptions if available
    if hasattr(signature_class, "__annotations__"):
        # Get field descriptions from class attributes
        for field_name in signature_class.__annotations__.keys():
            if hasattr(signature_class, field_name):
                field_obj = getattr(signature_class, field_name)
                # DSPy fields have a 'desc' or '__metadata__' attribute
                if hasattr(field_obj, "json_schema_extra"):
                    desc = field_obj.json_schema_extra.get("desc", "")
                    parts.append(f"{field_name}:{desc}")

    # Create hash
    content = "|".join(parts)
    hash_obj = hashlib.sha256(content.encode())
    return f"v_{hash_obj.hexdigest()[:10]}"


def extract_generation_id(lm: Any) -> str | None:
    """Extract OpenRouter generation ID from DSPy LM history.

    Args:
        lm: DSPy LM object with history attribute

    Returns:
        Generation ID string, or None if not found
    """
    if not hasattr(lm, "history") or not lm.history:
        return None

    # Get the last call from history
    last_call = lm.history[-1]

    # Try different possible locations for the generation ID
    if isinstance(last_call, dict):
        # Try response.id attribute (litellm ModelResponse object)
        if "response" in last_call:
            response = last_call["response"]

            # Check if response has id attribute (litellm ModelResponse)
            if hasattr(response, "id"):
                return response.id

            # Fallback: try dict access
            if isinstance(response, dict) and "id" in response:
                return response["id"]

        # Try direct id field
        if "id" in last_call:
            return last_call["id"]

    return None


def ensure_extraction_table_exists(ducklake: DuckLakeResource, table_name: str) -> None:
    """Create the patent extraction table if it doesn't exist.

    Args:
        ducklake: DuckLake resource
        table_name: Name of the extraction table to create
    """
    with ducklake.get_connection() as conn:
        full_table_name = f"{ducklake.ducklake_schema}.{table_name}"

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                -- Primary identifiers
                publication_number VARCHAR NOT NULL,
                
                -- Patent basic info (denormalized)
                title_text VARCHAR,
                abstract_text VARCHAR,
                
                -- Extraction configuration/versioning
                model_name VARCHAR NOT NULL,
                model_provider VARCHAR NOT NULL,
                prompt_version VARCHAR NOT NULL,
                
                -- OpenRouter generation tracking
                generation_id VARCHAR NOT NULL,
                
                -- Extraction results
                problem TEXT NOT NULL,
                solution TEXT NOT NULL,
                
                -- Vector embeddings (DOUBLE[] for DuckLake compatibility)
                problem_embedding DOUBLE[],
                solution_embedding DOUBLE[],
                embedding_generation_id VARCHAR,
                
                -- Metadata
                extraction_timestamp TIMESTAMP,
                processing_time_ms INTEGER
            )
        """)

        # For existing tables, add embedding columns if they don't exist
        # DuckLake uses DOUBLE[] (dynamic list) instead of FLOAT[N] (fixed array)
        try:
            conn.execute(f"""
                ALTER TABLE {full_table_name}
                ADD COLUMN IF NOT EXISTS problem_embedding DOUBLE[]
            """)
            conn.execute(f"""
                ALTER TABLE {full_table_name}
                ADD COLUMN IF NOT EXISTS solution_embedding DOUBLE[]
            """)
            conn.execute(f"""
                ALTER TABLE {full_table_name}
                ADD COLUMN IF NOT EXISTS embedding_generation_id VARCHAR
            """)
        except Exception:
            # Columns likely already exist, that's OK
            pass


def get_unprocessed_patents(
    conn,
    ducklake_schema: str,
    source_table: str,
    extraction_table: str,
    model_name: str,
    prompt_version: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """Query patents that haven't been processed with the given model+prompt version.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        source_table: Source patents table name
        extraction_table: Extractions table name
        model_name: Model name to check for
        prompt_version: Prompt version to check for
        limit: Optional limit on number of patents to return

    Returns:
        DataFrame with unprocessed patents (publication_number, title_text, abstract_text)
    """
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT 
            src.publication_number,
            src.title_text,
            src.abstract_text
        FROM {ducklake_schema}.{source_table} AS src
        LEFT JOIN {ducklake_schema}.{extraction_table} AS ext
            ON src.publication_number = ext.publication_number
            AND ext.model_name = ?
            AND ext.prompt_version = ?
        WHERE ext.publication_number IS NULL
        {limit_clause}
    """

    return conn.execute(query, [model_name, prompt_version]).fetchdf()


def store_extraction_batch(
    conn, ducklake_schema: str, table_name: str, batch: list[dict]
) -> None:
    """Batch insert extraction results into the table.

    Note: Without PRIMARY KEY support in DuckLake, duplicates are prevented
    by the get_unprocessed_patents query which filters out already-processed patents.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        table_name: Extraction table name
        batch: List of dicts with extraction data
    """
    if not batch:
        return

    full_table_name = f"{ducklake_schema}.{table_name}"

    # Build insert query
    # Note: No ON CONFLICT needed since we pre-filter unprocessed patents
    insert_query = f"""
        INSERT INTO {full_table_name} (
            publication_number,
            title_text,
            abstract_text,
            model_name,
            model_provider,
            prompt_version,
            generation_id,
            problem,
            solution,
            extraction_timestamp,
            processing_time_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    """

    # Prepare batch data
    batch_data = [
        (
            item["publication_number"],
            item["title_text"],
            item["abstract_text"],
            item["model_name"],
            item["model_provider"],
            item["prompt_version"],
            item["generation_id"],
            item["problem"],
            item["solution"],
            item["processing_time_ms"],
        )
        for item in batch
    ]

    # Execute batch insert
    conn.executemany(insert_query, batch_data)


def get_total_patent_count(conn, ducklake_schema: str, source_table: str) -> int:
    """Get total count of patents in source table.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        source_table: Source patents table name

    Returns:
        Total patent count
    """
    result = conn.execute(
        f"SELECT COUNT(*) FROM {ducklake_schema}.{source_table}"
    ).fetchone()
    return result[0] if result else 0


def get_processed_count(
    conn,
    ducklake_schema: str,
    extraction_table: str,
    model_name: str,
    prompt_version: str,
) -> int:
    """Get count of already processed patents for given model+prompt.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        extraction_table: Extractions table name
        model_name: Model name
        prompt_version: Prompt version

    Returns:
        Count of processed patents
    """
    result = conn.execute(
        f"""
        SELECT COUNT(*) 
        FROM {ducklake_schema}.{extraction_table}
        WHERE model_name = ? AND prompt_version = ?
        """,
        [model_name, prompt_version],
    ).fetchone()
    return result[0] if result else 0


def setup_embedder():
    """Configure Qwen3-Embedding-8B embedder for OpenRouter.

    Returns:
        DSPy Embedder configured for instruction-aware embeddings.
        Model: qwen/qwen3-embedding-8b (4096 dimensions)
        Cost: $0.01/M input tokens
    """
    import os

    import dspy

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENROUTER_API_KEY environment variable not set. "
            "Please set it to use the embeddings feature."
        )

    # Note: pyright shows type errors for kwargs but they work correctly at runtime
    # Use custom_llm_provider="openai" since OpenRouter uses OpenAI-compatible API
    embedder = dspy.Embedder(
        "qwen/qwen3-embedding-8b",
        batch_size=100,
        api_base="https://openrouter.ai/api/v1",  # type: ignore
        api_key=api_key,  # type: ignore
        custom_llm_provider="openai",  # type: ignore
    )
    return embedder


def get_detailed_instruct(task_description: str, text: str) -> str:
    """Format text with Qwen3 instruction format.

    Args:
        task_description: One-sentence task description
        text: Text to embed

    Returns:
        Formatted string: 'Instruct: {task_description}\nQuery: {text}'
    """
    return f"Instruct: {task_description}\nQuery: {text}"


def get_patents_without_embeddings(
    conn,
    ducklake_schema: str,
    table_name: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """Query patents that don't have embeddings yet.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        table_name: Extraction table name (patent_problems_solutions)
        limit: Optional limit on number of patents to return

    Returns:
        DataFrame with patents needing embeddings (publication_number, problem, solution)
    """
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT 
            publication_number,
            problem,
            solution
        FROM {ducklake_schema}.{table_name}
        WHERE problem_embedding IS NULL 
           OR solution_embedding IS NULL
        {limit_clause}
    """

    return conn.execute(query).fetchdf()


def store_embeddings_batch(
    conn,
    ducklake_schema: str,
    table_name: str,
    batch: list[dict],
) -> None:
    """Batch UPDATE embeddings for patents.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        table_name: Extraction table name
        batch: List of dicts with embedding data:
            - publication_number
            - problem_embedding (list of 4096 floats)
            - solution_embedding (list of 4096 floats)
            - embedding_generation_id
    """
    if not batch:
        return

    full_table_name = f"{ducklake_schema}.{table_name}"

    # Build UPDATE query
    update_query = f"""
        UPDATE {full_table_name}
        SET 
            problem_embedding = ?,
            solution_embedding = ?,
            embedding_generation_id = ?
        WHERE publication_number = ?
    """

    # Prepare batch data
    batch_data = [
        (
            item["problem_embedding"],
            item["solution_embedding"],
            item["embedding_generation_id"],
            item["publication_number"],
        )
        for item in batch
    ]

    # Execute batch update
    conn.executemany(update_query, batch_data)


def get_embeddings_stats(
    conn,
    ducklake_schema: str,
    table_name: str,
) -> dict:
    """Get counts of patents with/without embeddings.

    Args:
        conn: DuckDB connection
        ducklake_schema: Schema name
        table_name: Extraction table name

    Returns:
        Dict with total, with_embeddings, without_embeddings counts
    """
    result = conn.execute(
        f"""
        SELECT 
            COUNT(*) as total,
            COUNT(problem_embedding) as with_embeddings,
            COUNT(*) - COUNT(problem_embedding) as without_embeddings
        FROM {ducklake_schema}.{table_name}
        """
    ).fetchone()

    return {
        "total": int(result[0]) if result else 0,
        "with_embeddings": int(result[1]) if result else 0,
        "without_embeddings": int(result[2]) if result else 0,
    }
