"""Fetch OpenRouter generation metadata for cost and provider analytics."""

import os
from typing import Any

import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset

from _shared import DuckLakeResource


def fetch_generation_metadata(
    generation_id: str, api_key: str
) -> dict[str, Any] | None:
    """Fetch metadata for a generation from OpenRouter API.

    Args:
        generation_id: OpenRouter generation ID
        api_key: OpenRouter API key

    Returns:
        Dict with generation metadata, or None if request fails

    Response includes:
        - provider_name: Actual provider that served the request
        - total_cost: Cost in USD
        - tokens_prompt: Prompt tokens used
        - tokens_completion: Completion tokens used
        - latency: Request latency in ms
        - generation_time: Generation time in ms
        - And more...
    """
    url = f"https://openrouter.ai/api/v1/generation?id={generation_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Extract the data field from response
        if "data" in data:
            return data["data"]
        return data

    except requests.exceptions.RequestException as e:
        return None


@asset(
    group_name="patent_extraction",
    description=(
        "Fetches provider and cost information from OpenRouter for patent problem-solution extractions. "
        "Uses generation IDs to retrieve detailed metadata including actual provider, "
        "token usage, costs, and performance metrics. Stores results in extraction_generation_metadata table."
    ),
    kinds={"python", "openrouter", "ducklake"},
    metadata={
        "analytics_table": "extraction_generation_metadata",
        "api": "openrouter_generations",
    },
    deps=["patent_problems_solutions"],
    code_version="1.1.0",
)
def extraction_generation_metadata(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> None:
    """Fetch and store OpenRouter generation metadata for problem-solution extractions."""

    # Configuration
    EXTRACTION_TABLE = "patent_problems_solutions"
    METADATA_TABLE = "extraction_generation_metadata"
    COMMIT_BATCH_SIZE = 50  # Commit to database every N items for resumability

    # Get API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENROUTER_API_KEY environment variable not set. "
            "Required to fetch generation metadata."
        )

    # Ensure metadata table exists
    with ducklake.get_connection() as conn:
        full_table_name = f"{ducklake.ducklake_schema}.{METADATA_TABLE}"

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                generation_id VARCHAR NOT NULL,
                publication_number VARCHAR,
                
                -- Provider and model info
                provider_name VARCHAR,
                model VARCHAR,
                
                -- Cost information
                total_cost DOUBLE,
                cache_discount DOUBLE,
                upstream_inference_cost DOUBLE,
                
                -- Token usage
                tokens_prompt INTEGER,
                tokens_completion INTEGER,
                native_tokens_prompt INTEGER,
                native_tokens_completion INTEGER,
                native_tokens_cached INTEGER,
                
                -- Performance metrics
                latency DOUBLE,
                generation_time DOUBLE,
                
                -- Request metadata
                created_at TIMESTAMP,
                finish_reason VARCHAR,
                streamed BOOLEAN,
                
                -- Fetch metadata
                fetched_at TIMESTAMP
            )
        """)
        context.log.info(f"Ensured {METADATA_TABLE} table exists")

        # Query for ALL generation IDs that haven't been fetched yet
        query = f"""
            SELECT 
                e.generation_id,
                e.publication_number
            FROM {ducklake.ducklake_schema}.{EXTRACTION_TABLE} AS e
            LEFT JOIN {full_table_name} AS m
                ON e.generation_id = m.generation_id
            WHERE m.generation_id IS NULL
        """

        unfetched_df = conn.execute(query).fetchdf()

    unfetched_count = len(unfetched_df)

    if unfetched_count == 0:
        context.log.info("No new generation IDs to fetch metadata for")
        context.add_output_metadata(
            metadata={
                "status": "all_fetched",
                "newly_fetched": 0,
                "table_name": METADATA_TABLE,
            }
        )
        return

    context.log.info(
        f"Fetching metadata for {unfetched_count} generation IDs from OpenRouter..."
    )

    # Fetch metadata for each generation
    successful_count = 0
    failed_count = 0
    metadata_batch = []

    # Prepare database insert query for batch commits
    insert_query = f"""
        INSERT INTO {full_table_name} (
            generation_id,
            publication_number,
            provider_name,
            model,
            total_cost,
            cache_discount,
            upstream_inference_cost,
            tokens_prompt,
            tokens_completion,
            native_tokens_prompt,
            native_tokens_completion,
            native_tokens_cached,
            latency,
            generation_time,
            created_at,
            finish_reason,
            streamed,
            fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """

    for idx, (_, row) in enumerate(unfetched_df.iterrows()):
        generation_id = str(row["generation_id"])
        publication_number = str(row["publication_number"])

        try:
            metadata = fetch_generation_metadata(generation_id, api_key)

            if metadata:
                metadata_batch.append(
                    {
                        "generation_id": generation_id,
                        "publication_number": publication_number,
                        "provider_name": metadata.get("provider_name"),
                        "model": metadata.get("model"),
                        "total_cost": metadata.get("total_cost"),
                        "cache_discount": metadata.get("cache_discount"),
                        "upstream_inference_cost": metadata.get(
                            "upstream_inference_cost"
                        ),
                        "tokens_prompt": metadata.get("tokens_prompt"),
                        "tokens_completion": metadata.get("tokens_completion"),
                        "native_tokens_prompt": metadata.get("native_tokens_prompt"),
                        "native_tokens_completion": metadata.get(
                            "native_tokens_completion"
                        ),
                        "native_tokens_cached": metadata.get("native_tokens_cached"),
                        "latency": metadata.get("latency"),
                        "generation_time": metadata.get("generation_time"),
                        "created_at": metadata.get("created_at"),
                        "finish_reason": metadata.get("finish_reason"),
                        "streamed": metadata.get("streamed"),
                    }
                )
                successful_count += 1
            else:
                context.log.warning(
                    f"Failed to fetch metadata for generation {generation_id}"
                )
                failed_count += 1

        except Exception as exc:
            context.log.warning(
                f"Error fetching metadata for generation {generation_id}: {exc}"
            )
            failed_count += 1
            continue

        # Commit batch to database periodically for resumability
        if len(metadata_batch) >= COMMIT_BATCH_SIZE:
            with ducklake.get_connection() as conn:
                batch_data = [
                    (
                        item["generation_id"],
                        item["publication_number"],
                        item["provider_name"],
                        item["model"],
                        item["total_cost"],
                        item["cache_discount"],
                        item["upstream_inference_cost"],
                        item["tokens_prompt"],
                        item["tokens_completion"],
                        item["native_tokens_prompt"],
                        item["native_tokens_completion"],
                        item["native_tokens_cached"],
                        item["latency"],
                        item["generation_time"],
                        item["created_at"],
                        item["finish_reason"],
                        item["streamed"],
                    )
                    for item in metadata_batch
                ]
                conn.executemany(insert_query, batch_data)

            context.log.info(
                f"Committed batch: {len(metadata_batch)} records | "
                f"Progress: {idx + 1}/{unfetched_count} ({round((idx + 1) / unfetched_count * 100, 1)}%) | "
                f"Total success: {successful_count} | Failed: {failed_count}"
            )
            metadata_batch = []

        # Log progress periodically
        elif (idx + 1) % 10 == 0:
            progress_pct = round((idx + 1) / unfetched_count * 100, 1)
            context.log.info(
                f"Progress: {idx + 1}/{unfetched_count} ({progress_pct}%) | "
                f"Success: {successful_count} | Failed: {failed_count}"
            )

    # Store any remaining metadata in database
    if metadata_batch:
        with ducklake.get_connection() as conn:
            batch_data = [
                (
                    item["generation_id"],
                    item["publication_number"],
                    item["provider_name"],
                    item["model"],
                    item["total_cost"],
                    item["cache_discount"],
                    item["upstream_inference_cost"],
                    item["tokens_prompt"],
                    item["tokens_completion"],
                    item["native_tokens_prompt"],
                    item["native_tokens_completion"],
                    item["native_tokens_cached"],
                    item["latency"],
                    item["generation_time"],
                    item["created_at"],
                    item["finish_reason"],
                    item["streamed"],
                )
                for item in metadata_batch
            ]

            conn.executemany(insert_query, batch_data)
            context.log.info(f"Stored final batch: {len(metadata_batch)} generations")

    # Query for sample metadata with costs
    with ducklake.get_connection() as conn:
        sample_df = conn.execute(f"""
            SELECT 
                generation_id,
                provider_name,
                total_cost,
                tokens_prompt,
                tokens_completion,
                latency
            FROM {full_table_name}
            ORDER BY fetched_at DESC
            LIMIT 5
        """).fetchdf()

        # Calculate total costs
        total_cost_result = conn.execute(f"""
            SELECT 
                COUNT(*) as total_generations,
                SUM(total_cost) as total_cost_usd,
                AVG(total_cost) as avg_cost_per_generation,
                AVG(tokens_prompt) as avg_prompt_tokens,
                AVG(tokens_completion) as avg_completion_tokens,
                AVG(latency) as avg_latency_ms
            FROM {full_table_name}
        """).fetchone()

    # Format sample preview
    preview_rows = []
    for _, row in sample_df.iterrows():
        gen_id = str(row["generation_id"])[:16] + "..."

        try:
            provider = str(row["provider_name"])
            if not provider or provider == "nan" or provider == "None":
                provider = "Unknown"
        except:
            provider = "Unknown"

        try:
            cost_val = float(row["total_cost"])
            cost = f"${cost_val:.6f}"
        except (ValueError, TypeError):
            cost = "N/A"

        tokens = f"{row['tokens_prompt']}+{row['tokens_completion']}"

        try:
            latency_val = float(row["latency"])
            latency = f"{latency_val:.0f}ms"
        except (ValueError, TypeError):
            latency = "N/A"

        preview_rows.append(
            f"- **{gen_id}** | Provider: {provider} | Cost: {cost} | Tokens: {tokens} | Latency: {latency}"
        )

    # Add output metadata
    stats = {
        "newly_fetched": int(successful_count),
        "failed_count": int(failed_count),
        "table_name": METADATA_TABLE,
    }

    if total_cost_result:
        stats.update(
            {
                "total_generations": int(total_cost_result[0]),
                "total_cost_usd": float(total_cost_result[1])
                if total_cost_result[1]
                else 0.0,
                "avg_cost_per_generation": float(total_cost_result[2])
                if total_cost_result[2]
                else 0.0,
                "avg_prompt_tokens": float(total_cost_result[3])
                if total_cost_result[3]
                else 0.0,
                "avg_completion_tokens": float(total_cost_result[4])
                if total_cost_result[4]
                else 0.0,
                "avg_latency_ms": float(total_cost_result[5])
                if total_cost_result[5]
                else 0.0,
            }
        )

    stats["preview"] = MetadataValue.md(
        "### Recent Generation Metadata\n\n" + "\n".join(preview_rows)
        if preview_rows
        else "No metadata fetched yet"
    )

    context.add_output_metadata(metadata=stats)

    context.log.info(
        f"Metadata fetch complete: {successful_count}/{unfetched_count} successful, "
        f"{failed_count} failed"
    )


@asset(
    group_name="patent_extraction",
    description=(
        "Fetches provider and cost information from OpenRouter for patent embeddings generation. "
        "Uses generation IDs to retrieve detailed metadata including actual provider, "
        "token usage, costs, and performance metrics. Tracks batch sizes since embeddings "
        "are generated in batches. Stores results in embeddings_generation_metadata table."
    ),
    kinds={"python", "openrouter", "ducklake"},
    metadata={
        "analytics_table": "embeddings_generation_metadata",
        "api": "openrouter_generations",
        "batch_aware": True,
    },
    deps=["patent_embeddings"],
    code_version="1.0.0",
)
def embeddings_generation_metadata(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> None:
    """Fetch and store OpenRouter generation metadata for embeddings generation."""

    # Configuration
    SOURCE_TABLE = "patent_problems_solutions"
    METADATA_TABLE = "embeddings_generation_metadata"
    COMMIT_BATCH_SIZE = 50  # Commit to database every N items for resumability

    # Get API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENROUTER_API_KEY environment variable not set. "
            "Required to fetch generation metadata."
        )

    # Ensure metadata table exists
    with ducklake.get_connection() as conn:
        full_table_name = f"{ducklake.ducklake_schema}.{METADATA_TABLE}"

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                generation_id VARCHAR NOT NULL,
                
                -- Batch processing info
                patents_in_batch INTEGER,
                embeddings_in_batch INTEGER,
                
                -- Provider and model info
                provider_name VARCHAR,
                model VARCHAR,
                
                -- Cost information
                total_cost DOUBLE,
                cache_discount DOUBLE,
                upstream_inference_cost DOUBLE,
                
                -- Token usage
                tokens_prompt INTEGER,
                tokens_completion INTEGER,
                native_tokens_prompt INTEGER,
                native_tokens_completion INTEGER,
                native_tokens_cached INTEGER,
                
                -- Performance metrics
                latency DOUBLE,
                generation_time DOUBLE,
                
                -- Request metadata
                created_at TIMESTAMP,
                finish_reason VARCHAR,
                streamed BOOLEAN,
                
                -- Fetch metadata
                fetched_at TIMESTAMP
            )
        """)
        context.log.info(f"Ensured {METADATA_TABLE} table exists")

        # Query for ALL generation IDs that haven't been fetched yet
        # Group by generation_id to count patents per batch
        query = f"""
            SELECT 
                e.embedding_generation_id as generation_id,
                COUNT(*) as patents_in_batch,
                COUNT(*) * 2 as embeddings_in_batch
            FROM {ducklake.ducklake_schema}.{SOURCE_TABLE} AS e
            LEFT JOIN {full_table_name} AS m
                ON e.embedding_generation_id = m.generation_id
            WHERE e.embedding_generation_id IS NOT NULL
                AND m.generation_id IS NULL
            GROUP BY e.embedding_generation_id
        """

        unfetched_df = conn.execute(query).fetchdf()

    unfetched_count = len(unfetched_df)

    if unfetched_count == 0:
        context.log.info("No new generation IDs to fetch metadata for")
        context.add_output_metadata(
            metadata={
                "status": "all_fetched",
                "newly_fetched": 0,
                "table_name": METADATA_TABLE,
            }
        )
        return

    context.log.info(
        f"Fetching metadata for {unfetched_count} generation IDs from OpenRouter..."
    )

    # Fetch metadata for each generation
    successful_count = 0
    failed_count = 0
    metadata_batch = []

    # Prepare database insert query for batch commits
    insert_query = f"""
        INSERT INTO {full_table_name} (
            generation_id,
            patents_in_batch,
            embeddings_in_batch,
            provider_name,
            model,
            total_cost,
            cache_discount,
            upstream_inference_cost,
            tokens_prompt,
            tokens_completion,
            native_tokens_prompt,
            native_tokens_completion,
            native_tokens_cached,
            latency,
            generation_time,
            created_at,
            finish_reason,
            streamed,
            fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """

    for idx, (_, row) in enumerate(unfetched_df.iterrows()):
        generation_id = str(row["generation_id"])
        patents_in_batch = int(row["patents_in_batch"])
        embeddings_in_batch = int(row["embeddings_in_batch"])

        try:
            metadata = fetch_generation_metadata(generation_id, api_key)

            if metadata:
                metadata_batch.append(
                    {
                        "generation_id": generation_id,
                        "patents_in_batch": patents_in_batch,
                        "embeddings_in_batch": embeddings_in_batch,
                        "provider_name": metadata.get("provider_name"),
                        "model": metadata.get("model"),
                        "total_cost": metadata.get("total_cost"),
                        "cache_discount": metadata.get("cache_discount"),
                        "upstream_inference_cost": metadata.get(
                            "upstream_inference_cost"
                        ),
                        "tokens_prompt": metadata.get("tokens_prompt"),
                        "tokens_completion": metadata.get("tokens_completion"),
                        "native_tokens_prompt": metadata.get("native_tokens_prompt"),
                        "native_tokens_completion": metadata.get(
                            "native_tokens_completion"
                        ),
                        "native_tokens_cached": metadata.get("native_tokens_cached"),
                        "latency": metadata.get("latency"),
                        "generation_time": metadata.get("generation_time"),
                        "created_at": metadata.get("created_at"),
                        "finish_reason": metadata.get("finish_reason"),
                        "streamed": metadata.get("streamed"),
                    }
                )
                successful_count += 1
            else:
                context.log.warning(
                    f"Failed to fetch metadata for generation {generation_id}"
                )
                failed_count += 1

        except Exception as exc:
            context.log.warning(
                f"Error fetching metadata for generation {generation_id}: {exc}"
            )
            failed_count += 1
            continue

        # Commit batch to database periodically for resumability
        if len(metadata_batch) >= COMMIT_BATCH_SIZE:
            with ducklake.get_connection() as conn:
                batch_data = [
                    (
                        item["generation_id"],
                        item["patents_in_batch"],
                        item["embeddings_in_batch"],
                        item["provider_name"],
                        item["model"],
                        item["total_cost"],
                        item["cache_discount"],
                        item["upstream_inference_cost"],
                        item["tokens_prompt"],
                        item["tokens_completion"],
                        item["native_tokens_prompt"],
                        item["native_tokens_completion"],
                        item["native_tokens_cached"],
                        item["latency"],
                        item["generation_time"],
                        item["created_at"],
                        item["finish_reason"],
                        item["streamed"],
                    )
                    for item in metadata_batch
                ]
                conn.executemany(insert_query, batch_data)

            context.log.info(
                f"Committed batch: {len(metadata_batch)} records | "
                f"Progress: {idx + 1}/{unfetched_count} ({round((idx + 1) / unfetched_count * 100, 1)}%) | "
                f"Total success: {successful_count} | Failed: {failed_count}"
            )
            metadata_batch = []

        # Log progress periodically
        elif (idx + 1) % 10 == 0:
            progress_pct = round((idx + 1) / unfetched_count * 100, 1)
            context.log.info(
                f"Progress: {idx + 1}/{unfetched_count} ({progress_pct}%) | "
                f"Success: {successful_count} | Failed: {failed_count}"
            )

    # Store any remaining metadata in database
    if metadata_batch:
        with ducklake.get_connection() as conn:
            batch_data = [
                (
                    item["generation_id"],
                    item["patents_in_batch"],
                    item["embeddings_in_batch"],
                    item["provider_name"],
                    item["model"],
                    item["total_cost"],
                    item["cache_discount"],
                    item["upstream_inference_cost"],
                    item["tokens_prompt"],
                    item["tokens_completion"],
                    item["native_tokens_prompt"],
                    item["native_tokens_completion"],
                    item["native_tokens_cached"],
                    item["latency"],
                    item["generation_time"],
                    item["created_at"],
                    item["finish_reason"],
                    item["streamed"],
                )
                for item in metadata_batch
            ]

            conn.executemany(insert_query, batch_data)
            context.log.info(f"Stored final batch: {len(metadata_batch)} generations")

    # Query for sample metadata with costs
    with ducklake.get_connection() as conn:
        sample_df = conn.execute(f"""
            SELECT 
                generation_id,
                patents_in_batch,
                embeddings_in_batch,
                provider_name,
                total_cost,
                tokens_prompt,
                tokens_completion,
                latency
            FROM {full_table_name}
            ORDER BY fetched_at DESC
            LIMIT 5
        """).fetchdf()

        # Calculate total costs and stats
        total_cost_result = conn.execute(f"""
            SELECT 
                COUNT(*) as total_generations,
                SUM(patents_in_batch) as total_patents_embedded,
                SUM(embeddings_in_batch) as total_embeddings_generated,
                SUM(total_cost) as total_cost_usd,
                AVG(total_cost) as avg_cost_per_generation,
                AVG(total_cost / NULLIF(patents_in_batch, 0)) as avg_cost_per_patent,
                AVG(total_cost / NULLIF(embeddings_in_batch, 0)) as avg_cost_per_embedding,
                AVG(tokens_prompt) as avg_prompt_tokens,
                AVG(tokens_completion) as avg_completion_tokens,
                AVG(latency) as avg_latency_ms,
                AVG(patents_in_batch) as avg_patents_per_batch,
                AVG(embeddings_in_batch) as avg_embeddings_per_batch
            FROM {full_table_name}
        """).fetchone()

    # Format sample preview
    preview_rows = []
    for _, row in sample_df.iterrows():
        gen_id = str(row["generation_id"])[:16] + "..."

        try:
            provider = str(row["provider_name"])
            if not provider or provider == "nan" or provider == "None":
                provider = "Unknown"
        except:
            provider = "Unknown"

        try:
            cost_val = float(row["total_cost"])
            cost = f"${cost_val:.6f}"
        except (ValueError, TypeError):
            cost = "N/A"

        try:
            patents = int(row["patents_in_batch"])
        except (ValueError, TypeError):
            patents = 0

        try:
            embeddings = int(row["embeddings_in_batch"])
        except (ValueError, TypeError):
            embeddings = 0

        tokens = f"{row['tokens_prompt']}+{row['tokens_completion']}"

        try:
            latency_val = float(row["latency"])
            latency = f"{latency_val:.0f}ms"
        except (ValueError, TypeError):
            latency = "N/A"

        preview_rows.append(
            f"- **{gen_id}** | Batch: {patents} patents ({embeddings} embeddings) | "
            f"Provider: {provider} | Cost: {cost} | Tokens: {tokens} | Latency: {latency}"
        )

    # Add output metadata
    stats = {
        "newly_fetched": int(successful_count),
        "failed_count": int(failed_count),
        "table_name": METADATA_TABLE,
    }

    if total_cost_result:
        stats.update(
            {
                "total_generations": int(total_cost_result[0]),
                "total_patents_embedded": int(total_cost_result[1])
                if total_cost_result[1]
                else 0,
                "total_embeddings_generated": int(total_cost_result[2])
                if total_cost_result[2]
                else 0,
                "total_cost_usd": float(total_cost_result[3])
                if total_cost_result[3]
                else 0.0,
                "avg_cost_per_generation": float(total_cost_result[4])
                if total_cost_result[4]
                else 0.0,
                "avg_cost_per_patent": float(total_cost_result[5])
                if total_cost_result[5]
                else 0.0,
                "avg_cost_per_embedding": float(total_cost_result[6])
                if total_cost_result[6]
                else 0.0,
                "avg_prompt_tokens": float(total_cost_result[7])
                if total_cost_result[7]
                else 0.0,
                "avg_completion_tokens": float(total_cost_result[8])
                if total_cost_result[8]
                else 0.0,
                "avg_latency_ms": float(total_cost_result[9])
                if total_cost_result[9]
                else 0.0,
                "avg_patents_per_batch": float(total_cost_result[10])
                if total_cost_result[10]
                else 0.0,
                "avg_embeddings_per_batch": float(total_cost_result[11])
                if total_cost_result[11]
                else 0.0,
            }
        )

    stats["preview"] = MetadataValue.md(
        "### Recent Generation Metadata\n\n" + "\n".join(preview_rows)
        if preview_rows
        else "No metadata fetched yet"
    )

    context.add_output_metadata(metadata=stats)

    context.log.info(
        f"Metadata fetch complete: {successful_count}/{unfetched_count} successful, "
        f"{failed_count} failed"
    )
