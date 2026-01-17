"""Patent problem-solution extraction using LLM with generation tracking."""

import os
import time
import uuid

import dspy
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from sywi_core import DuckLakeResource
from .extraction_utils import (
    ensure_extraction_table_exists,
    extract_generation_id,
    get_detailed_instruct,
    get_embeddings_stats,
    get_patents_without_embeddings,
    get_processed_count,
    get_prompt_version,
    get_total_patent_count,
    get_unprocessed_patents,
    parse_model_info,
    store_embeddings_batch,
    store_extraction_batch,
)


class PatentProblemSolutionExtraction(dspy.Signature):
    """Extract the problem a patent solves and its solution from title and abstract."""

    title: str = dspy.InputField(desc="The patent title")
    abstract: str = dspy.InputField(desc="The patent abstract")
    problem: str = dspy.OutputField(
        desc="A concise description (1-2 sentences) of the problem this patent solves"
    )
    solution: str = dspy.OutputField(
        desc="A concise description (1-2 sentences) of the solution this patent proposes"
    )


def setup_llm():
    """Configure the language model for DSPy using OpenRouter."""
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENROUTER_API_KEY environment variable not set. "
            "Please set it to use the solution extraction feature."
        )

    lm = dspy.LM(
        "openrouter/meta-llama/llama-4-maverick",
        api_key=api_key,
        api_base="https://openrouter.ai/api/v1",
        max_tokens=200,
        temperature=0.3,  # Some creativity for good summaries
    )
    dspy.configure(lm=lm)
    return lm


# Task descriptions for instruction tuning
PROBLEM_TASK = "Given a patent problem description, retrieve similar technical problems that companies or researchers are trying to solve"
SOLUTION_TASK = "Given a patent solution description, retrieve similar technical solutions or innovations that address comparable challenges"


@asset(
    group_name="patent_extraction",
    description=(
        "Extracts problems and solutions from patents using LLM. "
        "Stores results in patent_problems_solutions table with OpenRouter generation IDs. "
        "Resumable - skips patents already processed with this model+prompt version."
    ),
    kinds={"python", "dspy", "ducklake"},
    metadata={
        "model": "meta-llama-4-maverick",
        "table_name": "patent_problems_solutions",
        "batch_size": 10,
        "resumable": True,
    },
    deps=["cleaned_patents_data"],  # Declare dependency on source data table
    code_version="1.0.0",
)
def patent_problems_solutions(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> None:
    """Extract problem-solution pairs from patents, stored incrementally with generation IDs."""

    # Configuration
    MODEL_STRING = "openrouter/meta-llama/llama-4-maverick"
    TABLE_NAME = "patent_problems_solutions"
    SOURCE_TABLE = "cleaned_patents_data"
    BATCH_SIZE = 10
    TEST_LIMIT = 735  # Only process first N patents for testing

    # Parse model information
    model_info = parse_model_info(MODEL_STRING)
    context.log.info(
        f"Using model: {model_info['model_name']} via {model_info['provider']}"
    )
    context.log.warning(f"TEST MODE: Limited to first {TEST_LIMIT} patents")

    # Get prompt version from signature
    prompt_version = get_prompt_version(PatentProblemSolutionExtraction)
    context.log.info(f"Prompt version: {prompt_version}")

    # Ensure extraction table exists
    ensure_extraction_table_exists(ducklake, TABLE_NAME)
    context.log.info(f"Ensured table {TABLE_NAME} exists")

    # Setup LLM and get LM object for history access
    lm = setup_llm()
    context.log.info("LLM configured successfully")

    # Query for unprocessed patents and counts
    with ducklake.get_connection() as conn:
        total_patents = get_total_patent_count(
            conn, ducklake.ducklake_schema, SOURCE_TABLE
        )
        already_processed = get_processed_count(
            conn,
            ducklake.ducklake_schema,
            TABLE_NAME,
            model_info["model_name"],
            prompt_version,
        )

        unprocessed_df = get_unprocessed_patents(
            conn,
            ducklake.ducklake_schema,
            SOURCE_TABLE,
            TABLE_NAME,
            model_info["model_name"],
            prompt_version,
            limit=TEST_LIMIT,
        )

    unprocessed_count = len(unprocessed_df)

    context.log.info(
        f"Found {unprocessed_count} unprocessed patents out of {total_patents} total "
        f"({already_processed} already processed with this model+prompt)"
    )

    # Early return if all processed
    if unprocessed_count == 0:
        context.log.info(
            f"All {total_patents} patents already processed with {model_info['model_name']} {prompt_version}!"
        )
        context.add_output_metadata(
            metadata={
                "status": "all_processed",
                "total_patents": int(total_patents),
                "already_processed": int(already_processed),
                "newly_processed": 0,
                "model_name": model_info["model_name"],
                "model_provider": model_info["provider"],
                "prompt_version": prompt_version,
                "table_name": TABLE_NAME,
            }
        )
        return

    # Initialize extractor
    extractor = dspy.Predict(PatentProblemSolutionExtraction)

    # Processing state
    batch = []
    successful_count = 0
    failed_count = 0

    context.log.info(f"Starting extraction for {unprocessed_count} patents...")

    # Process patents
    for idx, row in unprocessed_df.iterrows():
        publication_number = row.get("publication_number")
        title = row.get("title_text", "")
        abstract = row.get("abstract_text", "")

        # Skip if both title and abstract are missing
        if (not title or pd.isna(title)) and (not abstract or pd.isna(abstract)):
            context.log.warning(
                f"Patent {publication_number}: Missing both title and abstract, skipping"
            )
            failed_count += 1
            continue

        try:
            # Truncate inputs to reasonable lengths
            title_text = str(title)[:200] if title and not pd.isna(title) else ""
            abstract_text = (
                str(abstract)[:1000] if abstract and not pd.isna(abstract) else ""
            )

            # Extract problem+solution with timing
            start_time = time.time()
            result = extractor(title=title_text, abstract=abstract_text)
            processing_time_ms = int((time.time() - start_time) * 1000)

            # Extract generation ID from LM history
            generation_id = extract_generation_id(lm)

            if not generation_id:
                context.log.warning(
                    f"Patent {publication_number}: No generation ID found in response, skipping"
                )
                failed_count += 1
                continue

            # Validate extraction results
            problem_text = result.problem.strip() if result.problem else ""
            solution_text = result.solution.strip() if result.solution else ""

            if len(problem_text) < 10 or len(solution_text) < 10:
                context.log.warning(
                    f"Patent {publication_number}: Extraction too short "
                    f"(problem: {len(problem_text)}, solution: {len(solution_text)}), skipping"
                )
                failed_count += 1
                continue

            # Add to batch
            batch.append(
                {
                    "publication_number": publication_number,
                    "title_text": title,
                    "abstract_text": abstract,
                    "model_name": model_info["model_name"],
                    "model_provider": model_info["provider"],
                    "prompt_version": prompt_version,
                    "generation_id": generation_id,
                    "problem": problem_text,
                    "solution": solution_text,
                    "processing_time_ms": processing_time_ms,
                }
            )
            successful_count += 1

            # Commit batch when full
            if len(batch) >= BATCH_SIZE:
                with ducklake.get_connection() as conn:
                    store_extraction_batch(
                        conn, ducklake.ducklake_schema, TABLE_NAME, batch
                    )
                progress_pct = round(
                    (successful_count + failed_count) / unprocessed_count * 100, 1
                )
                context.log.info(
                    f"Progress: {successful_count + failed_count}/{unprocessed_count} ({progress_pct}%) | "
                    f"Success: {successful_count} | Failed: {failed_count} | "
                    f"Committed batch of {len(batch)}"
                )
                batch = []

        except Exception as exc:
            context.log.warning(
                f"Patent {publication_number}: Extraction failed with error: {exc}"
            )
            failed_count += 1
            continue

    # Commit remaining batch (even if smaller than BATCH_SIZE)
    if batch:
        with ducklake.get_connection() as conn:
            store_extraction_batch(conn, ducklake.ducklake_schema, TABLE_NAME, batch)
        context.log.info(f"Committed final batch of {len(batch)} extractions")

    context.log.info(
        f"Extraction complete: {successful_count}/{unprocessed_count} successful, "
        f"{failed_count} failed"
    )

    # Query for sample results
    with ducklake.get_connection() as conn:
        sample_df = conn.execute(
            f"""
            SELECT publication_number, problem, solution, generation_id
            FROM {ducklake.ducklake_schema}.{TABLE_NAME}
            WHERE model_name = ? AND prompt_version = ?
            LIMIT 3
        """,
            [model_info["model_name"], prompt_version],
        ).fetchdf()

    # Format preview
    preview_rows = []
    for _, row in sample_df.iterrows():
        pub_num = str(row["publication_number"])
        gen_id = str(row["generation_id"])
        problem = str(row["problem"])
        solution = str(row["solution"])

        # Truncate for display
        gen_id_short = gen_id[:16] + "..." if len(gen_id) > 16 else gen_id
        problem_short = (problem[:100] + "...") if len(problem) > 100 else problem
        solution_short = (solution[:100] + "...") if len(solution) > 100 else solution

        preview_rows.append(
            f"**{pub_num}** (gen: `{gen_id_short}`)\n"
            f"- Problem: {problem_short}\n"
            f"- Solution: {solution_short}"
        )

    # Add output metadata
    context.add_output_metadata(
        metadata={
            "total_patents_in_source": int(total_patents),
            "already_processed": int(already_processed),
            "newly_processed": int(successful_count),
            "failed_count": int(failed_count),
            "success_rate_pct": float(
                round(successful_count / unprocessed_count * 100, 1)
                if unprocessed_count > 0
                else 0.0
            ),
            "model_name": model_info["model_name"],
            "model_provider": model_info["provider"],
            "prompt_version": prompt_version,
            "table_name": TABLE_NAME,
            "generation_ids_stored": True,
            "preview": MetadataValue.md(
                "### Sample Extractions\n\n" + "\n\n".join(preview_rows)
                if preview_rows
                else "No extractions yet"
            ),
        }
    )


def setup_embedder():
    """Configure Qwen3-Embedding-8B embedder for OpenRouter.

    Returns:
        DSPy Embedder configured for instruction-aware embeddings.
        Model: qwen/qwen3-embedding-8b (4096 dimensions)
        Cost: $0.01/M input tokens
    """
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


@asset(
    group_name="patent_extraction",
    description=(
        "Generates vector embeddings for patent problems and solutions using Qwen3-Embedding-8B. "
        "Stores 4096-dimensional embeddings as DOUBLE[] in patent_problems_solutions table. "
        "Uses batched API calls to process multiple patents efficiently. "
        "Resumable - skips patents already embedded."
    ),
    kinds={"python", "dspy", "ducklake"},
    metadata={
        "model": "qwen3-embedding-8b",
        "embedding_dimension": 4096,
        "storage_type": "DOUBLE[]",
        "instruction_aware": True,
        "table_name": "patent_problems_solutions",
        "api_batch_size": 10,
        "db_commit_batch_size": 10,
        "batched_processing": True,
        "resumable": True,
    },
    deps=["patent_problems_solutions"],
    code_version="1.2.0",
)
def patent_embeddings(
    context: AssetExecutionContext,
    ducklake: DuckLakeResource,
) -> None:
    """Generate vector embeddings for patent problems and solutions using batched API calls."""

    # Configuration
    MODEL_STRING = "qwen/qwen3-embedding-8b"  # via OpenRouter
    TABLE_NAME = "patent_problems_solutions"
    API_BATCH_SIZE = 10  # Number of patents to process in single API call
    DB_COMMIT_BATCH_SIZE = 10  # Commit to database every N patents for resumability
    EMBEDDING_DIM = 4096  # Qwen3-Embedding-8B outputs 4096 dimensions

    context.log.info(
        f"Using embedding model: {MODEL_STRING} ({EMBEDDING_DIM} dimensions)"
    )
    context.log.info(f"Processing {API_BATCH_SIZE} patents per API batch")

    # Ensure table has embedding columns
    ensure_extraction_table_exists(ducklake, TABLE_NAME)
    context.log.info(f"Ensured table {TABLE_NAME} has embedding columns")

    # Setup embedder
    try:
        embedder = setup_embedder()
        context.log.info("Embedder configured successfully")
    except Exception as exc:
        context.log.error(f"Failed to setup embedder: {exc}")
        raise

    # Query for patents without embeddings
    with ducklake.get_connection() as conn:
        stats = get_embeddings_stats(conn, ducklake.ducklake_schema, TABLE_NAME)

        patents_df = get_patents_without_embeddings(
            conn,
            ducklake.ducklake_schema,
            TABLE_NAME,
            limit=None,  # Process all patents without embeddings
        )

    patents_count = len(patents_df)

    context.log.info(
        f"Found {patents_count} patents without embeddings out of {stats['total']} total "
        f"({stats['with_embeddings']} already embedded)"
    )

    # Early return if all embedded
    if patents_count == 0:
        context.log.info(f"All {stats['total']} patents already have embeddings!")
        context.add_output_metadata(
            metadata={
                "status": "all_embedded",
                "total_patents": int(stats["total"]),
                "already_embedded": int(stats["with_embeddings"]),
                "newly_embedded": 0,
                "model": MODEL_STRING,
                "embedding_dimension": EMBEDDING_DIM,
                "table_name": TABLE_NAME,
            }
        )
        return

    # Processing state
    db_batch = []
    successful_count = 0
    failed_count = 0

    context.log.info(
        f"Starting batched embedding generation for {patents_count} patents..."
    )

    # Process patents in batches
    for batch_start in range(0, patents_count, API_BATCH_SIZE):
        batch_end = min(batch_start + API_BATCH_SIZE, patents_count)
        api_batch_df = patents_df.iloc[batch_start:batch_end]

        context.log.info(
            f"Processing API batch {batch_start // API_BATCH_SIZE + 1}: "
            f"patents {batch_start + 1}-{batch_end} of {patents_count}"
        )

        # Prepare batch: list of valid patent records with their formatted texts
        valid_patents = []

        for idx, row in api_batch_df.iterrows():
            publication_number = row.get("publication_number")
            problem = row.get("problem", "")
            solution = row.get("solution", "")

            # Skip if both are missing
            if (not problem or pd.isna(problem)) and (
                not solution or pd.isna(solution)
            ):
                context.log.warning(
                    f"Patent {publication_number}: Missing both problem and solution, skipping"
                )
                failed_count += 1
                continue

            # Format inputs with instructions
            problem_text = (
                str(problem)[:8000] if problem and not pd.isna(problem) else ""
            )
            solution_text = (
                str(solution)[:8000] if solution and not pd.isna(solution) else ""
            )

            problem_input = get_detailed_instruct(PROBLEM_TASK, problem_text)
            solution_input = get_detailed_instruct(SOLUTION_TASK, solution_text)

            # Store patent info with its formatted texts
            valid_patents.append(
                {
                    "publication_number": publication_number,
                    "problem_input": problem_input,
                    "solution_input": solution_input,
                }
            )

        # Skip if no valid patents in this batch
        if not valid_patents:
            continue

        # Build flat list of all texts for the embedder
        # Format: [patent1_problem, patent1_solution, patent2_problem, patent2_solution, ...]
        batch_texts = []
        for patent in valid_patents:
            batch_texts.append(patent["problem_input"])
            batch_texts.append(patent["solution_input"])

        # Generate embeddings for entire batch in one API call
        try:
            start_time = time.time()
            embeddings = embedder(batch_texts)
            processing_time_ms = int((time.time() - start_time) * 1000)

            context.log.info(
                f"Generated {len(embeddings)} embeddings in {processing_time_ms}ms "
                f"({processing_time_ms / len(embeddings):.1f}ms per embedding)"
            )

            # Verify we got the right number of embeddings
            expected_count = len(valid_patents) * 2
            if len(embeddings) != expected_count:
                context.log.error(
                    f"API batch failed: Expected {expected_count} embeddings, got {len(embeddings)}"
                )
                failed_count += len(valid_patents)
                continue

            # Extract generation ID
            generation_id = None
            if hasattr(dspy.settings, "lm") and dspy.settings.lm:
                generation_id = extract_generation_id(dspy.settings.lm)

            if not generation_id:
                generation_id = f"emb-{int(time.time())}-{uuid.uuid4().hex[:8]}"

            # Process embeddings: pair them back with patents
            # Since we know batch_texts has exactly 2 texts per patent in order,
            # embeddings[i*2] is patent i's problem, embeddings[i*2+1] is patent i's solution
            for i, patent in enumerate(valid_patents):
                try:
                    # Embeddings are in pairs: [problem, solution, problem, solution, ...]
                    problem_embedding = embeddings[i * 2].tolist()
                    solution_embedding = embeddings[i * 2 + 1].tolist()

                    # Verify dimensions
                    if (
                        len(problem_embedding) != EMBEDDING_DIM
                        or len(solution_embedding) != EMBEDDING_DIM
                    ):
                        context.log.warning(
                            f"Patent {patent['publication_number']}: Wrong embedding dimension "
                            f"(expected {EMBEDDING_DIM}, got {len(problem_embedding)}, {len(solution_embedding)}), skipping"
                        )
                        failed_count += 1
                        continue

                    # Add to database batch
                    db_batch.append(
                        {
                            "publication_number": patent["publication_number"],
                            "problem_embedding": problem_embedding,
                            "solution_embedding": solution_embedding,
                            "embedding_generation_id": generation_id,
                        }
                    )
                    successful_count += 1

                    context.log.info(
                        f"Patent {patent['publication_number']}: Embedded successfully"
                    )

                except Exception as exc:
                    context.log.warning(
                        f"Patent {patent['publication_number']}: Processing failed: {exc}"
                    )
                    failed_count += 1
                    continue

        except Exception as exc:
            context.log.error(f"API batch failed: {exc}")
            # Mark all patents in this API batch as failed
            failed_count += len(valid_patents)
            continue

        # Commit database batch when full
        if len(db_batch) >= DB_COMMIT_BATCH_SIZE:
            with ducklake.get_connection() as conn:
                store_embeddings_batch(
                    conn, ducklake.ducklake_schema, TABLE_NAME, db_batch
                )
            progress_pct = round(
                (successful_count + failed_count) / patents_count * 100, 1
            )
            context.log.info(
                f"Progress: {successful_count + failed_count}/{patents_count} ({progress_pct}%) | "
                f"Success: {successful_count} | Failed: {failed_count} | "
                f"Committed batch of {len(db_batch)}"
            )
            db_batch = []

    # Commit remaining database batch
    if db_batch:
        with ducklake.get_connection() as conn:
            store_embeddings_batch(conn, ducklake.ducklake_schema, TABLE_NAME, db_batch)
        context.log.info(f"Committed final batch of {len(db_batch)} embeddings")

    context.log.info(
        f"Embedding complete: {successful_count}/{patents_count} successful, "
        f"{failed_count} failed"
    )

    # Get updated stats
    with ducklake.get_connection() as conn:
        final_stats = get_embeddings_stats(conn, ducklake.ducklake_schema, TABLE_NAME)

        # Sample embeddings
        sample_df = conn.execute(
            f"""
            SELECT 
                publication_number,
                array_length(problem_embedding) as prob_dim,
                array_length(solution_embedding) as sol_dim,
                embedding_generation_id
            FROM {ducklake.ducklake_schema}.{TABLE_NAME}
            WHERE problem_embedding IS NOT NULL
            LIMIT 3
            """,
        ).fetchdf()

    # Format preview
    preview_rows = []
    for _, row in sample_df.iterrows():
        pub_num = str(row["publication_number"])

        # Handle prob_dim safely
        try:
            prob_dim = int(row["prob_dim"])
        except (ValueError, TypeError):
            prob_dim = 0

        # Handle sol_dim safely
        try:
            sol_dim = int(row["sol_dim"])
        except (ValueError, TypeError):
            sol_dim = 0

        gen_id = str(row["embedding_generation_id"])[:20] + "..."

        preview_rows.append(
            f"**{pub_num}**: Problem dim={prob_dim}, Solution dim={sol_dim}, Gen ID={gen_id}"
        )

    # Add output metadata
    context.add_output_metadata(
        metadata={
            "total_patents_in_table": int(final_stats["total"]),
            "already_embedded": int(final_stats["with_embeddings"]) - successful_count,
            "newly_embedded": int(successful_count),
            "failed_count": int(failed_count),
            "success_rate_pct": float(
                round(successful_count / patents_count * 100, 1)
                if patents_count > 0
                else 0.0
            ),
            "model": MODEL_STRING,
            "embedding_dimension": EMBEDDING_DIM,
            "instruction_aware": True,
            "table_name": TABLE_NAME,
            "api_batch_size": API_BATCH_SIZE,
            "problem_task_instruction": PROBLEM_TASK,
            "solution_task_instruction": SOLUTION_TASK,
            "generation_ids_stored": True,
            "preview": MetadataValue.md(
                "### Sample Embeddings\n\n" + "\n\n".join(preview_rows)
                if preview_rows
                else "No embeddings yet"
            ),
        }
    )
