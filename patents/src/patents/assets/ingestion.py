"""Patent data ingestion from Google BigQuery and local CSV files."""

import os
import re
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from google.cloud import bigquery

from sywi_core import DuckLakeResource


@asset(
    group_name="patent_ingestion",
    description="Loads evaluation patent data from CSV and inserts into raw_patents_data table",
    kinds={"csv", "duckdb"},
    deps=["raw_patents_data"],
    code_version="1.0.0",
)
def evaluation_patents(
    context: AssetExecutionContext, ducklake: DuckLakeResource
) -> None:
    """Read evaluation patents from CSV and insert into raw_patents_data with is_evaluation flag."""
    csv_path = Path(__file__).parent.parent / "raw_data" / "patents_loaded.csv"

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    context.log.info(f"Inserting evaluation patents from {csv_path}...")

    with ducklake.get_connection() as conn:
        schema = ducklake.ducklake_schema
        table = f"{schema}.raw_patents_data"

        # Add is_evaluation column if it doesn't exist
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS is_evaluation BOOLEAN"
        )

        # Insert CSV data with is_evaluation = TRUE
        # Map CSV columns to raw_patents_data schema (CSV has extra Cluster, GooglePatentsID columns)
        # Omit assignee column due to data type mismatch
        conn.execute(f"""
            INSERT INTO {table} (publication_number, application_number, publication_date, grant_date, title, abstract, claims_text, is_evaluation)
            SELECT 
                publication_number,
                application_number,
                publication_date,
                grant_date,
                title,
                abstract,
                claims_text,
                TRUE as is_evaluation
            FROM read_csv_auto('{csv_path}')
        """)

        # Get count of inserted rows
        count = conn.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE is_evaluation = TRUE
        """).fetchone()[0]

    context.log.info(f"Inserted {count} evaluation patents")

    context.add_output_metadata(
        metadata={
            "num_records": int(count),
            "file_path": csv_path,
            "target_table": table,
        }
    )


@asset(
    group_name="patent_ingestion",
    description="Fetches 10,000 newest US patents with titles, abstracts, and first English claim from Google BigQuery",
    kinds={"bigquery", "python"},
    metadata={
        "source": "Google Patents Public Data",
        "dataset": "patents-public-data.patents.publications",
        "includes": "title, abstract, first English claim",
    },
    code_version="1.3.0",
)
def raw_patents_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Query BigQuery for the newest 10,000 US patents."""
    # Check for service account credentials
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable not set. "
            "Please set it to the path of your service account JSON file."
        )

    context.log.info("Authenticating with Google Cloud BigQuery...")
    client = bigquery.Client.from_service_account_json(credentials_path)

    query = """
        SELECT
            p.publication_number,
            p.application_number,
            p.publication_date,
            p.grant_date,
            p.title_localized[SAFE_OFFSET(0)].text AS title,
            p.abstract_localized[SAFE_OFFSET(0)].text AS abstract,
            p.assignee,
            (SELECT text 
             FROM UNNEST(p.claims_localized) 
             WHERE language = 'en' 
             LIMIT 1) AS claims_text
        FROM
            `patents-public-data.patents.publications` AS p
        WHERE
            p.country_code = 'US'
        ORDER BY
            p.publication_date DESC
        LIMIT 10000
    """

    context.log.info("Executing BigQuery job...")
    query_job = client.query(query)
    df = query_job.to_dataframe()

    context.log.info(f"Loaded {len(df)} patents from BigQuery")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "columns": MetadataValue.json(df.columns.tolist()),
            "date_range": MetadataValue.md(
                f"**Newest:** {df['publication_date'].max()}\n\n"
                f"**Oldest:** {df['publication_date'].min()}"
            ),
        }
    )

    return df


@asset(
    group_name="patent_ingestion",
    description="Cleans patent data including removing excessive newlines from text fields",
    kinds={"python", "pandas"},
    deps=["evaluation_patents"],
    code_version="1.6.0",
)
def cleaned_patents_data(
    context: AssetExecutionContext, ducklake: DuckLakeResource
) -> pd.DataFrame:
    """Rename columns, clean text fields, and add basic statistics."""
    # Read from raw_patents_data table (includes both BigQuery and evaluation data)
    with ducklake.get_connection() as conn:
        schema = ducklake.ducklake_schema
        df = conn.execute(f"SELECT * FROM {schema}.raw_patents_data").fetchdf()

    def clean_text(text):
        """Remove excessive newlines, keeping only single newlines."""
        if text is None or pd.isna(text):
            return text
        # Replace multiple newlines (2 or more) with a single newline
        cleaned = re.sub(r"\n\s*\n+", "\n", text)
        # Also clean up any remaining whitespace around newlines
        cleaned = re.sub(r"\n\s+", "\n", cleaned)
        cleaned = re.sub(r"\s+\n", "\n", cleaned)
        return cleaned.strip()

    # Rename columns for consistency (title and abstract already extracted by BigQuery)
    if "title" in df.columns:
        df = df.rename(columns={"title": "title_text"})
        context.log.info("Renamed title column")

    if "abstract" in df.columns:
        df = df.rename(columns={"abstract": "abstract_text"})
        # Clean abstract text
        df["abstract_text"] = df["abstract_text"].apply(clean_text)
        context.log.info("Renamed and cleaned abstract column")

    # Clean claims_text (already extracted by BigQuery)
    has_claims = 0
    if "claims_text" in df.columns:
        df["claims_text"] = df["claims_text"].apply(clean_text)
        has_claims = df["claims_text"].notna().sum()
        context.log.info(f"Cleaned claims for {has_claims} of {len(df)} patents")

    # Count null values and convert to Python native types
    null_counts = df.isnull().sum()
    non_zero_nulls = null_counts[null_counts > 0]
    # Convert numpy types to Python native types for serialization
    null_values_dict = {str(k): int(v) for k, v in non_zero_nulls.to_dict().items()}

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "num_with_claims": int(has_claims),
            "claims_coverage_pct": float(round(has_claims / len(df) * 100, 1))
            if len(df) > 0
            else 0.0,
            "columns": MetadataValue.json(df.columns.tolist()),
            "null_values": MetadataValue.json(null_values_dict),
            "preview": MetadataValue.md(
                "### Sample Patents\n\n"
                + "\n".join(
                    [
                        f"- **{row['publication_number']}**: {row.get('title_text', 'N/A')}"
                        + (
                            f" ({len(row.get('claims_text', '') or '')} chars of claims)"
                            if row.get("claims_text")
                            else " (no claims)"
                        )
                        for _, row in df.head(3).iterrows()
                    ]
                )
            ),
        }
    )

    return df
