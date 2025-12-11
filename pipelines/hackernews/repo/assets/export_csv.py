"""Export HackerNews data to CSV."""

from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(
    group_name="hackernews",
    description="Exports HackerNews stories to a CSV file for analysis",
    metadata={
        "output_format": "CSV",
    },
    kinds={"python", "csv"},
    tags={
        "storage": "local",
    },
    code_version="2.0.0",
)
def save_to_csv(context: AssetExecutionContext, hackernews_titles: pd.DataFrame) -> str:
    """Export HackerNews titles to CSV file."""
    output_path = Path("hackernews.csv")

    # Save DataFrame to CSV
    hackernews_titles.to_csv(output_path, index=False)

    context.log.info(f"Wrote {len(hackernews_titles)} records to {output_path}")

    context.add_output_metadata(
        metadata={
            "num_records": len(hackernews_titles),
            "file_path": MetadataValue.path(str(output_path.absolute())),
            "file_size_bytes": output_path.stat().st_size,
            "preview": MetadataValue.md(
                f"```csv\n{hackernews_titles.head(3).to_csv(index=False)}```"
            ),
        }
    )

    return f"CSV file created at {output_path}"
