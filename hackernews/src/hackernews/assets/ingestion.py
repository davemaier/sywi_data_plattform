"""HackerNews data ingestion from API."""

import os
from pathlib import Path

import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset

# Lock file path - inside assets folder so it's visible in Docker mount
LOCK_FILE = Path(__file__).parent / ".ingestion.lock"


def is_ingestion_locked() -> bool:
    """Check if ingestion is locked by presence of lock file."""
    return LOCK_FILE.exists()


@asset(
    group_name="hackernews",
    description="Fetches the top 5 story IDs from the HackerNews API. Create .hackernews_ingestion.lock file to disable.",
    metadata={
        "source": "HackerNews API",
        "api_endpoint": "https://hacker-news.firebaseio.com/v0/topstories.json",
    },
    kinds={"python", "rest_api"},
    tags={
        "source": "hackernews",
    },
    code_version="2.0.0",
)
def hackernews_top_stories_ids(context: AssetExecutionContext) -> pd.DataFrame:
    """Fetch the top 5 story IDs from HackerNews."""
    if is_ingestion_locked():
        raise RuntimeError(f"Asset is locked. Remove {LOCK_FILE} to enable execution.")

    top_story_ids = requests.get(
        "https://hacker-news.firebaseio.com/v0/topstories.json"
    ).json()[:5]

    context.log.info(f"Fetched {len(top_story_ids)} story IDs: {top_story_ids}")

    # Convert to DataFrame for DuckLake storage
    df = pd.DataFrame({"story_id": top_story_ids})

    context.add_output_metadata(
        metadata={
            "num_stories": len(df),
            "story_ids": MetadataValue.json(top_story_ids),
            "preview": MetadataValue.md(
                f"Story IDs: {', '.join(map(str, top_story_ids))}"
            ),
        }
    )

    return df


@asset(
    group_name="hackernews",
    description="Enriches story IDs with titles and author information from HackerNews API",
    metadata={
        "source": "HackerNews API",
    },
    kinds={"python", "rest_api"},
    tags={
        "source": "hackernews",
    },
    code_version="2.0.0",
)
def hackernews_titles(
    context: AssetExecutionContext, hackernews_top_stories_ids: pd.DataFrame
) -> pd.DataFrame:
    """Fetch titles and authors for the top stories."""
    # Convert DataFrame to list of story IDs
    story_ids = hackernews_top_stories_ids["story_id"].tolist()

    results = []
    for story_id in story_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
        ).json()
        results.append([story_id, item.get("title"), item.get("by")])

    context.log.info(f"Fetched details for {len(results)} stories")

    # Convert to DataFrame
    df = pd.DataFrame(results, columns=["id", "title", "author"])

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(
                "\n".join(
                    [
                        f"- **{row['title']}** by {row['author']}"
                        for _, row in df.head(3).iterrows()
                    ]
                )
            ),
        }
    )

    return df
