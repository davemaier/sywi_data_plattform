"""HackerNews data ingestion from API."""

import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(
    group_name="hackernews",
    description="Fetches the top 5 story IDs from the HackerNews API",
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
def hackernews_top_stories(context: AssetExecutionContext) -> pd.DataFrame:
    """Fetch the top 5 story IDs from HackerNews."""
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
    context: AssetExecutionContext, hackernews_top_stories: pd.DataFrame
) -> pd.DataFrame:
    """Fetch titles and authors for the top stories."""
    # Convert DataFrame to list of story IDs
    story_ids = hackernews_top_stories["story_id"].tolist()

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
