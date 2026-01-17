"""HackerNews story analytics and insights."""

import re
from collections import Counter

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

# Tech keywords to detect in titles
TECH_KEYWORDS = {
    "ai",
    "ml",
    "llm",
    "gpt",
    "openai",
    "anthropic",
    "claude",
    "chatgpt",
    "rust",
    "python",
    "javascript",
    "typescript",
    "golang",
    "java",
    "kubernetes",
    "docker",
    "aws",
    "cloud",
    "serverless",
    "blockchain",
    "crypto",
    "bitcoin",
    "ethereum",
    "startup",
    "vc",
    "funding",
    "yc",
    "ycombinator",
    "open source",
    "opensource",
    "github",
    "linux",
    "security",
    "hack",
    "vulnerability",
    "breach",
    "apple",
    "google",
    "microsoft",
    "meta",
    "amazon",
}


def extract_tech_tags(title: str) -> list[str]:
    """Extract tech-related tags from a title."""
    title_lower = title.lower()
    found_tags = []
    for keyword in TECH_KEYWORDS:
        if keyword in title_lower:
            found_tags.append(keyword)
    return found_tags


def compute_clickbait_score(title: str) -> float:
    """
    Compute a 'clickbait score' based on common patterns.
    Higher score = more clickbait-y (0.0 to 1.0)
    """
    score = 0.0
    title_lower = title.lower()

    # Check for question marks
    if "?" in title:
        score += 0.15

    # Check for numbers (listicles, stats)
    if re.search(r"\d+", title):
        score += 0.1

    # Clickbait phrases
    clickbait_phrases = [
        "you won't believe",
        "shocking",
        "amazing",
        "incredible",
        "why you should",
        "here's why",
        "the truth about",
        "everything you need",
        "secret",
        "revealed",
    ]
    for phrase in clickbait_phrases:
        if phrase in title_lower:
            score += 0.2

    # ALL CAPS words (excluding common acronyms)
    caps_words = re.findall(r"\b[A-Z]{4,}\b", title)
    if caps_words:
        score += 0.1 * min(len(caps_words), 3)

    return min(score, 1.0)


@asset(
    group_name="hackernews",
    description="Analyzes HackerNews stories: extracts tech tags, computes word counts, and generates engagement scores",
    metadata={
        "analysis_type": "NLP & Statistics",
    },
    kinds={"python", "analytics"},
    tags={
        "type": "analytics",
    },
    code_version="1.0.0",
)
def story_analytics(
    context: AssetExecutionContext,
    hackernews_titles: pd.DataFrame,
) -> pd.DataFrame:
    """Compute analytics and insights for HackerNews stories."""
    df = hackernews_titles.copy()

    # Word count
    df["word_count"] = df["title"].apply(lambda t: len(t.split()) if pd.notna(t) else 0)

    # Character count
    df["char_count"] = df["title"].apply(lambda t: len(t) if pd.notna(t) else 0)

    # Extract tech tags
    df["tech_tags"] = df["title"].apply(
        lambda t: extract_tech_tags(t) if pd.notna(t) else []
    )
    df["tech_tag_count"] = df["tech_tags"].apply(len)

    # Clickbait score
    df["clickbait_score"] = df["title"].apply(
        lambda t: compute_clickbait_score(t) if pd.notna(t) else 0.0
    )

    # Convert tech_tags list to comma-separated string for storage
    df["tech_tags_str"] = df["tech_tags"].apply(lambda tags: ", ".join(tags))

    # Compute aggregate stats (convert to native Python types for serialization)
    avg_word_count = float(df["word_count"].mean())
    avg_clickbait = float(df["clickbait_score"].mean())
    all_tags = [tag for tags in df["tech_tags"] for tag in tags]
    top_tags = Counter(all_tags).most_common(5)

    context.log.info(f"Analyzed {len(df)} stories")
    context.log.info(f"Average word count: {avg_word_count:.1f}")
    context.log.info(f"Average clickbait score: {avg_clickbait:.2f}")

    # Build rich metadata
    tag_summary = (
        ", ".join([f"{tag} ({count})" for tag, count in top_tags])
        if top_tags
        else "None detected"
    )

    context.add_output_metadata(
        metadata={
            "num_stories_analyzed": len(df),
            "avg_word_count": round(avg_word_count, 1),
            "avg_clickbait_score": round(avg_clickbait, 3),
            "top_tech_tags": MetadataValue.json(dict(top_tags) if top_tags else {}),
            "preview": MetadataValue.md(
                f"**Story Analytics Summary**\n\n"
                f"- Stories analyzed: {len(df)}\n"
                f"- Avg word count: {avg_word_count:.1f}\n"
                f"- Avg clickbait score: {avg_clickbait:.2f}\n"
                f"- Top tech tags: {tag_summary}\n\n"
                f"**Sample Analysis:**\n\n"
                + "\n".join(
                    [
                        f"- **{row['title'][:50]}{'...' if len(row['title']) > 50 else ''}**\n"
                        f"  - Words: {int(row['word_count'])}, Clickbait: {float(row['clickbait_score']):.2f}, Tags: {row['tech_tags_str'] or 'none'}"
                        for _, row in df.head(3).iterrows()
                    ]
                )
            ),
        }
    )

    # Return DataFrame with analytics columns (drop the list column for DuckLake compatibility)
    return df.drop(columns=["tech_tags"])
