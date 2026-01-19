"""Example asset for __NAME__ project."""

import pandas as pd
from dagster import asset


@asset(group_name="__name__")
def example_asset() -> pd.DataFrame:
    """An example asset that returns a simple DataFrame.

    Replace this with your actual data processing logic.
    """
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }
    )
