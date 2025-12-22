"""Example asset - replace with your own."""

from dagster import asset


@asset(group_name="my_pipeline")
def hello_world():
    """A simple hello world asset."""
    return "Hello from this pipeline!"
