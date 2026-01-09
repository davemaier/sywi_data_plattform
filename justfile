# Development CLI for Dagster pipelines
# Usage: just <command>
#
# Windows users: install just with `winget install Casey.Just`
# macOS users: install just with `brew install just`
# Linux users: install just with `cargo install just` or your package manager

# Default recipe - show help
default:
    uv run python dev.py --help

# Start Dagster dev environment
up:
    uv run python dev.py up

# Stop all containers
down:
    uv run python dev.py down

# Create a new pipeline from template
new name:
    uv run python dev.py new {{name}}

# Rebuild base Docker image
build:
    uv run python dev.py build

# View logs (optionally for specific service)
logs service="":
    uv run python dev.py logs {{service}}

# Open interactive DuckDB session (local + remote)
db:
    uv run python dev.py db

# Open interactive DuckDB session (remote only)
db-remote:
    uv run python dev.py db-remote

# Export table to file
export table format="parquet" source="local":
    uv run python dev.py export {{table}} {{format}} {{source}}

# List remote tables or pull table from remote
pull table="" limit="":
    uv run python dev.py pull {{table}} {{limit}}

# Mark assets as materialized
mark assets:
    uv run python dev.py mark {{assets}}
