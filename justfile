# Use PowerShell on Windows
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Load .env.local if present
set dotenv-load
set dotenv-filename := ".env.local"

# Default recipe - show available commands
default:
    @just --list

# Start local development (PostgreSQL + native Dagster)
dev:
    uv run dev up

# Start production-like environment (full Docker)
prod:
    uv run dev prod

# Stop services
down:
    uv run dev down

# Open interactive DuckDB session (use -l or --local-only to skip remote)
db *args:
    uv run dev db {{args}}

# Create a new pipeline
new name:
    uv run dev new {{name}}

# View logs
logs service="":
    uv run dev logs {{service}}

# Export table to file
export table format="parquet" source="local":
    uv run dev export {{table}} {{format}} {{source}}

# Pull table from remote DuckLake
pull table="" limit="":
    uv run dev pull {{table}} {{limit}}

# Clean local Dagster state
clean:
    rm -rf .dagster

# Generate production Docker Compose
generate:
    uv run python generate_platform.py

# Install sywi-core for local development
install-core:
    uv pip install -e sywi-core
