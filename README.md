# SYWI Data Platform

A data platform built with [Dagster](https://dagster.io/) and [DuckLake](https://ducklake.select/).

## Prerequisites

### Required

| Tool | macOS | Windows | Linux |
|------|-------|---------|-------|
| [uv](https://github.com/astral-sh/uv) | `brew install uv` | `winget install --id=astral-sh.uv -e` | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [just](https://github.com/casey/just) | `brew install just` | `winget install --id=Casey.Just --exact` | `brew install just` or [see docs](https://just.systems/man/en/packages.html) |
| [Docker](https://docs.docker.com/get-docker/) | Docker Desktop | Docker Desktop | `apt install docker.io docker-compose` |

### Optional

| Tool | macOS | Windows | Linux |
|------|-------|---------|-------|
| [DuckDB CLI](https://duckdb.org/docs/installation/) | `brew install duckdb` | `winget install DuckDB.cli` | [See docs](https://duckdb.org/docs/installation/) |

DuckDB CLI is only needed for the `just db` command (interactive SQL session).

## Quick Start

```bash
# 1. Clone and enter the project
cd sywi_data_plattform

# 2. Create your local environment file
cp .env.example .env.local
# Or place the .env.local you received via email in the root folder

# 3. Start the development environment
just dev

# 4. Open http://localhost:3000 in your browser
```

## Concepts

This platform uses Dagster for orchestration. Here's how our terminology maps to Dagster:

| Term | Dagster Concept | Description |
|------|-----------------|-------------|
| **Project** | Code Location | A folder with `pyproject.toml` and its own virtual environment. Each project is deployed as a separate Dagster code location with isolated dependencies. |
| **Asset** | Asset | A logical unit of data (e.g., a table, dataset, or file). Defined with the `@asset` decorator. |
| **Asset Group** | Asset Group | A way to organize assets visually in the Dagster UI using the `group_name` parameter. |

A single project can contain multiple asset groups. For example, the `patents` project has groups like `patent_ingestion`, `patent_extraction`, and `patent_graph`.

## Local Development

### What `just dev` Does

1. **Syncs all projects** - Creates/updates virtual environments for each project
2. **Starts PostgreSQL** - Runs in Docker (DuckLake catalog)
3. **Runs Dagster** - Starts the dev server via `dg dev`

Each project runs in its own isolated Python environment, allowing different dependency versions without conflicts.

### Hot Reload

The dev environment automatically detects code changes:

1. Edit files in any project's `src/` directory
2. Dagster detects the change automatically
3. Click **"Reload"** in the Dagster UI to apply changes

### Configuration

The `.env.local` file configures your local environment:

```bash
# Local DuckLake - PostgreSQL catalog + local file storage
DUCKLAKE_CATALOG_DSN="dbname=ducklake_catalog host=localhost user=dagster_user password=dagster_password port=5432"
DUCKLAKE_DATA_PATH=./data/
DUCKLAKE_SCHEMA=local
```

DuckLake stores:

- **Catalog metadata** in PostgreSQL (shared with Dagster)
- **Table data** as Parquet files in `./data/`

### Working with Data

#### Interactive DuckDB Session

```bash
just db
```

Opens a DuckDB shell with local and remote DuckLake attached:

```sql
-- Show local tables
SHOW TABLES FROM local;

-- Query local data
SELECT * FROM local.hackernews_stories LIMIT 10;

-- Query remote data (if configured)
SELECT * FROM remote.hackernews_stories LIMIT 10;
```

Use `just db -l` to skip connecting to remote.

#### Pull Data from Remote

If you have access to a remote DuckLake, configure it in `.env.local`:

```bash
DUCKLAKE_REMOTE_CATALOG_DSN="dbname=... host=... user=... password=... port=5432"
DUCKLAKE_REMOTE_DATA_PATH=s3://bucket/path/
DUCKLAKE_REMOTE_S3_REGION=us-east-1
DUCKLAKE_REMOTE_S3_ENDPOINT=...
DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID=...
DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY=...
```

Then pull tables:

```bash
# List available remote tables
just pull

# Pull a complete table
just pull hackernews_stories

# Pull only 1000 rows (faster iteration)
just pull hackernews_stories 1000
```

Pulled tables are automatically marked as materialized in Dagster.

#### Export Data

```bash
# Export as Parquet (default)
just export customers

# Export as CSV
just export customers csv

# Export remote table
just export customers parquet remote
```

Supported formats: `parquet`, `csv`, `json`, `ndjson`

## Adding a New Project

```bash
just new my_project
```

This creates a new project (Dagster code location) and registers it in `dg.toml`:

```
my_project/
├── pyproject.toml
├── Dockerfile
└── src/my_project/
    ├── __init__.py
    ├── definitions.py
    └── assets/
        └── __init__.py
```

Then:

1. Add assets in `my_project/src/my_project/assets/`
2. Export them in `assets/__init__.py`
3. Register them in `definitions.py`
4. Add dependencies to `pyproject.toml`
5. Run `just dev` - the new project loads automatically

### Example Asset

```python
# my_project/src/my_project/assets/example.py
import pandas as pd
from dagster import asset

@asset(group_name="my_data_ingestion")
def my_asset() -> pd.DataFrame:
    return pd.DataFrame({"col": [1, 2, 3]})
```

## Cross-Project Dependencies

Projects can depend on assets from other projects using DuckLake as the data contract:

```python
from dagster import SourceAsset, asset

# Declare external dependency
hackernews_stories = SourceAsset(
    key="hackernews_stories",
    description="Stories from hackernews project",
)

@asset(deps=[hackernews_stories])
def my_analysis(ducklake: DuckLakeResource):
    with ducklake.get_connection() as conn:
        df = conn.execute("SELECT * FROM local.hackernews_stories").fetchdf()
    # ... your analysis
    return result
```

## Client Libraries

For accessing SYWI DuckLake from external scripts or notebooks:

### Python: sywi-duckdb

```bash
pip install git+https://github.com/sywi/sywi_data_plattform.git#subdirectory=libs/sywi_duckdb
```

```python
import sywi_duckdb as duckdb

conn = duckdb.connect()
conn.execute("SELECT * FROM local.my_table").fetchdf()
```

### R: sywi.duckdb

```r
remotes::install_github("sywi/sywi_data_plattform", subdir = "libs/sywi.duckdb")
```

```r
library(sywi.duckdb)
con <- dbConnect(sywi_duckdb())
dbGetQuery(con, "SELECT * FROM local.my_table")
```

## Command Reference

| Command | Description |
|---------|-------------|
| `just dev` | Start development environment |
| `just down` | Stop PostgreSQL |
| `just new <name>` | Create a new project (Dagster code location) |
| `just db` | Open interactive DuckDB session |
| `just db -l` | DuckDB session (local only, skip remote) |
| `just pull` | List remote tables |
| `just pull <table>` | Pull table from remote |
| `just pull <table> <limit>` | Pull limited rows |
| `just export <table> [format] [source]` | Export table to file |
| `just logs [service]` | View Docker logs |
| `just prod` | Start production-like environment |
| `just generate` | Generate production Docker Compose |
| `just clean` | Remove local Dagster state |

## Troubleshooting

### `just dev` fails with "No such file or directory: .venv/bin/python"

The subproject virtual environments need to be created. This should happen automatically, but you can manually sync:

```bash
cd hackernews && uv sync && cd ..
cd patents && uv sync && cd ..
```

### `just db` fails to connect

The dev environment must be running (PostgreSQL needs to be up):

```bash
just dev  # Start first, then in another terminal:
just db
```

### Docker permission errors on Linux

Add your user to the docker group:

```bash
sudo usermod -aG docker $USER
# Then log out and back in
```

---

## Production Deployment

Production runs on Docker with each project as a separate container communicating via gRPC.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Server                               │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  PostgreSQL │  │    MinIO    │  │   Dagster Webserver │ │
│  │  (Dagster + │  │  (S3 data)  │  │     (port 3000)     │ │
│  │  DuckLake)  │  │             │  │                     │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│         │                │                    │             │
│  ┌──────┴────────────────┴────────────────────┴──────────┐ │
│  │              Pipeline Containers (gRPC)               │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │ │
│  │  │  hackernews  │  │   patents    │  │    ...     │  │ │
│  │  │  (port 4000) │  │  (port 4000) │  │            │  │ │
│  │  └──────────────┘  └──────────────┘  └────────────┘  │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Configuration

Production `.env` file:

```bash
DUCKLAKE_CATALOG_DSN="dbname=ducklake_catalog host=postgresql user=... password=... port=5432"
DUCKLAKE_DATA_PATH=s3://ducklake/data/
DUCKLAKE_SCHEMA=sywi
DUCKLAKE_S3_REGION=us-east-1
DUCKLAKE_S3_ENDPOINT=minio:9000
DUCKLAKE_S3_ACCESS_KEY_ID=...
DUCKLAKE_S3_SECRET_ACCESS_KEY=...
DUCKLAKE_S3_URL_STYLE=path
DUCKLAKE_S3_USE_SSL=false
```

### Deployment Scripts

#### Generate Configuration

```bash
just generate
```

Scans for projects and generates:

- `docker-compose.prod.yaml` - All services
- `workspace.yaml` - Dagster workspace config

#### Manual Deploy

```bash
./deploy.sh
```

Pulls latest code, builds images, and restarts containers.

#### Auto-Deploy (Watchdog)

```bash
./watchdog.sh
```

Polls GitHub every 60 seconds and auto-deploys on changes.

### Docker Images

Each project has its own `Dockerfile` that:

1. Installs `sywi-core` as a dependency
2. Installs the project package
3. Runs the Dagster gRPC server on port 4000

The `Dockerfile.dagster` image runs the webserver and daemon.
