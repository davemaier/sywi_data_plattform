# SYWI Data Platform

A data pipeline platform built with [Dagster](https://dagster.io/) and [DuckLake](https://ducklake.select/).

## Architecture

The platform uses DuckLake as its data lakehouse, which provides:
- Version-controlled tables with time travel
- Parquet-based storage
- ACID transactions

**Two modes:**

| Mode | Catalog | Storage | Use Case |
|------|---------|---------|----------|
| Local | DuckDB file | Local files | Development |
| Production | PostgreSQL | S3/MinIO | Server deployment |

The same pipeline code works in both modes - only the configuration differs.

**Pipeline Isolation:**

Each pipeline runs in its own Docker container with isolated dependencies. This allows different pipelines to use different versions of libraries without conflicts.

## Local Development

Local development uses Docker Compose to run isolated pipeline containers, matching production architecture.

### Quick Start

```bash
# 1. Clone and enter the project
cd sywi_data_plattform

# 2. Create your local environment file
cp .env.example .env.local

# 3. Start Dagster (Docker Compose)
./dev up
```

Open http://localhost:3000 in your browser.

### Configuration

The `.env.local` file for local development:

```bash
# Local mode - file-based DuckLake (no infrastructure needed)
DUCKLAKE_CATALOG_DSN=./data/ducklake.ducklake
DUCKLAKE_DATA_PATH=./data/
DUCKLAKE_SCHEMA=sywi
```

That's it! DuckLake will store:
- Catalog metadata in `./data/ducklake.ducklake` (DuckDB file)
- Table data as Parquet files in `./data/`

### Development Workflow

#### 1. Start Dagster

```bash
./dev up
```

This will:
- Build the base Docker image (if needed)
- Generate workspace configuration
- Start PostgreSQL, Dagster daemon, webserver, and all pipeline containers

#### 2. Hot Reload

The dev environment uses `watchfiles` to automatically restart the Dagster gRPC server when code changes. After editing files in `pipelines/<name>/assets/`, `_shared/`, or `defs.py`:

1. The pipeline container detects the change and restarts the gRPC server automatically
2. Click **"Reload"** in the Dagster UI to reload the code location

This enables fast iteration without manually restarting containers.

#### 3. Pull Data from Production (Optional)

If you need production data for development, configure remote access in `.env.local`:

```bash
# Remote DuckLake (production)
DUCKLAKE_REMOTE_CATALOG_DSN="dbname=ducklake_catalog host=prod-server.com user=... password=... port=5432"
DUCKLAKE_REMOTE_DATA_PATH=s3://ducklake/data/
DUCKLAKE_REMOTE_S3_REGION=us-east-1
DUCKLAKE_REMOTE_S3_ENDPOINT=prod-minio.com:9000
DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID=...
DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY=...
```

Then pull tables:

```bash
# List available tables on remote
./dev pull

# Pull a complete table
./dev pull hackernews_stories

# Pull only 1000 rows (for faster iteration)
./dev pull hackernews_stories 1000
```

When you pull a table, the matching Dagster asset is automatically marked as materialized.

#### 4. Run Assets

Use the Dagster UI at http://localhost:3000 to materialize assets.

#### 5. Mark Assets as Materialized (Without Running)

If you've manually loaded data or want to skip upstream assets:

```bash
./dev mark hackernews_top_stories
./dev mark asset1,asset2,asset3
```

### Inspecting Data

Open an interactive DuckDB session with local and remote DuckLake attached:

```bash
# Local DuckLake (with remote also attached as read-only)
./dev db

# Remote DuckLake only
./dev db-remote
```

Inside the session:

```sql
-- Show local tables
SHOW TABLES FROM local;

-- Show remote tables
SHOW TABLES FROM remote;

-- Query local data
SELECT * FROM local.hackernews_stories LIMIT 10;

-- Query remote data
SELECT * FROM remote.hackernews_stories LIMIT 10;

-- Copy a table from remote to local
CREATE TABLE local.my_table AS SELECT * FROM remote.my_table;
```

### Exporting Data

Export tables to local files:

```bash
# Export local table as parquet (default)
./dev export customers

# Export as CSV
./dev export customers csv

# Export remote table as parquet
./dev export customers parquet remote

# Export remote table as JSON
./dev export customers json remote
```

Supported formats: `parquet` (default), `csv`, `json`, `ndjson`

## Production Deployment

Production runs on a server using Docker. Each pipeline runs as a separate container, communicating with the Dagster webserver via gRPC.

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
│         └────────────────┼────────────────────┘             │
│                          │                                  │
│  ┌───────────────────────┼───────────────────────────────┐ │
│  │              Pipeline Containers (gRPC)               │ │
│  │                                                       │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │ │
│  │  │  hackernews  │  │  pipeline_2  │  │    ...     │  │ │
│  │  │  (port 4000) │  │  (port 4000) │  │            │  │ │
│  │  └──────────────┘  └──────────────┘  └────────────┘  │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Configuration

Production uses a `.env` file with PostgreSQL + S3 configuration:

```bash
# Production mode - PostgreSQL catalog + S3 storage
DUCKLAKE_CATALOG_DSN="dbname=ducklake_catalog host=postgresql user=dagster_user password=dagster_password port=5432"
DUCKLAKE_DATA_PATH=s3://ducklake/data/
DUCKLAKE_SCHEMA=sywi
DUCKLAKE_S3_REGION=us-east-1
DUCKLAKE_S3_ENDPOINT=minio:9000
DUCKLAKE_S3_ACCESS_KEY_ID=minioadmin
DUCKLAKE_S3_SECRET_ACCESS_KEY=minioadmin
DUCKLAKE_S3_URL_STYLE=path
DUCKLAKE_S3_USE_SSL=false
```

### Deployment Scripts

#### `deploy.sh` - Manual Deployment

Deploys the platform by:
1. Pulling latest code from `main` branch
2. Building the base Docker image (`Dockerfile.base`)
3. Running `generate_platform.py` to discover pipelines and generate Docker Compose config
4. Starting/updating all containers
5. Reloading the Dagster webserver

```bash
./deploy.sh
```

#### `watchdog.sh` - Automatic Deployment

Polls the GitHub `main` branch every 60 seconds and automatically triggers `deploy.sh` when changes are detected.

```bash
# Start the watchdog (runs in foreground)
./watchdog.sh

# Or run in background with nohup
nohup ./watchdog.sh > /var/log/watchdog.log 2>&1 &
```

This enables a simple CI/CD workflow:
1. Push changes to `main` branch
2. Watchdog detects changes within 60 seconds
3. Platform automatically redeploys

#### `generate_platform.py` - Config Generator

Scans the `pipelines/` directory and generates:
- `docker-compose.override.generated.yaml` - Container definitions for each pipeline
- `workspace.generated.yaml` - Dagster workspace config with gRPC endpoints

Each pipeline in `pipelines/<name>/` (excluding `_shared` and `_template`) gets its own container.

### Docker Images

#### `Dockerfile.base`

Base image used by all services. Contains:
- Python 3.11
- R runtime
- uv package manager
- Dagster core packages
- watchfiles (for dev hot reload)

#### Pipeline Dockerfiles

Each pipeline has its own `Dockerfile` (e.g., `pipelines/hackernews/Dockerfile`) that:
1. Extends `my-platform-base:latest`
2. Copies shared code from `_shared/`
3. Installs pipeline-specific dependencies via `uv sync`
4. Runs the Dagster gRPC server on port 4000

### Core Services

Defined in `docker-compose.core.yaml`:

| Service | Description |
|---------|-------------|
| `postgresql` | Stores Dagster run history and DuckLake catalog |
| `dagster_daemon` | Runs schedules, sensors, and background jobs |
| `dagster_webserver` | Web UI on port 3000 |

## Project Structure

```
sywi_data_plattform/
├── dev                       # Development helper script
├── deploy.sh                 # Production deployment script
├── watchdog.sh               # Auto-deploy on git changes
├── generate_platform.py      # Generates Docker Compose config
├── pyproject.toml            # Minimal deps for utility commands
├── dagster.yaml              # Dagster instance config (production)
├── Dockerfile.base           # Base Docker image
├── docker-compose.core.yaml  # Core services (Postgres, Dagster)
├── data/                     # Local DuckLake data (gitignored)
├── pipelines/
│   ├── _shared/              # Shared resources (copied into all containers)
│   │   ├── __init__.py
│   │   ├── ducklake_io_manager.py
│   │   └── ducklake_resource.py
│   ├── _template/            # Template for new pipelines
│   │   ├── assets/
│   │   │   └── __init__.py
│   │   ├── defs.py
│   │   ├── pyproject.toml
│   │   └── Dockerfile
│   └── hackernews/           # Example pipeline
│       ├── assets/
│       │   ├── __init__.py
│       │   ├── ingestion.py
│       │   ├── storage.py
│       │   ├── analytics.py
│       │   └── export_csv.py
│       ├── defs.py           # Self-contained Dagster definitions
│       ├── pyproject.toml    # Pipeline-specific dependencies
│       └── Dockerfile
└── .env.example              # Environment template
```

## Adding a New Pipeline

Creating a new pipeline is a single command:

```bash
./dev new my_pipeline
```

This will:
1. Copy the template to `pipelines/my_pipeline/`
2. Update all file references automatically
3. Set up the correct project structure

Then:
1. Add your assets in `pipelines/my_pipeline/assets/`
2. Export them in `pipelines/my_pipeline/assets/__init__.py`
3. Import and register them in `pipelines/my_pipeline/defs.py`
4. Add any dependencies to `pipelines/my_pipeline/pyproject.toml`
5. Run `./dev up` - configs are auto-generated

### Example Asset

```python
# pipelines/my_pipeline/assets/my_asset.py
import pandas as pd
from dagster import asset, AssetExecutionContext

@asset(group_name="my_pipeline")
def my_asset(context: AssetExecutionContext) -> pd.DataFrame:
    # Your logic here
    return pd.DataFrame({"col": [1, 2, 3]})
```

Export in `__init__.py`:
```python
from .my_asset import my_asset
__all__ = ["my_asset"]
```

Update `defs.py`:
```python
from assets import my_asset

defs = Definitions(
    assets=[my_asset],
    resources={...},
)
```

The `generate_platform.py` script automatically discovers all pipelines and generates both dev and production Docker Compose configs.

## Cross-Pipeline Dependencies

Pipelines can depend on assets from other pipelines using DuckLake as the contract. Use `SourceAsset` to declare dependencies:

```python
# In pipelines/my_pipeline/defs.py
from dagster import SourceAsset, asset

# Declare that hackernews_stories exists (managed by hackernews pipeline)
hackernews_stories = SourceAsset(
    key="hackernews_stories",
    description="HackerNews stories from hackernews pipeline",
)

@asset(deps=[hackernews_stories])
def my_downstream_analysis(ducklake: DuckLakeResource):
    """Analyze hackernews stories."""
    with ducklake.get_connection() as conn:
        df = conn.execute("SELECT * FROM my_ducklake.hackernews_stories").fetchdf()
    # ... your analysis
    return result

defs = Definitions(
    assets=[hackernews_stories, my_downstream_analysis],
    resources={...},
)
```

Dagster handles cross-location scheduling automatically.

## dev Reference

| Command | Description |
|---------|-------------|
| `./dev up` | Start Dagster dev environment (Docker) |
| `./dev down` | Stop all containers |
| `./dev new <name>` | Create a new pipeline from template |
| `./dev build` | Rebuild base Docker image |
| `./dev logs [service]` | View logs (optionally for specific service) |
| `./dev db` | Open interactive DuckDB session (local + remote) |
| `./dev db-remote` | Open interactive DuckDB session (remote only) |
| `./dev export <table> [format] [source]` | Export table to file (format: parquet/csv/json/ndjson, source: local/remote) |
| `./dev pull` | List remote tables |
| `./dev pull <table>` | Pull table from remote to local DuckLake |
| `./dev pull <table> <limit>` | Pull limited rows from remote |
| `./dev mark <assets>` | Mark assets as materialized (comma-separated) |

## Client Packages

For accessing SYWI DuckLake data from external scripts or notebooks, use the provided client packages:

### Python: sywi-duckdb

```bash
pip install git+https://github.com/sywi/sywi_data_plattform.git#subdirectory=packages/sywi_duckdb
```

```python
import sywi_duckdb as duckdb

conn = duckdb.connect()
conn.execute("SELECT * FROM local.my_table").fetchdf()
conn.execute("SELECT * FROM remote.my_table").fetchdf()
```

See [packages/sywi_duckdb/README.md](packages/sywi_duckdb/README.md) for details.

### R: sywi.duckdb

```r
remotes::install_github("sywi/sywi_data_plattform", subdir = "packages/sywi.duckdb")
```

```r
library(sywi.duckdb)

# Works just like duckdb - use sywi_duckdb() instead of duckdb()
con <- dbConnect(sywi_duckdb())
dbGetQuery(con, "SELECT * FROM local.my_table")
dbGetQuery(con, "SELECT * FROM remote.my_table")
dbDisconnect(con)
```

See [packages/sywi.duckdb/README.md](packages/sywi.duckdb/README.md) for details.

## Dependencies

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (Python package manager) - for `pull`/`mark` commands
- Docker and Docker Compose
- DuckDB 1.3.0+ (installed automatically via uv)
