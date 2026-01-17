# sywi-duckdb

DuckDB wrapper with auto-configured SYWI DuckLake connections.

## Installation

```bash
pip install git+https://github.com/sywi/sywi_data_plattform.git#subdirectory=packages/sywi_duckdb

# Or with uv
uv add git+https://github.com/sywi/sywi_data_plattform.git#subdirectory=packages/sywi_duckdb
```

## Usage

```python
import sywi_duckdb as duckdb

# connect() auto-loads .env.local and attaches local + remote DuckLake
conn = duckdb.connect()

# Query local database
df = conn.execute("SELECT * FROM local.customers LIMIT 10").fetchdf()

# Query remote database (read-only)
df = conn.execute("SELECT * FROM remote.customers LIMIT 10").fetchdf()

# Copy from remote to local
conn.execute("CREATE TABLE local.my_copy AS SELECT * FROM remote.customers")

# All standard duckdb functions still work
duckdb.query("SELECT 1 + 1")
```

## Configuration

Create a `.env.local` file in your project root (or any parent directory):

```bash
# Local DuckLake
DUCKLAKE_CATALOG_DSN=./data/ducklake.ducklake
DUCKLAKE_DATA_PATH=./data/

# Remote DuckLake
DUCKLAKE_REMOTE_CATALOG_DSN="dbname=ducklake host=server.com user=... password=... port=5432"
DUCKLAKE_REMOTE_DATA_PATH=s3://bucket/data/
DUCKLAKE_REMOTE_S3_REGION=us-east-1
DUCKLAKE_REMOTE_S3_ENDPOINT=minio.server.com:9000
DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID=...
DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY=...
DUCKLAKE_REMOTE_S3_URL_STYLE=path
DUCKLAKE_REMOTE_S3_USE_SSL=false
```

## Databases

The connection provides two attached databases:

| Database | Description |
|----------|-------------|
| `local`  | Local DuckLake (read-write) |
| `remote` | Remote DuckLake (read-only) |

Always prefix table names with the database name:
- `local.my_table`
- `remote.my_table`
