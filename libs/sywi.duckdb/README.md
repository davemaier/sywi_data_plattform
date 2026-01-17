# sywi.duckdb

R package for connecting to SYWI DuckLake databases.

This is a drop-in wrapper around the `duckdb` R package that automatically configures connections to SYWI DuckLake databases. It loads environment variables from `.env` and `.env.local` files and attaches both local and remote DuckLake databases.

## Installation

Install directly from GitHub:

```r
# Install remotes if you don't have it
install.packages("remotes")

# Install sywi.duckdb
remotes::install_github("sywi/sywi_data_plattform", subdir = "packages/sywi.duckdb")
```

Or install from a local clone:

```r
# From the repository root
install.packages("packages/sywi.duckdb", repos = NULL, type = "source")
```

## Usage

Works just like the standard `duckdb` package - use `sywi_duckdb()` instead of `duckdb()`:

```r
library(sywi.duckdb)

# Connect using standard DBI pattern (just like duckdb)
con <- dbConnect(sywi_duckdb())

# Query local database
local_data <- dbGetQuery(con, "SELECT * FROM local.my_table")

# Query remote database (read-only)
remote_data <- dbGetQuery(con, "SELECT * FROM remote.my_table")

# List tables
local_tables <- dbGetQuery(con, "SHOW TABLES FROM local")
remote_tables <- dbGetQuery(con, "SHOW TABLES FROM remote")

# Copy from remote to local
dbExecute(con, "CREATE OR REPLACE TABLE local.my_table AS SELECT * FROM remote.my_table")

# Disconnect when done
dbDisconnect(con)
```

### Comparison with standard duckdb

| Standard duckdb | sywi.duckdb |
|-----------------|-------------|
| `library(duckdb)` | `library(sywi.duckdb)` |
| `con <- dbConnect(duckdb())` | `con <- dbConnect(sywi_duckdb())` |
| Manual setup required | Auto-configured with local + remote |

Everything else (querying, writing, disconnecting) works exactly the same.

## Environment Variables

The package automatically loads environment variables from `.env` and `.env.local` files, searching upward from the current working directory. `.env.local` values override `.env` values.

### Required Variables

| Variable | Description |
|----------|-------------|
| `DUCKLAKE_CATALOG_DSN` | Local DuckLake catalog path |
| `DUCKLAKE_DATA_PATH` | Local data path |
| `DUCKLAKE_REMOTE_CATALOG_DSN` | Remote Postgres connection string |
| `DUCKLAKE_REMOTE_DATA_PATH` | Remote S3 path |
| `DUCKLAKE_REMOTE_S3_ENDPOINT` | S3 endpoint |
| `DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID` | S3 access key |
| `DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY` | S3 secret key |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKLAKE_REMOTE_S3_REGION` | `us-east-1` | S3 region |
| `DUCKLAKE_REMOTE_S3_URL_STYLE` | `path` | S3 URL style |
| `DUCKLAKE_REMOTE_S3_USE_SSL` | `false` | Use SSL for S3 |

## Comparison with Python Package

| Python | R |
|--------|---|
| `import sywi_duckdb as duckdb` | `library(sywi.duckdb)` |
| `conn = duckdb.connect()` | `con <- dbConnect(sywi_duckdb())` |
| `conn.execute("SELECT ...").fetchdf()` | `dbGetQuery(con, "SELECT ...")` |
| `conn.close()` | `dbDisconnect(con)` |

## Dependencies

- `duckdb` (>= 1.0.0)
- `DBI`
