#!/usr/bin/env python3
"""Cross-platform development CLI for Dagster pipelines.

Usage:
    uv run dev <command> [options]

On Unix, you can also use:
    ./dev <command> [options]
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import duckdb
import typer
from dotenv import load_dotenv
from rich.console import Console

# Setup paths and load environment
SCRIPT_DIR = Path(__file__).parent.resolve()
ENV_FILE = SCRIPT_DIR / ".env.local"
DEV_COMPOSE = "docker-compose.dev.generated.yaml"

# Load .env.local if it exists
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

# Rich console for pretty output
console = Console()
err_console = Console(stderr=True)

# Typer app
app = typer.Typer(
    help="Local development CLI for Dagster pipelines.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)


def run(
    cmd: list[str],
    check: bool = True,
    capture_output: bool = False,
    cwd: Optional[Path] = None,
) -> subprocess.CompletedProcess:
    """Run a subprocess command."""
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture_output,
        text=True,
        cwd=cwd or SCRIPT_DIR,
    )


def docker_compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a docker compose command with the dev compose file."""
    return run(["docker", "compose", "-f", DEV_COMPOSE, *args], check=check)


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Get environment variable with optional default."""
    return os.environ.get(name, default)


def require_env(name: str) -> str:
    """Get required environment variable or exit with error."""
    value = os.environ.get(name)
    if not value:
        err_console.print(f"[red]Error:[/red] {name} not set in .env.local")
        raise typer.Exit(1)
    return value


def _dsn_for_host(dsn: str) -> str:
    """Convert Docker hostname to localhost for CLI usage outside containers."""
    return dsn.replace("host=postgresql", "host=localhost")


def _is_postgres_dsn(dsn: str) -> bool:
    """Check if DSN is for Postgres mode (vs local file mode)."""
    return "host=" in dsn


def build_duckdb_init_sql(
    include_remote: bool = False, remote_only: bool = False
) -> str:
    """Build DuckDB initialization SQL for local and/or remote DuckLake."""
    # Get local config to determine if we need postgres extension
    local_catalog_dsn = _dsn_for_host(get_env("DUCKLAKE_CATALOG_DSN") or "")
    local_is_postgres = _is_postgres_dsn(local_catalog_dsn)

    lines = [
        "INSTALL ducklake;",
        "LOAD ducklake;",
    ]

    # Load postgres extension if needed (for local or remote)
    if local_is_postgres or include_remote or remote_only:
        lines.extend(
            [
                "INSTALL postgres;",
                "LOAD postgres;",
            ]
        )

    # Load httpfs and S3 config for remote
    if include_remote or remote_only:
        lines.extend(
            [
                "INSTALL httpfs;",
                "LOAD httpfs;",
                "",
                f"SET s3_region = '{get_env('DUCKLAKE_REMOTE_S3_REGION', 'us-east-1')}';",
                f"SET s3_endpoint = '{get_env('DUCKLAKE_REMOTE_S3_ENDPOINT', '')}';",
                f"SET s3_url_style = '{get_env('DUCKLAKE_REMOTE_S3_URL_STYLE', 'path')}';",
                f"SET s3_access_key_id = '{get_env('DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID', '')}';",
                f"SET s3_secret_access_key = '{get_env('DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY', '')}';",
                f"SET s3_use_ssl = {get_env('DUCKLAKE_REMOTE_S3_USE_SSL', 'false')};",
                "",
            ]
        )

    if not remote_only:
        data_path = get_env("DUCKLAKE_DATA_PATH", "")
        schema = get_env("DUCKLAKE_SCHEMA", "local")
        if local_is_postgres:
            lines.append(
                f"ATTACH 'ducklake:postgres:{local_catalog_dsn}' AS {schema} (DATA_PATH '{data_path}');"
            )
        else:
            lines.append(
                f"ATTACH 'ducklake:{local_catalog_dsn}' AS {schema} (DATA_PATH '{data_path}');"
            )

    if include_remote or remote_only:
        remote_catalog_dsn = get_env("DUCKLAKE_REMOTE_CATALOG_DSN", "")
        remote_data_path = get_env("DUCKLAKE_REMOTE_DATA_PATH", "")
        read_only = ", READ_ONLY" if not remote_only else ""
        lines.append(
            f"ATTACH 'ducklake:postgres:{remote_catalog_dsn}' AS remote "
            f"(DATA_PATH '{remote_data_path}'{read_only});"
        )

    # Set default database
    schema = get_env("DUCKLAKE_SCHEMA", "local")
    if remote_only:
        lines.append("USE remote;")
    else:
        lines.append(f"USE {schema};")

    return "\n".join(lines)


@app.command()
def up():
    """Start Dagster dev environment (Docker)."""
    # Ensure data directory exists
    (SCRIPT_DIR / "data").mkdir(parents=True, exist_ok=True)

    # Check for .env.local
    if not ENV_FILE.exists():
        err_console.print(
            "[red]Error:[/red] .env.local not found. "
            "Copy .env.example to .env.local and configure it."
        )
        raise typer.Exit(1)

    # Build base image if needed
    result = run(
        ["docker", "image", "inspect", "my-platform-base:latest"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        console.print("Building base image...")
        run(
            [
                "docker",
                "build",
                "-t",
                "my-platform-base:latest",
                "-f",
                "Dockerfile.base",
                ".",
            ]
        )

    # Generate configs
    console.print("Generating configs...")
    run(["uv", "run", "python", "generate_platform.py"])

    console.print()
    console.print("Starting Dagster dev environment...")
    console.print(
        "Open [link=http://localhost:3000]http://localhost:3000[/link] in your browser"
    )
    console.print()
    console.print("Hot reload: Edit code, then click 'Reload' in the Dagster UI")
    console.print()

    docker_compose("up")


@app.command()
def down():
    """Stop all containers."""
    docker_compose("down")


@app.command()
def new(
    name: str = typer.Argument(
        ..., help="Name for the new pipeline (lowercase, underscores)"
    ),
):
    """Create a new pipeline from template."""
    # Validate name
    if not re.match(r"^[a-z][a-z0-9_]*$", name):
        err_console.print(
            "[red]Error:[/red] Pipeline name must be lowercase, start with a letter, "
            "and contain only letters, numbers, and underscores."
        )
        raise typer.Exit(1)

    dest = SCRIPT_DIR / "pipelines" / name
    if dest.exists():
        err_console.print(
            f"[red]Error:[/red] Pipeline '{name}' already exists at {dest}"
        )
        raise typer.Exit(1)

    console.print(f"Creating new pipeline: [bold]{name}[/bold]")

    # Copy template
    template_dir = SCRIPT_DIR / "pipelines" / "_template"
    shutil.copytree(template_dir, dest)

    # Update Dockerfile
    dockerfile = dest / "Dockerfile"
    content = dockerfile.read_text()
    content = content.replace("my_pipeline", name)
    dockerfile.write_text(content)

    # Update pyproject.toml
    pyproject = dest / "pyproject.toml"
    content = pyproject.read_text()
    content = content.replace("my-pipeline", f"{name}-pipeline")
    pyproject.write_text(content)

    # Update defs.py
    defs_file = dest / "defs.py"
    content = defs_file.read_text()
    content = content.replace("my_pipeline", name)
    defs_file.write_text(content)

    console.print()
    console.print(f"[green]Pipeline '{name}' created at {dest}[/green]")
    console.print()
    console.print("Next steps:")
    console.print(f"  1. Add your assets in {dest}/assets/")
    console.print(f"  2. Update {dest}/defs.py to import and register your assets")
    console.print(f"  3. Add any dependencies to {dest}/pyproject.toml")
    console.print("  4. Run [bold]./dev up[/bold] to start the dev environment")


@app.command()
def build():
    """Rebuild base Docker image."""
    console.print("Rebuilding base image...")
    run(
        [
            "docker",
            "build",
            "-t",
            "my-platform-base:latest",
            "-f",
            "Dockerfile.base",
            ".",
        ]
    )


@app.command()
def logs(
    service: Optional[str] = typer.Argument(
        None, help="Specific service to view logs for"
    ),
):
    """View logs (optionally for specific service)."""
    if service:
        docker_compose("logs", "-f", service)
    else:
        docker_compose("logs", "-f")


@app.command()
def db():
    """Open interactive DuckDB session (local + remote attached).

    Requires DuckDB CLI to be installed separately:
    - macOS: brew install duckdb
    - Windows: winget install DuckDB.cli
    - Linux: https://duckdb.org/docs/installation/
    """
    # Check if duckdb CLI is available
    if not shutil.which("duckdb"):
        err_console.print("[red]Error:[/red] DuckDB CLI not found.")
        err_console.print("Install it separately:")
        err_console.print("  macOS:   brew install duckdb")
        err_console.print("  Windows: winget install DuckDB.cli")
        err_console.print("  Linux:   https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    sql = build_duckdb_init_sql(include_remote=True)
    run(["duckdb", "-cmd", sql])


@app.command("export")
def export_table(
    table: str = typer.Argument(..., help="Table name to export"),
    format: str = typer.Argument(
        "parquet", help="Export format: parquet, csv, json, ndjson"
    ),
    source: str = typer.Argument("local", help="Data source: local or remote"),
):
    """Export table to local file."""
    # Validate format
    format_map = {
        "parquet": ("parquet", "FORMAT PARQUET"),
        "csv": ("csv", "FORMAT CSV, HEADER"),
        "json": ("json", "FORMAT JSON, ARRAY true"),
        "ndjson": ("ndjson", "FORMAT JSON"),
    }

    if format not in format_map:
        err_console.print(f"[red]Error:[/red] Unknown format '{format}'")
        err_console.print("Supported formats: parquet, csv, json, ndjson")
        raise typer.Exit(1)

    ext, format_opts = format_map[format]
    output_file = f"{table}.{ext}"

    schema = get_env("DUCKLAKE_SCHEMA") or "local"
    if source == "remote":
        require_env("DUCKLAKE_REMOTE_CATALOG_DSN")
        init_sql = build_duckdb_init_sql(remote_only=True)
        source_table = f"remote.{table}"
    else:
        init_sql = build_duckdb_init_sql()
        source_table = f"{schema}.{table}"

    copy_sql = f"COPY (SELECT * FROM {source_table}) TO '{output_file}' ({format_opts})"
    count_sql = f"SELECT COUNT(*) FROM {source_table}"

    conn = duckdb.connect()
    conn.execute(init_sql)
    conn.execute(copy_sql)
    result = conn.execute(count_sql).fetchone()
    row_count = result[0] if result else 0
    console.print(f"{output_file}: {row_count} rows exported from {source}")


@app.command()
def pull(
    table: Optional[str] = typer.Argument(
        None, help="Table to pull (omit to list tables)"
    ),
    limit: Optional[int] = typer.Argument(None, help="Limit number of rows to pull"),
):
    """List remote tables or pull table from remote DuckLake."""
    require_env("DUCKLAKE_REMOTE_CATALOG_DSN")

    # Ensure data directory exists
    (SCRIPT_DIR / "data").mkdir(parents=True, exist_ok=True)

    init_sql = build_duckdb_init_sql(include_remote=True)
    conn = duckdb.connect()
    conn.execute(init_sql)

    schema = get_env("DUCKLAKE_SCHEMA") or "local"
    if table is None:
        # List tables
        console.print("Remote tables:")
        tables = conn.execute("SELECT name FROM (SHOW TABLES FROM remote)").fetchall()
        for row in tables:
            console.print(row[0])
    else:
        # Pull table
        conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        if limit:
            conn.execute(
                f"CREATE TABLE {schema}.{table} AS SELECT * FROM remote.{table} LIMIT {limit}"
            )
        else:
            conn.execute(
                f"CREATE TABLE {schema}.{table} AS SELECT * FROM remote.{table}"
            )

        result = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()
        row_count = result[0] if result else 0
        console.print(f"{table}: {row_count} rows pulled")

        # Mark asset as materialized in Dagster
        mark_script = f"""
from dagster import DagsterInstance, AssetKey, AssetMaterialization

instance = DagsterInstance.get()
asset_name = '{table}'
key = AssetKey(asset_name)

try:
    instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=key,
            description='Pulled from remote DuckLake',
            metadata={{'source': 'dev pull'}}
        )
    )
    print(f'Marked {{asset_name}} as materialized')
except Exception as e:
    pass
"""
        docker_compose(
            "exec",
            "-T",
            "dagster_webserver",
            "python",
            "-c",
            mark_script,
            check=False,
        )


@app.command()
def mark(
    assets: str = typer.Argument(
        ..., help="Comma-separated list of asset names to mark"
    ),
):
    """Mark assets as materialized without running them."""
    mark_script = f"""
from dagster import DagsterInstance, AssetKey, AssetMaterialization

instance = DagsterInstance.get()

assets = '{assets}'.split(',')
for asset_name in assets:
    asset_name = asset_name.strip()
    key = AssetKey(asset_name)

    instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=key,
            description='Manually marked as materialized',
            metadata={{'source': 'dev mark'}}
        )
    )
    print(f'Marked {{asset_name}} as materialized')
"""
    docker_compose("exec", "dagster_webserver", "python", "-c", mark_script)


def main():
    """Entry point for the CLI."""
    os.chdir(SCRIPT_DIR)
    app()


if __name__ == "__main__":
    main()
