#!/usr/bin/env python3
"""Cross-platform development CLI for Dagster pipelines.

Usage:
    uv run dev <command> [options]
    just <command>
"""

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import duckdb
import typer
from dotenv import load_dotenv
from rich.console import Console

# Setup paths and load environment
SCRIPT_DIR = Path(__file__).parent.resolve()
ENV_FILE = SCRIPT_DIR / ".env.local"
PROD_COMPOSE = "docker-compose.prod.yaml"

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
    """Run a docker compose command."""
    return run(["docker", "compose", *args], check=check)


def docker_compose_prod(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a docker compose command with production compose file."""
    return run(["docker", "compose", "-f", PROD_COMPOSE, *args], check=check)


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


def _wait_for_postgres(max_attempts: int = 30) -> bool:
    """Wait for PostgreSQL to be ready."""
    import socket

    for attempt in range(max_attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", 5432))
            sock.close()
            if result == 0:
                return True
        except Exception:
            pass
        time.sleep(1)
        if attempt % 5 == 0:
            console.print(f"Waiting for PostgreSQL... ({attempt}/{max_attempts})")
    return False


def build_duckdb_init_sql(
    default_schema: str = "local", include_remote: bool = True
) -> str:
    """Build DuckDB initialization SQL with local and optionally remote DuckLake attached."""
    local_catalog_dsn = _dsn_for_host(os.environ.get("DUCKLAKE_CATALOG_DSN") or "")
    local_data_path = os.environ.get("DUCKLAKE_DATA_PATH", "")
    local_schema = os.environ.get("DUCKLAKE_SCHEMA", "local")

    lines = [
        "INSTALL ducklake; LOAD ducklake;",
        "INSTALL postgres; LOAD postgres;",
    ]

    lines.extend(
        [
            "",
            f"ATTACH 'ducklake:postgres:{local_catalog_dsn}' AS {local_schema} (DATA_PATH '{local_data_path}');",
        ]
    )

    if include_remote:
        remote_catalog_dsn = os.environ.get("DUCKLAKE_REMOTE_CATALOG_DSN", "")
        remote_data_path = os.environ.get("DUCKLAKE_REMOTE_DATA_PATH", "")

        lines.extend(
            [
                "INSTALL httpfs; LOAD httpfs;",
                "",
                f"SET s3_region = '{os.environ.get('DUCKLAKE_REMOTE_S3_REGION', 'us-east-1')}';",
                f"SET s3_endpoint = '{os.environ.get('DUCKLAKE_REMOTE_S3_ENDPOINT', '')}';",
                f"SET s3_url_style = '{os.environ.get('DUCKLAKE_REMOTE_S3_URL_STYLE', 'path')}';",
                f"SET s3_access_key_id = '{os.environ.get('DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID', '')}';",
                f"SET s3_secret_access_key = '{os.environ.get('DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY', '')}';",
                f"SET s3_use_ssl = {os.environ.get('DUCKLAKE_REMOTE_S3_USE_SSL', 'false')};",
                f"ATTACH 'ducklake:postgres:{remote_catalog_dsn}' AS remote (DATA_PATH '{remote_data_path}', READ_ONLY);",
            ]
        )

    lines.extend(
        [
            "",
            f"USE {default_schema};",
        ]
    )

    return "\n".join(lines)


@app.command()
def up():
    """Start Dagster dev environment (native Python, PostgreSQL in Docker)."""
    # Ensure data directory exists
    (SCRIPT_DIR / "data").mkdir(parents=True, exist_ok=True)

    # Check for .env.local
    if not ENV_FILE.exists():
        err_console.print(
            "[red]Error:[/red] .env.local not found. "
            "Copy .env.example to .env.local and configure it."
        )
        raise typer.Exit(1)

    # Start PostgreSQL
    console.print("Starting PostgreSQL...")
    docker_compose("up", "-d", "postgresql")

    # Wait for PostgreSQL to be ready
    if not _wait_for_postgres():
        err_console.print("[red]Error:[/red] PostgreSQL failed to start")
        raise typer.Exit(1)

    console.print("[green]PostgreSQL is ready![/green]")

    console.print()
    console.print("Starting Dagster dev server (via dg)...")
    console.print(
        "Open [link=http://localhost:3000]http://localhost:3000[/link] in your browser"
    )
    console.print()

    # Run dg dev which handles multi-code-location isolation via dg.toml
    os.execvp("dg", ["dg", "dev"])


@app.command()
def down():
    """Stop PostgreSQL container."""
    docker_compose("down")


@app.command()
def prod():
    """Start production-like environment (full Docker stack)."""
    # Check for .env.local
    if not ENV_FILE.exists():
        err_console.print(
            "[red]Error:[/red] .env.local not found. "
            "Copy .env.example to .env.local and configure it."
        )
        raise typer.Exit(1)

    # Generate production compose
    console.print("Generating production config...")
    run(["uv", "run", "python", "generate_platform.py"])

    console.print()
    console.print("Starting production environment...")
    console.print(
        "Open [link=http://localhost:3000]http://localhost:3000[/link] in your browser"
    )
    console.print()

    docker_compose_prod("up", "--build")


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

    dest = SCRIPT_DIR / name
    if dest.exists():
        err_console.print(
            f"[red]Error:[/red] Pipeline '{name}' already exists at {dest}"
        )
        raise typer.Exit(1)

    console.print(f"Creating new pipeline: [bold]{name}[/bold]")

    # Copy template
    template_dir = SCRIPT_DIR / "_template"
    if not template_dir.exists():
        err_console.print(
            f"[red]Error:[/red] Template directory not found at {template_dir}"
        )
        raise typer.Exit(1)

    shutil.copytree(template_dir, dest)

    # Replace placeholders in all files
    for file_path in dest.rglob("*"):
        if file_path.is_file():
            try:
                content = file_path.read_text()
                # Replace __name__ placeholder
                content = content.replace("__name__", name)
                content = content.replace("__NAME__", name.upper())
                file_path.write_text(content)
            except UnicodeDecodeError:
                # Skip binary files
                pass

    # Rename src/__name__ directory if it exists
    name_dir = dest / "src" / "__name__"
    if name_dir.exists():
        name_dir.rename(dest / "src" / name)

    # Generate lock file
    console.print("Generating lock file...")
    run(["uv", "lock"], cwd=dest)

    # Add to dg.toml
    dg_toml = SCRIPT_DIR / "dg.toml"
    if dg_toml.exists():
        content = dg_toml.read_text()
        if f'path = "{name}"' not in content:
            content += f'\n[[workspace.projects]]\npath = "{name}"\n'
            dg_toml.write_text(content)

    console.print()
    console.print(f"[green]Pipeline '{name}' created at {dest}[/green]")
    console.print()
    console.print("Next steps:")
    console.print(f"  1. Add your assets in {dest}/src/{name}/assets/")
    console.print(f"  2. Update {dest}/src/{name}/definitions.py")
    console.print(f"  3. Add any dependencies to {dest}/pyproject.toml")
    console.print("  4. Run [bold]just dev[/bold] to start the dev environment")


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
def db(
    local_only: bool = typer.Option(
        False, "--local-only", "-l", help="Only attach local database (skip remote)"
    ),
):
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

    sql = build_duckdb_init_sql(include_remote=not local_only)
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

    schema = os.environ.get("DUCKLAKE_SCHEMA") or "local"
    if source == "remote":
        init_sql = build_duckdb_init_sql("remote")
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
    # Ensure data directory exists
    (SCRIPT_DIR / "data").mkdir(parents=True, exist_ok=True)

    init_sql = build_duckdb_init_sql()
    conn = duckdb.connect()
    conn.execute(init_sql)

    schema = os.environ.get("DUCKLAKE_SCHEMA") or "local"
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

        # Mark asset as materialized in native Dagster instance
        _mark_asset_materialized(table, "Pulled from remote DuckLake")


@app.command()
def mark(
    assets: str = typer.Argument(
        ..., help="Comma-separated list of asset names to mark"
    ),
):
    """Mark assets as materialized without running them."""
    for asset_name in assets.split(","):
        asset_name = asset_name.strip()
        _mark_asset_materialized(asset_name, "Manually marked as materialized")


def _mark_asset_materialized(asset_name: str, description: str) -> None:
    """Mark an asset as materialized in the native Dagster instance."""
    try:
        from dagster import DagsterInstance, AssetKey, AssetMaterialization

        instance = DagsterInstance.get()
        key = AssetKey(asset_name)

        instance.report_runless_asset_event(
            AssetMaterialization(
                asset_key=key,
                description=description,
                metadata={"source": "dev cli"},
            )
        )
        console.print(f"Marked {asset_name} as materialized")
    except Exception as e:
        err_console.print(f"[yellow]Warning:[/yellow] Could not mark {asset_name}: {e}")


def main():
    """Entry point for the CLI."""
    os.chdir(SCRIPT_DIR)
    app()


if __name__ == "__main__":
    main()
