#!/usr/bin/env python3
"""Cross-platform development CLI for Dagster projects.

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
SCRIPT_DIR = Path(__file__).parent.parent.parent.resolve()
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
    help="Local development CLI for Dagster projects.",
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


def _check_docker_running() -> None:
    """Check if Docker daemon is running."""
    result = run(["docker", "info"], check=False, capture_output=True)
    if result.returncode != 0:
        err_console.print("[red]Error:[/red] Docker daemon is not running.")
        raise typer.Exit(1)


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


def _get_all_projects() -> list[dict]:
    """Get all projects defined in dg.toml."""
    import tomllib

    dg_toml = SCRIPT_DIR / "dg.toml"
    if not dg_toml.exists():
        return []

    with open(dg_toml, "rb") as f:
        config = tomllib.load(f)

    return config.get("workspace", {}).get("projects", [])


def _get_enabled_projects() -> list[dict]:
    """Get enabled projects based on DEV_PROJECTS env var.

    Returns:
        List of enabled project dicts (each with 'path' key).
        If DEV_PROJECTS is empty/unset, returns all projects.
        If DEV_PROJECTS is set, validates and returns only those projects.

    Raises:
        typer.Exit: If any project in DEV_PROJECTS is not found in dg.toml.
    """
    all_projects = _get_all_projects()
    all_paths: set[str] = {p.get("path") for p in all_projects if p.get("path")}  # type: ignore[misc]

    dev_projects_env = os.environ.get("DEV_PROJECTS", "").strip()

    # Empty or unset means start all projects
    if not dev_projects_env:
        return all_projects

    # Parse comma-separated list
    requested = [p.strip() for p in dev_projects_env.split(",") if p.strip()]

    # Validate all requested projects exist
    for project_name in requested:
        if project_name not in all_paths:
            err_console.print(
                f"[red]Error:[/red] Project '{project_name}' not found in dg.toml"
            )
            err_console.print(f"Available projects: {', '.join(sorted(all_paths))}")
            raise typer.Exit(1)

    # Filter to only requested projects
    return [p for p in all_projects if p.get("path") in requested]


def _sync_subprojects(projects: list[dict] | None = None) -> None:
    """Sync subprojects.

    Args:
        projects: List of project dicts to sync. If None, syncs all from dg.toml.
    """
    if projects is None:
        projects = _get_all_projects()

    if not projects:
        return

    console.print("Syncing subprojects...")
    for project in projects:
        path = project.get("path")
        if path:
            project_dir = SCRIPT_DIR / path
            if project_dir.exists():
                console.print(f"  Syncing [bold]{path}[/bold]...")
                try:
                    env = os.environ.copy()
                    env.pop("VIRTUAL_ENV", None)
                    subprocess.run(["uv", "sync"], check=True, cwd=project_dir, env=env)
                except subprocess.CalledProcessError as e:
                    err_console.print(f"[red]Error:[/red] Failed to sync {path}")
                    raise typer.Exit(1) from e
    console.print("[green]All subprojects synced![/green]")
    console.print()


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


def _create_temp_dg_toml(projects: list[dict]) -> Path:
    """Create a temporary dg.toml with only the specified projects.

    Args:
        projects: List of project dicts to include.

    Returns:
        Path to the directory containing the temp dg.toml.
    """
    # Use a fixed location inside .dagster for visibility
    temp_dir = SCRIPT_DIR / ".dagster" / "dev-workspace"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Build dg.toml content
    lines = ['directory_type = "workspace"', "", "[workspace]"]
    for project in projects:
        path = project.get("path")
        if path:
            lines.append("[[workspace.projects]]")
            lines.append(f'path = "../../{path}"')
            lines.append("")

    temp_dg_toml = temp_dir / "dg.toml"
    temp_dg_toml.write_text("\n".join(lines))

    return temp_dir


@app.command()
def up():
    """Start Dagster dev environment (native Python, PostgreSQL in Docker)."""
    _check_docker_running()

    # Ensure data directory exists
    (SCRIPT_DIR / "data").mkdir(parents=True, exist_ok=True)

    # Check for .env.local
    if not ENV_FILE.exists():
        err_console.print(
            "[red]Error:[/red] .env.local not found. "
            "Copy .env.example to .env.local and configure it."
        )
        raise typer.Exit(1)

    # Get enabled projects based on DEV_PROJECTS env var
    all_projects = _get_all_projects()
    enabled_projects = _get_enabled_projects()

    # Show which projects are enabled
    enabled_names: list[str] = [p["path"] for p in enabled_projects if p.get("path")]
    if len(enabled_projects) < len(all_projects):
        console.print(
            f"Enabled projects: [bold]{', '.join(enabled_names)}[/bold] "
            f"({len(enabled_projects)} of {len(all_projects)})"
        )
    else:
        console.print(f"Enabled projects: [bold]all[/bold] ({len(all_projects)})")
    console.print()

    # Sync only enabled subprojects
    _sync_subprojects(enabled_projects)

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

    # Set DAGSTER_HOME to project's .dagster/home directory
    dagster_home = SCRIPT_DIR / ".dagster" / "home"
    dagster_home.mkdir(parents=True, exist_ok=True)
    os.environ["DAGSTER_HOME"] = str(dagster_home)

    # Create dagster.yaml for dev using PostgreSQL storage (avoids SQLite locking issues)
    dagster_yaml = dagster_home / "dagster.yaml"
    dagster_yaml.write_text(
        "storage:\n"
        "  postgres:\n"
        "    postgres_db:\n"
        "      username: dagster_user\n"
        "      password: dagster_password\n"
        "      hostname: localhost\n"
        "      db_name: dagster_db\n"
        "      port: 5432\n"
        "\n"
        "telemetry:\n"
        "  enabled: false\n"
    )

    # Create temp dg.toml with only enabled projects and run dg dev from there
    if len(enabled_projects) < len(all_projects):
        temp_dir = _create_temp_dg_toml(enabled_projects)
        os.execvp("dg", ["dg", "dev", "--target-path", str(temp_dir)])
    else:
        # All projects enabled, use normal dg.toml
        os.execvp("dg", ["dg", "dev"])


@app.command()
def down():
    """Stop PostgreSQL container."""
    docker_compose("down")


@app.command()
def prod():
    """Start production-like environment (full Docker stack)."""
    _check_docker_running()

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
        ..., help="Name for the new project (lowercase, underscores)"
    ),
):
    """Create a new project (Dagster code location) from template."""
    # Validate name
    if not re.match(r"^[a-z][a-z0-9_]*$", name):
        err_console.print(
            "[red]Error:[/red] Project name must be lowercase, start with a letter, "
            "and contain only letters, numbers, and underscores."
        )
        raise typer.Exit(1)

    dest = SCRIPT_DIR / name
    if dest.exists():
        err_console.print(
            f"[red]Error:[/red] Project '{name}' already exists at {dest}"
        )
        raise typer.Exit(1)

    console.print(f"Creating new project: [bold]{name}[/bold]")

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
    console.print(f"[green]Project '{name}' created at {dest}[/green]")
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
