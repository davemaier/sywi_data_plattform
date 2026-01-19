#!/usr/bin/env python3
"""Generate Docker Compose for production deployment.

This script scans the root directory for Dagster projects and generates:
- docker-compose.prod.yaml (production deployment with all services)
- workspace.yaml (Dagster workspace config for production)

For local development, use `dagster dev` with dg.toml instead.
"""

import os
import yaml

# Directories to exclude from project discovery
EXCLUDED_DIRS = {
    "_template",
    "sywi-core",
    "libs",
    "packages",
    "temp",
    "data",
    ".git",
    ".venv",
    "__pycache__",
    ".dagster",
}

COMPOSE_PROD_FILE = "docker-compose.prod.yaml"
WORKSPACE_FILE = "workspace.yaml"


def is_project_dir(path: str) -> bool:
    """Check if a directory is a Dagster project (has pyproject.toml and Dockerfile)."""
    pyproject = os.path.join(path, "pyproject.toml")
    if not os.path.exists(pyproject):
        return False
    # Check if it has a Dockerfile (indicating it's deployable)
    dockerfile = os.path.join(path, "Dockerfile")
    return os.path.exists(dockerfile)


def discover_projects() -> list[str]:
    """Discover all Dagster projects in the root directory."""
    projects = []
    for item in os.listdir("."):
        if item.startswith(".") or item.startswith("_"):
            continue
        if item in EXCLUDED_DIRS:
            continue
        if os.path.isdir(item) and is_project_dir(item):
            projects.append(item)
    return sorted(projects)


def generate_compose(projects: list[str]) -> dict:
    """Generate production Docker Compose configuration."""
    services = {
        "postgresql": {
            "image": "postgres:16",
            "environment": {
                "POSTGRES_USER": "dagster_user",
                "POSTGRES_PASSWORD": "dagster_password",
                "POSTGRES_DB": "dagster_db",
            },
            "volumes": [
                "dagster_pgdata:/var/lib/postgresql/data",
                "./init-db.sh:/docker-entrypoint-initdb.d/init-db.sh:ro",
            ],
            "networks": ["sywi-dagster"],
            "healthcheck": {
                "test": ["CMD-SHELL", "pg_isready -U dagster_user -d dagster_db"],
                "interval": "5s",
                "timeout": "5s",
                "retries": 5,
            },
        },
        "dagster_daemon": {
            "build": {
                "context": ".",
                "dockerfile": "Dockerfile.dagster",
            },
            "command": "dagster-daemon run",
            "environment": {"DAGSTER_HOME": "/opt/dagster/dagster_home"},
            "env_file": ".env",
            "volumes": [
                "./workspace.yaml:/opt/dagster/app/workspace.yaml",
                "./dagster.yaml:/opt/dagster/dagster_home/dagster.yaml",
                "./data:/opt/dagster/app/data",
            ],
            "depends_on": {
                "postgresql": {"condition": "service_healthy"},
            },
            "networks": ["sywi-dagster"],
            "restart": "unless-stopped",
        },
        "dagster_webserver": {
            "build": {
                "context": ".",
                "dockerfile": "Dockerfile.dagster",
            },
            "command": "dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml --path-prefix /dagster",
            "environment": {
                "DAGSTER_HOME": "/opt/dagster/dagster_home",
                "DAGSTER_UI_PATH_PREFIX": "/dagster",
            },
            "env_file": ".env",
            "volumes": [
                "./workspace.yaml:/opt/dagster/app/workspace.yaml",
                "./dagster.yaml:/opt/dagster/dagster_home/dagster.yaml",
                "./data:/opt/dagster/app/data",
            ],
            "ports": ["3000:3000"],
            "depends_on": {
                "postgresql": {"condition": "service_healthy"},
                "dagster_daemon": {"condition": "service_started"},
            },
            "networks": ["sywi-dagster"],
            "restart": "unless-stopped",
        },
    }

    # Add project services
    for project in projects:
        service_name = f"project_{project}"
        services[service_name] = {
            "build": {
                "context": ".",
                "dockerfile": f"{project}/Dockerfile",
            },
            "env_file": ".env",
            "expose": ["4000"],
            "networks": ["sywi-dagster"],
            "restart": "unless-stopped",
            "ulimits": {"nofile": {"soft": 65536, "hard": 65536}},
            "depends_on": {
                "postgresql": {"condition": "service_healthy"},
            },
        }

        # Add project as dependency for daemon and webserver
        services["dagster_daemon"]["depends_on"][service_name] = {
            "condition": "service_started"
        }
        services["dagster_webserver"]["depends_on"][service_name] = {
            "condition": "service_started"
        }

    return {
        "services": services,
        "networks": {"sywi-dagster": {"name": "sywi-dagster", "driver": "bridge"}},
        "volumes": {"dagster_pgdata": None},
    }


def generate_workspace(projects: list[str]) -> dict:
    """Generate Dagster workspace configuration for production."""
    load_from = []
    for project in projects:
        load_from.append(
            {
                "grpc_server": {
                    "host": f"project_{project}",
                    "port": 4000,
                    "location_name": project,
                }
            }
        )
    return {"load_from": load_from}


def main():
    # Discover projects
    projects = discover_projects()
    print(f"Discovered {len(projects)} project(s): {', '.join(projects)}")

    # Generate production compose
    compose = generate_compose(projects)
    with open(COMPOSE_PROD_FILE, "w") as f:
        f.write("# AUTO-GENERATED - DO NOT EDIT\n")
        f.write("# Production Docker Compose configuration\n")
        f.write("# Usage: docker compose -f docker-compose.prod.yaml up --build\n\n")
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)
    print(f"Generated {COMPOSE_PROD_FILE}")

    # Generate workspace config
    workspace = generate_workspace(projects)
    with open(WORKSPACE_FILE, "w") as f:
        f.write("# AUTO-GENERATED - DO NOT EDIT\n")
        f.write("# Dagster workspace configuration for production\n\n")
        if workspace["load_from"]:
            yaml.dump(workspace, f, default_flow_style=False)
        else:
            f.write("load_from: []\n")
    print(f"Generated {WORKSPACE_FILE}")


if __name__ == "__main__":
    main()
