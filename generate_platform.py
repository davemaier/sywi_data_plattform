#!/usr/bin/env python3
"""Generate Docker Compose and workspace configs for all pipelines.

This script scans the pipelines/ directory and generates:
- docker-compose.override.generated.yaml (production)
- docker-compose.dev.generated.yaml (development with volume mounts)
- workspace.generated.yaml (Dagster workspace config)
"""

import os
import yaml

PIPELINE_DIR = "./pipelines"
COMPOSE_PROD_FILE = "docker-compose.override.generated.yaml"
COMPOSE_DEV_FILE = "docker-compose.dev.generated.yaml"
WORKSPACE_FILE = "workspace.generated.yaml"

# Production compose - just pipeline services
compose_prod = {"services": {}}

# Dev compose - full stack with volume mounts
compose_dev = {
    "services": {
        "postgresql": {
            "image": "postgres:16",
            "environment": {
                "POSTGRES_USER": "dagster_user",
                "POSTGRES_PASSWORD": "dagster_password",
                "POSTGRES_DB": "dagster_db",
            },
            "volumes": ["dagster_pgdata_dev:/var/lib/postgresql/data"],
            "networks": ["dagster_network"],
            "ports": ["5432:5432"],
        },
        "dagster_daemon": {
            "image": "my-platform-base:latest",
            "command": "dagster-daemon run",
            "environment": {"DAGSTER_HOME": "/opt/dagster/dagster_home"},
            "env_file": ".env.local",
            "volumes": [
                "./workspace.generated.yaml:/opt/dagster/app/workspace.yaml",
                "./dagster.yaml:/opt/dagster/dagster_home/dagster.yaml",
                "./data:/opt/dagster/app/data",
            ],
            "depends_on": ["postgresql"],
            "networks": ["dagster_network"],
        },
        "dagster_webserver": {
            "image": "my-platform-base:latest",
            "command": "dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml",
            "environment": {"DAGSTER_HOME": "/opt/dagster/dagster_home"},
            "env_file": ".env.local",
            "volumes": [
                "./workspace.generated.yaml:/opt/dagster/app/workspace.yaml",
                "./dagster.yaml:/opt/dagster/dagster_home/dagster.yaml",
                "./data:/opt/dagster/app/data",
            ],
            "ports": ["3000:3000"],
            "depends_on": ["postgresql", "dagster_daemon"],
            "networks": ["dagster_network"],
        },
    },
    "networks": {"dagster_network": {"driver": "bridge"}},
    "volumes": {"dagster_pgdata_dev": None},
}

workspace_entries = {"load_from": []}

if not os.path.exists(PIPELINE_DIR):
    os.makedirs(PIPELINE_DIR)

# Get all projects, excluding _template, _shared, __pycache__
projects = [
    d
    for d in os.listdir(PIPELINE_DIR)
    if os.path.isdir(os.path.join(PIPELINE_DIR, d)) and not d.startswith("_")
]

# Collect all pipeline service names first
pipeline_services = [f"pipeline_{project}" for project in projects]

for project in projects:
    service_name = f"pipeline_{project}"

    # Production Docker Compose Entry
    compose_prod["services"][service_name] = {
        "build": {"context": f"{PIPELINE_DIR}", "dockerfile": f"{project}/Dockerfile"},
        "env_file": ".env",
        "expose": ["4000"],
        "networks": ["dagster_network"],
        "restart": "unless-stopped",
        "ulimits": {"nofile": {"soft": 65536, "hard": 65536}},
    }

    # Dev Docker Compose Entry (with volume mounts for hot reload)
    compose_dev["services"][service_name] = {
        "build": {"context": f"{PIPELINE_DIR}", "dockerfile": f"{project}/Dockerfile"},
        "env_file": ".env.local",
        "volumes": [
            f"./pipelines/{project}/assets:/opt/dagster/app/assets:ro",
            f"./pipelines/{project}/defs.py:/opt/dagster/app/defs.py:ro",
            "./pipelines/_shared:/opt/dagster/app/_shared:ro",
            "./data:/opt/dagster/app/data",
        ],
        # Use watchfiles to auto-restart grpc server on code changes
        "command": [
            "watchfiles",
            "--filter",
            "python",
            "uv run dagster api grpc -h 0.0.0.0 -p 4000 -f defs.py",
            "./assets",
            "./_shared",
            "./defs.py",
        ],
        "expose": ["4000"],
        "networks": ["dagster_network"],
        "depends_on": ["postgresql"],
        "healthcheck": {
            "test": [
                "CMD",
                "python",
                "-c",
                "import socket; s=socket.socket(); s.connect(('localhost', 4000)); s.close()",
            ],
            "interval": "5s",
            "timeout": "3s",
            "retries": 10,
            "start_period": "5s",
        },
    }

    # Workspace Entry
    workspace_entries["load_from"].append(
        {"grpc_server": {"host": service_name, "port": 4000, "location_name": project}}
    )

# Add pipeline services as dependencies for webserver and daemon
# Using condition: service_healthy ensures gRPC servers are ready before connecting
if pipeline_services:
    # Convert simple depends_on list to dict format with conditions
    webserver_deps = {
        dep: {"condition": "service_started"}
        for dep in compose_dev["services"]["dagster_webserver"]["depends_on"]
    }
    daemon_deps = {
        dep: {"condition": "service_started"}
        for dep in compose_dev["services"]["dagster_daemon"]["depends_on"]
    }

    # Add pipeline services with service_healthy condition
    for svc in pipeline_services:
        webserver_deps[svc] = {"condition": "service_healthy"}
        daemon_deps[svc] = {"condition": "service_healthy"}

    compose_dev["services"]["dagster_webserver"]["depends_on"] = webserver_deps
    compose_dev["services"]["dagster_daemon"]["depends_on"] = daemon_deps

# Write production compose override
with open(COMPOSE_PROD_FILE, "w") as f:
    f.write("# AUTO-GENERATED - DO NOT EDIT\n")
    yaml.dump(compose_prod, f, default_flow_style=False)

# Write dev compose
with open(COMPOSE_DEV_FILE, "w") as f:
    f.write("# AUTO-GENERATED - DO NOT EDIT\n")
    f.write("# Development environment with volume mounts for hot reload\n")
    f.write(
        "# Usage: docker compose -f docker-compose.dev.generated.yaml up --build\n\n"
    )
    yaml.dump(compose_dev, f, default_flow_style=False, sort_keys=False)

# Write workspace config
with open(WORKSPACE_FILE, "w") as f:
    f.write("# AUTO-GENERATED - DO NOT EDIT\n")
    if workspace_entries["load_from"]:
        yaml.dump(workspace_entries, f, default_flow_style=False)
    else:
        f.write("load_from: []\n")

print(f"Generated config for {len(projects)} pipeline(s).")
