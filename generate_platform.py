#!/usr/bin/env python3
import os
import yaml

PIPELINE_DIR = "./pipelines"
COMPOSE_FILE = "docker-compose.override.generated.yaml"
WORKSPACE_FILE = "workspace.generated.yaml"

compose_services = {
    "services": {},
    "networks": {
        "dagster_network": {
            "external": True,
            "name": "dagster-platform_dagster_network",
        }
    },
}
workspace_entries = {"load_from": []}

if not os.path.exists(PIPELINE_DIR):
    os.makedirs(PIPELINE_DIR)

# Get all projects, excluding _template
projects = [
    d
    for d in os.listdir(PIPELINE_DIR)
    if os.path.isdir(os.path.join(PIPELINE_DIR, d)) and not d.startswith("_")
]

for project in projects:
    service_name = f"pipeline_{project}"

    # Docker Compose Entry
    compose_services["services"][service_name] = {
        "build": {"context": PIPELINE_DIR, "dockerfile": f"{project}/Dockerfile"},
        "env_file": ".env",
        "expose": ["4000"],
        "networks": ["dagster_network"],
        "restart": "unless-stopped",
    }

    # Workspace Entry
    workspace_entries["load_from"].append(
        {"grpc_server": {"host": service_name, "port": 4000, "location_name": project}}
    )

# Write compose override
with open(COMPOSE_FILE, "w") as f:
    f.write("# AUTO-GENERATED - DO NOT EDIT\n")
    yaml.dump(compose_services, f, default_flow_style=False)

# Write workspace config
with open(WORKSPACE_FILE, "w") as f:
    f.write("# AUTO-GENERATED - DO NOT EDIT\n")
    if workspace_entries["load_from"]:
        yaml.dump(workspace_entries, f, default_flow_style=False)
    else:
        # Empty workspace placeholder
        f.write("load_from: []\n")

print(f"Generated config for {len(projects)} pipeline(s).")
