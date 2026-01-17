# Migration Plan: Lean Local Development

## Goal

Replace Docker-heavy development workflow with native `dagster dev` while keeping:
- PostgreSQL in Docker (for DuckLake catalog)
- Docker-based production deployment unchanged
- Separate lock files per pipeline (isolated dependencies)
- Flat project structure (hooli-style)

## Target Folder Structure

```
sywi_data_plattform/
├── dg.toml                      # Dagster workspace config
├── dagster.yaml                 # Dagster instance config (prod)
├── docker-compose.yml           # PostgreSQL only (local dev)
├── docker-compose.prod.yaml     # Full production stack (generated)
├── generate_platform.py         # Generates prod compose (updated)
├── dev.py                       # Dev CLI (updated)
├── pyproject.toml               # Root project (dev CLI)
├── justfile                     # Task runner (replaces Makefile)
├── .env.local
├── .env.example
├── init-db.sh
├── data/                        # Local DuckLake parquet storage
│
├── sywi-core/                   # Shared DuckLake library
│   ├── pyproject.toml
│   ├── uv.lock
│   └── src/
│       └── sywi_core/
│           ├── __init__.py
│           ├── ducklake_io_manager.py
│           └── ducklake_resource.py
│
├── hackernews/                  # Pipeline project
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── Dockerfile
│   └── src/
│       └── hackernews/
│           ├── __init__.py
│           ├── definitions.py
│           └── assets/
│
├── patents/                     # Pipeline project
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── Dockerfile
│   └── src/
│       └── patents/
│           ├── __init__.py
│           ├── definitions.py
│           ├── assets/
│           └── resources/
│
├── _template/                   # Template for new pipelines
│   └── ...
│
└── libs/                        # Client libraries (renamed from packages/)
    ├── sywi_duckdb/
    └── sywi.duckdb/
```

## Migration Checklist

### Phase 1: Create sywi-core package
- [x] Create `sywi-core/pyproject.toml`
- [x] Create `sywi-core/src/sywi_core/__init__.py`
- [x] Copy `pipelines/_shared/*.py` to `sywi-core/src/sywi_core/`
- [x] Run `uv lock` in sywi-core/

### Phase 2: Migrate hackernews pipeline
- [x] Create `hackernews/` directory at root
- [x] Create `hackernews/src/hackernews/` structure
- [x] Move assets from `pipelines/hackernews/assets/`
- [x] Convert `defs.py` to `definitions.py` with updated imports
- [x] Create `hackernews/pyproject.toml` with `[tool.dg]`
- [x] Create `hackernews/Dockerfile` (new clean version)
- [x] Run `uv lock` in hackernews/

### Phase 3: Migrate patents pipeline
- [x] Create `patents/` directory at root
- [x] Create `patents/src/patents/` structure
- [x] Move assets and resources from `pipelines/patents/`
- [x] Convert `defs.py` to `definitions.py` with updated imports
- [x] Create `patents/pyproject.toml` with `[tool.dg]`
- [x] Create `patents/Dockerfile` (new clean version)
- [x] Run `uv lock` in patents/

### Phase 4: Create workspace config
- [x] Create `dg.toml` at root
- [ ] Test: `dagster dev` loads both projects

### Phase 5: Simplify Docker setup
- [x] Create `docker-compose.yml` (PostgreSQL only)
- [ ] Test: `docker compose up -d && dagster dev`

### Phase 6: Update dev.py
- [x] Rewrite `up` command for native dagster
- [x] Rewrite `down` command
- [x] Add `prod` command
- [x] Update `new` command for `_template/`
- [x] Update `mark` command for native Dagster instance
- [x] Remove `build` command

### Phase 7: Update generate_platform.py
- [x] Update paths (no `pipelines/` prefix)
- [x] Update Dockerfile build contexts
- [x] Only generate `docker-compose.prod.yaml`
- [x] Update module paths for CMD

### Phase 8: Create template
- [x] Create `_template/` structure
- [x] Include sywi-core dependency placeholder
- [x] Include DuckLake resource setup
- [x] Include Dockerfile template

### Phase 9: Rename packages to libs
- [x] Rename `packages/` to `libs/`

### Phase 10: Create justfile
- [x] Create `justfile` with all commands
- [x] Configure PowerShell for Windows

### Phase 11: Update root pyproject.toml
- [x] Add `dagster-webserver` for local dev
- [x] Update dependencies

### Phase 12: Cleanup
- [ ] Remove `pipelines/` folder
- [ ] Remove `Dockerfile.base`
- [ ] Remove `docker-compose.base.yaml`
- [ ] Remove `docker-compose.dev.generated.yaml`
- [ ] Remove `workspace.generated.yaml`
- [ ] Update README.md

### Phase 13: Final testing
- [ ] Test: `just dev` (native development)
- [ ] Test: `just prod` (Docker production)
- [ ] Test: `just new` (create new pipeline)
- [ ] Test: Docker builds work

## Files to Remove After Migration

| File | Reason |
|------|--------|
| `pipelines/` | Content moved to root level |
| `Dockerfile.base` | Each project is self-contained |
| `docker-compose.base.yaml` | Merged into simplified compose |
| `docker-compose.dev.generated.yaml` | No longer needed |
| `workspace.generated.yaml` | Replaced by `dg.toml` |

## New Development Workflow

```bash
# One-time setup
docker compose up -d          # Start PostgreSQL
just install-core             # Install shared library

# Daily development  
just dev                      # Start native Dagster
# Opens http://localhost:3000

# Create new pipeline
just new my-pipeline

# Test production build
just prod
```

## Benefits

| Aspect | Before | After |
|--------|--------|-------|
| Startup time | 30-60s (Docker builds) | 2-3s (native Python) |
| Memory | 2-4GB (PostgreSQL + N containers) | ~500MB (PostgreSQL + native) |
| Structure | Nested `pipelines/` folder | Flat, hooli-style |
| Dockerfiles | Custom base + per-pipeline | Clean, self-contained |
| Hot reload | watchfiles in containers | Native Python |
| New pipeline | Manual template copy | `just new <name>` |
