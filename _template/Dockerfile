# Use Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /opt/dagster/app
ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Install dependencies (cached layer)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Install sywi-core
COPY sywi-core/ /opt/dagster/sywi-core/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install /opt/dagster/sywi-core/

# Copy project files
COPY __name__/ /opt/dagster/app/

# Install the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "__name__.definitions"]
