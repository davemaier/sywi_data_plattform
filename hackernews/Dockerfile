# Use Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:0.9.26-python3.12-bookworm-slim

WORKDIR /opt/dagster/app
ENV UV_PROJECT_ENVIRONMENT=/usr/local
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1

# Install dependencies (cached layer)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Install sywi-core
COPY sywi-core/ /opt/dagster/sywi-core/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system /opt/dagster/sywi-core/

# Copy project files
COPY hackernews/ /opt/dagster/app/

# Install the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "hackernews.definitions"]
