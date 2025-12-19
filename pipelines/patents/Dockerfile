FROM my-platform-base:latest

# Copy shared code first
COPY _shared/ /opt/dagster/app/_shared/

# Copy dependency files (for caching)
COPY patents/pyproject.toml patents/uv.lock* ./

# Install Python dependencies
RUN uv sync --frozen || uv sync

# Copy the pipeline code
COPY patents/assets/ ./assets/
COPY patents/resources/ ./resources/
COPY patents/defs.py ./

# Run the Dagster gRPC server
CMD ["uv", "run", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "defs.py"]
