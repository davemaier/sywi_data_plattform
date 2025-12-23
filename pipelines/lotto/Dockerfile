FROM my-platform-base:latest

# Copy shared code first
COPY _shared/ /opt/dagster/app/_shared/

# Copy dependency files (for caching)
# NOTE: Replace 'lotto' with your pipeline name
COPY lotto/pyproject.toml lotto/uv.lock* ./

# Install Python dependencies
RUN uv sync --frozen || uv sync

# Copy the pipeline code
# NOTE: Replace 'lotto' with your pipeline name
COPY lotto/assets/ ./assets/
COPY lotto/defs.py ./

# Run the Dagster gRPC server
CMD ["uv", "run", "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "defs.py"]
