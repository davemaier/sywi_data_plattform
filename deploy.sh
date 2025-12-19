#!/bin/bash
set -e

cd /home/sywi/dagster-platform

echo "[1/5] Pulling latest code..."
git pull origin main

echo "[2/5] Building base image..."
docker build -t my-platform-base:latest -f Dockerfile.base .

echo "[3/5] Generating configs..."
python3 generate_platform.py

echo "[4/5] Starting/updating containers..."
docker compose -f docker-compose.base.yaml -f docker-compose.prod.generated.yaml up --build -d --remove-orphans

echo "[5/5] Reloading webserver..."
docker compose -f docker-compose.base.yaml -f docker-compose.prod.generated.yaml restart dagster_webserver || true

echo "Deployment complete!"
