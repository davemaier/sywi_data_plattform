#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE ducklake_catalog;
    GRANT ALL PRIVILEGES ON DATABASE ducklake_catalog TO $POSTGRES_USER;
EOSQL
