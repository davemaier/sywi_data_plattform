"""
SYWI DuckDB - DuckDB wrapper with auto-configured SYWI DuckLake connections.

This module is a drop-in replacement for duckdb that automatically loads
environment configuration and attaches local and remote DuckLake databases.

Example:
    >>> import sywi_duckdb as duckdb
    >>> conn = duckdb.connect()
    >>> conn.execute("SELECT * FROM local.my_table").fetchdf()
    >>> conn.execute("SELECT * FROM remote.my_table").fetchdf()
"""

from __future__ import annotations

from typing import Any

import duckdb as _duckdb

from .connection import ConnectionError, connect

__all__ = ["connect", "ConnectionError"]


def __getattr__(name: str) -> Any:
    """Proxy all other attributes to the duckdb module."""
    return getattr(_duckdb, name)
