"""SYWI Core - Shared DuckLake utilities for the SYWI data platform."""

from .ducklake_io_manager import DuckLakeIOManager, ducklake_io_manager
from .ducklake_resource import DuckLakeResource

__all__ = ["DuckLakeIOManager", "ducklake_io_manager", "DuckLakeResource"]
