"""Shared DuckLake catalog: attach-or-create, and inspection of which
release schemas have already been materialized locally.

Each Open Targets release gets its own DuckLake schema inside a single
local DuckLake catalog (see PRD §5/§6): the catalog's metadata is stored
in a plain DuckDB file (`catalog.duckdb`), attached via the `ducklake`
extension as a database aliased `"lake"`. This module only knows how to
attach that catalog and enumerate the schemas already present in it;
building the schemas themselves (per release, from croissant.json, one
DuckLake table per dataset) lives in `schema_builder.py`.

DuckLake's metadata file has the same single-writer-lock semantics as a
plain DuckDB file (verified empirically: cross-process attach contention
raises `duckdb.IOException`, exactly like plain `duckdb.connect`), so two
concurrent `otai` invocations (e.g. parallel Claude Code subagents)
against the same catalog would otherwise fail nondeterministically. Two
mitigations, both used by `commands.py`:
- `try_connect_readonly` lets a caller peek at already-built schemas
  without taking the exclusive lock at all - multiple read-only
  connections coexist freely, so callers that find what they need already
  cached never need to fight over the write lock.
- `connect_catalog` (read-write, needed to build a schema) retries briefly
  on lock contention rather than failing on the first collision.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
from loguru import logger

CATALOG_FILENAME = "catalog.duckdb"
DATA_SUBDIR = "ducklake_data"
LAKE_ALIAS = "lake"

# The schema DuckLake creates by default inside every lake; never a
# "cached release" (Open Targets release names never collide with it).
BUILTIN_SCHEMAS = {"main"}

# A full from-scratch schema build (one CREATE TABLE + ducklake_add_data_files
# per dataset, each resolving a glob against real S3) measured ~18s for a
# 55-dataset release - the retry budget must comfortably outlast a
# concurrent writer doing that, not just a quick metadata write, or "retry"
# degrades back to "usually still fails" under real contention.
LOCK_RETRY_ATTEMPTS = 15
LOCK_RETRY_DELAY_SECONDS = 3.0


def get_catalog_path(cache_dir: Path) -> Path:
    """Return the predefined path of the shared DuckLake metadata file."""
    return Path(cache_dir) / CATALOG_FILENAME


def get_data_path(cache_dir: Path) -> Path:
    """Return the predefined DuckLake DATA_PATH directory.

    Never actually populated in practice: every registered file is added
    via `ducklake_add_data_files` (schema_builder.py), which references
    files in place rather than copying them here. DuckLake still requires
    a DATA_PATH be given on the first ATTACH that creates the catalog
    (verified: it does not need to exist yet - DuckLake records the path
    without creating the directory), so this exists as that formality.
    """
    return Path(cache_dir) / DATA_SUBDIR


def _new_lake_connection(
    catalog_path: Path, data_path: Path
) -> duckdb.DuckDBPyConnection:
    """Open a fresh in-memory connection and attach the lake read-write.

    A separate function (rather than inlining this in `connect_catalog`)
    so tests can patch exactly this seam to simulate lock contention -
    `duckdb.connect()` itself never fails, only the subsequent `ATTACH`
    can raise `duckdb.IOException` when another process holds the file.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    # catalog_path/data_path are derived from cache_dir, not user input;
    # DuckDB has no parameterized-identifier syntax for ATTACH.
    conn.execute(
        f"ATTACH 'ducklake:{catalog_path}' AS {LAKE_ALIAS} (DATA_PATH '{data_path}/')"
    )
    return conn


def _new_readonly_lake_connection(catalog_path: Path) -> duckdb.DuckDBPyConnection:
    """Open a fresh in-memory connection and attach the lake read-only.

    DATA_PATH doesn't need to be given again on a read-only attach to an
    existing catalog - it's already stored in the catalog's own metadata
    from the first (read-write) attach that created it.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    conn.execute(f"ATTACH 'ducklake:{catalog_path}' AS {LAKE_ALIAS} (READ_ONLY)")
    return conn


def connect_catalog(cache_dir: Path) -> duckdb.DuckDBPyConnection:
    """Attach the shared DuckLake catalog read-write, creating it if absent.

    Retries briefly on lock contention (`duckdb.IOException`) from a
    concurrent writer before giving up, since most contention resolves
    within one short-lived CLI invocation's lifetime of another.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = get_catalog_path(cache_dir)
    data_path = get_data_path(cache_dir)
    last_exc: duckdb.IOException | None = None
    for attempt in range(LOCK_RETRY_ATTEMPTS):
        try:
            return _new_lake_connection(catalog_path, data_path)
        except duckdb.IOException as exc:  # noqa: PERF203 - retry needs try/except per iteration
            last_exc = exc
            if attempt < LOCK_RETRY_ATTEMPTS - 1:
                logger.warning(
                    f"Catalog locked by another otai process, retrying "
                    f"({attempt + 1}/{LOCK_RETRY_ATTEMPTS})..."
                )
                time.sleep(LOCK_RETRY_DELAY_SECONDS)
    logger.error(
        f"Catalog still locked after {LOCK_RETRY_ATTEMPTS} attempts; giving up"
    )
    raise last_exc


def try_connect_readonly(cache_dir: Path) -> duckdb.DuckDBPyConnection | None:
    """Best-effort read-only connection; `None` if there's nothing to read yet
    or a concurrent writer currently holds the lock.

    Returning `None` in the lock-contention case (rather than retrying) is
    intentional: callers use this purely to avoid taking the write lock
    when possible, and fall back to `connect_catalog` (which does retry)
    when they actually need to build something.
    """
    catalog_path = get_catalog_path(Path(cache_dir))
    if not catalog_path.exists():
        return None
    try:
        return _new_readonly_lake_connection(catalog_path)
    except duckdb.IOException:
        return None


def list_cached_schemas(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """List release schemas already present in the lake (built-ins excluded)."""
    rows = conn.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?",
        [LAKE_ALIAS],
    ).fetchall()
    return sorted({row[0] for row in rows} - BUILTIN_SCHEMAS)
