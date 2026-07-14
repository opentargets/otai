# DuckLake Catalog Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `otai`'s plain-DuckDB `CREATE VIEW`-per-dataset catalog with a local DuckLake-backed catalog, in place, so schema attach avoids re-expanding each dataset's S3 glob on every `run-sql` call.

**Architecture:** `catalog.py` attaches a DuckLake catalog (`ducklake:<cache_dir>/catalog.duckdb`, metadata stored as a plain DuckDB file) as a database aliased `lake`, with one DuckLake schema per release, exactly mirroring today's one-DuckDB-schema-per-release layout. `schema_builder.py` resolves each dataset's glob once, introspects columns from a real file, creates a DuckLake table, and registers every resolved file onto it via `ducklake_add_data_files` (no data copy). `commands.py` needs one line changed (`search_path` now needs the `lake.` catalog qualifier).

**Tech Stack:** Python 3.10+, DuckDB (Python package already a dependency), the `ducklake` DuckDB extension (installed via `INSTALL ducklake` at runtime — no new Python/pip dependency), pytest.

This plan corrects three assumptions from the spec (`docs/superpowers/specs/2026-07-14-ducklake-catalog-backend-design.md`) that turned out to be wrong once verified empirically against a real `ducklake` extension (DuckDB 1.5.4, `ducklake` 1.0, verified during planning — see inline notes in Tasks 1–3):
- `commands.py` **does** need a change (the spec's "no structural changes needed" claim was wrong): `run_sql`'s `SET search_path` needs the `lake.` catalog prefix.
- `ducklake_add_data_files`'s second argument is a **bare table name resolved via `search_path`**, not a schema-qualified string — schema-qualified strings fail with a `Catalog Error`.
- Cross-catalog schema-qualified references (`"26.03".target`) resolve correctly with **no catalog prefix needed**, because DuckDB searches schema names across all attached catalogs when unambiguous — so `sql_guard.py` needs zero changes for cross-release joins.

## Global Constraints

- Python `>=3.10,<3.14` (pyproject.toml) — no new pip dependency; `ducklake` is a DuckDB extension, loaded via `INSTALL`/`LOAD`, not a package.
- Ruff lint (`make lint`) applies to all new/changed code, including the `S608` bandit rule on f-string SQL — every identifier-interpolating f-string passed to `conn.execute` needs a `# noqa: S608` comment plus a one-line trust justification, matching the existing convention in `schema_builder.py`.
- Tests must stay fully offline at run time (PRD §10, CLAUDE.md) — the `ducklake` extension is pre-installed once via `make dev`/CI (Task 4), never fetched mid-test-run.
- Never call DuckLake's compaction/cleanup functions (`merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files`) anywhere in `otai` — registered files are Open Targets' public S3 objects, not otai's to own or delete.
- This is a hard cutover: no dual backend, no `OTAI_CATALOG_BACKEND` toggle.

---

### Task 1: Rewrite `catalog.py` for DuckLake attach

**Files:**
- Modify: `src/otai/catalog.py`
- Test: `tests/test_catalog.py`

**Interfaces:**
- Produces: `catalog.LAKE_ALIAS: str` (the constant `"lake"` — the database alias every attach uses), `catalog.get_catalog_path(cache_dir) -> Path` (unchanged signature), `catalog.get_data_path(cache_dir) -> Path` (new), `catalog.connect_catalog(cache_dir) -> duckdb.DuckDBPyConnection` (unchanged signature/behavior), `catalog.try_connect_readonly(cache_dir) -> duckdb.DuckDBPyConnection | None` (unchanged signature/behavior), `catalog.list_cached_schemas(conn) -> list[str]` (unchanged signature/behavior). Tasks 2 and 3 import `LAKE_ALIAS` and call these functions exactly as before.

- [ ] **Step 1: Replace `tests/test_catalog.py` with DuckLake-aware assertions**

```python
from unittest.mock import patch

import duckdb
import pytest

from otai import catalog


def test_catalog_path_is_under_cache_dir(tmp_path):
    assert catalog.get_catalog_path(tmp_path) == tmp_path / "catalog.duckdb"


def test_connect_creates_catalog_file_when_absent(tmp_path):
    catalog_path = catalog.get_catalog_path(tmp_path)
    assert not catalog_path.exists()

    conn = catalog.connect_catalog(tmp_path)
    try:
        assert catalog_path.exists()
    finally:
        conn.close()


def test_connect_reuses_existing_catalog_file(tmp_path):
    conn1 = catalog.connect_catalog(tmp_path)
    conn1.execute(f'CREATE SCHEMA {catalog.LAKE_ALIAS}."26.06"')
    conn1.close()

    conn2 = catalog.connect_catalog(tmp_path)
    try:
        schemas = catalog.list_cached_schemas(conn2)
        assert "26.06" in schemas
    finally:
        conn2.close()


def test_list_cached_schemas_excludes_builtin_schemas(tmp_path):
    conn = catalog.connect_catalog(tmp_path)
    try:
        assert catalog.list_cached_schemas(conn) == []
    finally:
        conn.close()


def test_connect_creates_parent_cache_dir_if_missing(tmp_path):
    nested = tmp_path / "does" / "not" / "exist"
    assert not nested.exists()

    conn = catalog.connect_catalog(nested)
    try:
        assert nested.exists()
        assert (nested / "catalog.duckdb").exists()
    finally:
        conn.close()


def test_connect_catalog_retries_then_succeeds_on_lock_contention(tmp_path):
    real_new_lake_connection = catalog._new_lake_connection
    calls = []

    def flaky(catalog_path, data_path):
        calls.append(catalog_path)
        if len(calls) < 3:
            raise duckdb.IOException("Could not set lock on file (simulated)")
        return real_new_lake_connection(catalog_path, data_path)

    with (
        patch("otai.catalog._new_lake_connection", side_effect=flaky),
        patch("otai.catalog.time.sleep"),
    ):
        conn = catalog.connect_catalog(tmp_path)
    try:
        assert len(calls) == 3
    finally:
        conn.close()


def test_connect_catalog_raises_after_exhausting_retries(tmp_path):
    def always_locked(catalog_path, data_path):
        raise duckdb.IOException("Could not set lock on file (simulated)")

    with (
        patch("otai.catalog._new_lake_connection", side_effect=always_locked),
        patch("otai.catalog.time.sleep"),
        pytest.raises(duckdb.IOException),
    ):
        catalog.connect_catalog(tmp_path)


def test_try_connect_readonly_returns_none_when_catalog_does_not_exist(tmp_path):
    assert catalog.try_connect_readonly(tmp_path) is None


def test_try_connect_readonly_reads_existing_schemas(tmp_path):
    conn = catalog.connect_catalog(tmp_path)
    conn.execute(f'CREATE SCHEMA {catalog.LAKE_ALIAS}."26.06"')
    conn.close()

    ro_conn = catalog.try_connect_readonly(tmp_path)
    assert ro_conn is not None
    try:
        assert "26.06" in catalog.list_cached_schemas(ro_conn)
    finally:
        ro_conn.close()


def test_try_connect_readonly_returns_none_on_lock_contention(tmp_path):
    catalog.connect_catalog(tmp_path).close()  # ensure the file exists

    def always_locked(catalog_path):
        raise duckdb.IOException("Could not set lock on file (simulated)")

    with patch("otai.catalog._new_readonly_lake_connection", side_effect=always_locked):
        assert catalog.try_connect_readonly(tmp_path) is None
```

- [ ] **Step 2: Run the tests to confirm they fail against the old implementation**

Run: `uv run pytest tests/test_catalog.py -v`
Expected: FAIL — `AttributeError: module 'otai.catalog' has no attribute 'LAKE_ALIAS'` (and similar for `_new_lake_connection`/`_new_readonly_lake_connection`), since the old `catalog.py` doesn't define them yet.

- [ ] **Step 3: Rewrite `src/otai/catalog.py`**

```python
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
        f"ATTACH 'ducklake:{catalog_path}' AS {LAKE_ALIAS} "  # noqa: S608
        f"(DATA_PATH '{data_path}/')"
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
    conn.execute(
        f"ATTACH 'ducklake:{catalog_path}' AS {LAKE_ALIAS} (READ_ONLY)"  # noqa: S608
    )
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
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `uv run pytest tests/test_catalog.py -v`
Expected: PASS (all 11 tests)

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/otai/catalog.py tests/test_catalog.py && uv run ruff format --check src/otai/catalog.py tests/test_catalog.py`
Expected: no findings. If `ruff format` wants changes, run `uv run ruff format src/otai/catalog.py tests/test_catalog.py` and re-check.

- [ ] **Step 6: Commit**

```bash
git add src/otai/catalog.py tests/test_catalog.py
git commit -m "feat: back the shared catalog with a local DuckLake attach"
```

---

### Task 2: Rewrite `schema_builder.py` for DuckLake table registration + anonymous S3 secret

**Files:**
- Modify: `src/otai/schema_builder.py`
- Test: `tests/test_schema_builder.py`

**Interfaces:**
- Consumes: `catalog.LAKE_ALIAS` (Task 1).
- Produces: `schema_builder.build_release_schema(conn, release, datasets, base_uri=DEFAULT_BASE_URI) -> None` (unchanged signature), `schema_builder.S3_ANONYMOUS_SECRET_NAME: str`, `schema_builder._ensure_s3_access(conn, base_uri) -> None`, `schema_builder._s3_scope(base_uri) -> str`. Task 3's tests call `build_release_schema` exactly as before; no other task calls the new helpers directly.

- [ ] **Step 1: Replace `tests/test_schema_builder.py`**

```python
import duckdb
import pytest

from otai import catalog, schema_builder
from otai.croissant import DatasetInfo


def _datasets_for(dataset_rows):
    return [
        DatasetInfo(
            name=name, description=f"{name} dataset", file_glob=f"{name}/*.parquet"
        )
        for name in dataset_rows
    ]


def test_build_release_schema_creates_schema_and_queryable_tables(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder.build_release_schema(
            conn, release, _datasets_for(dataset_rows), base_uri=base_uri
        )

        assert release in catalog.list_cached_schemas(conn)

        target_rows = conn.execute(
            f'SELECT * FROM {catalog.LAKE_ALIAS}."{release}".target ORDER BY id'
        ).fetchall()
        assert len(target_rows) == len(dataset_rows["target"])
        assert target_rows[0][0] == "ENSG00000141510"  # TP53 sorts before BRAF's id
    finally:
        conn.close()


def test_build_release_schema_creates_one_table_per_dataset(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder.build_release_schema(
            conn, release, _datasets_for(dataset_rows), base_uri=base_uri
        )

        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = ? AND table_schema = ?",
                [catalog.LAKE_ALIAS, release],
            ).fetchall()
        }
        assert tables == set(dataset_rows)
    finally:
        conn.close()


def test_failed_table_creation_rolls_back_the_whole_schema(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    datasets = _datasets_for(dataset_rows)
    datasets.append(
        DatasetInfo(
            name="broken",
            description="dataset whose glob matches nothing",
            file_glob="no_such_dataset/*.parquet",
        )
    )
    conn = catalog.connect_catalog(tmp_path)
    try:
        with pytest.raises(duckdb.Error):
            schema_builder.build_release_schema(
                conn, release, datasets, base_uri=base_uri
            )

        assert release not in catalog.list_cached_schemas(conn), (
            "a mid-build failure must roll back CREATE SCHEMA too, otherwise "
            "list_cached_schemas would mistake the partial schema for already-built"
        )
    finally:
        conn.close()


def test_build_release_schema_reads_real_parquet_content(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder.build_release_schema(
            conn, release, _datasets_for(dataset_rows), base_uri=base_uri
        )

        association_rows = conn.execute(
            "SELECT targetId, diseaseId, CAST(score AS DOUBLE) FROM "
            f'{catalog.LAKE_ALIAS}."{release}".association_by_datasource_direct '
            "ORDER BY score DESC"
        ).fetchall()
        assert association_rows == [
            ("ENSG00000157764", "EFO_0000305", 0.8),
            ("ENSG00000141510", "EFO_0000616", 0.5),
        ]
    finally:
        conn.close()


def test_ensure_s3_access_creates_anonymous_secret_for_real_s3_base_uri(tmp_path):
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder._ensure_s3_access(
            conn, "s3://open-targets-public-data-releases/platform"
        )
        secrets = conn.execute(
            "SELECT name, provider, scope FROM duckdb_secrets() WHERE name = ?",
            [schema_builder.S3_ANONYMOUS_SECRET_NAME],
        ).fetchall()
        assert secrets == [
            (
                schema_builder.S3_ANONYMOUS_SECRET_NAME,
                "config",
                ["s3://open-targets-public-data-releases"],
            )
        ]
    finally:
        conn.close()


def test_ensure_s3_access_is_noop_for_local_file_uri(tmp_path):
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder._ensure_s3_access(conn, f"file://{tmp_path}")
        secrets = conn.execute("SELECT name FROM duckdb_secrets()").fetchall()
        assert secrets == []
    finally:
        conn.close()
```

- [ ] **Step 2: Run the tests to confirm they fail against the old implementation**

Run: `uv run pytest tests/test_schema_builder.py -v`
Expected: FAIL — `AttributeError: module 'otai.schema_builder' has no attribute '_ensure_s3_access'` (and the table/schema-content tests fail with `duckdb.CatalogException` since the old code still creates plain views in the default catalog, not `lake."<release>"` tables).

- [ ] **Step 3: Rewrite `src/otai/schema_builder.py`**

```python
"""Lazy DuckLake schema/table construction for a single release.

Implements PRD §6 step 3b of the implicit initialization pipeline: given a
release's parsed croissant dataset list, create that release's DuckLake
schema (inside the "lake" catalog attached by catalog.py) and one table
per dataset, registering the dataset's existing parquet files onto it via
`ducklake_add_data_files` - no data is copied or rewritten, DuckLake only
records metadata about files that stay exactly where they are.

The base URI is injectable (defaults to the real S3 bucket in production)
so tests can point it at a local directory of fixture parquet files via a
file:// URI, exercising DuckDB's real read_parquet()/glob() unmodified
against fixtures rather than mocking DuckDB itself (PRD §10).

Never call DuckLake's compaction or cleanup functions
(`merge_adjacent_files`, `expire_snapshots`/`cleanup_old_files`) anywhere
in this module or its callers: registering a file via
`ducklake_add_data_files` transfers "ownership" of it to DuckLake, and
those operations can delete registered files - but the files in question
are Open Targets' public S3 objects, not otai's to own or delete.
"""

from __future__ import annotations

import sys

import duckdb
from loguru import logger
from tqdm import tqdm

from otai.catalog import LAKE_ALIAS
from otai.config import DEFAULT_BASE_URI
from otai.croissant import DatasetInfo

__all__ = ["DEFAULT_BASE_URI", "build_release_schema"]

S3_ANONYMOUS_SECRET_NAME = "otai_s3_anonymous"


def _s3_scope(base_uri: str) -> str:
    """Return the `s3://<bucket>` scope for an `s3://<bucket>/<prefix...>` URI."""
    bucket = base_uri.removeprefix("s3://").split("/", 1)[0]
    return f"s3://{bucket}"


def _ensure_s3_access(conn: duckdb.DuckDBPyConnection, base_uri: str) -> None:
    """Install/load `httpfs` and force anonymous S3 access when `base_uri`
    is a real S3 URL (PRD §3).

    Explicit rather than relying on DuckDB's default `credential_chain`
    secret provider, which searches environment variables, the shared AWS
    config/credentials files, SSO, and instance metadata - if the caller's
    shell happens to have `AWS_PROFILE` or `~/.aws/credentials` set for
    unrelated work, DuckDB could otherwise sign requests with that real
    identity instead of reading this public bucket anonymously.
    `PROVIDER config` with no `KEY_ID`/`SECRET` never consults any of
    that - only what's explicitly given here, which is nothing.

    Gated on scheme so tests pointing `base_uri` at a local `file://`
    fixture (PRD §10) never trigger a network call for the extension or
    secret setup.
    """
    if base_uri.startswith("s3://"):
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        scope = _s3_scope(base_uri)
        # scope is derived from base_uri, not user input; DuckDB has no
        # parameterized-identifier syntax for CREATE SECRET.
        conn.execute(
            f"CREATE OR REPLACE SECRET {S3_ANONYMOUS_SECRET_NAME} "  # noqa: S608
            f"(TYPE s3, PROVIDER config, SCOPE '{scope}')"
        )


def build_release_schema(
    conn: duckdb.DuckDBPyConnection,
    release: str,
    datasets: list[DatasetInfo],
    base_uri: str = DEFAULT_BASE_URI,
) -> None:
    """Create `lake."<release>"` schema plus one table per dataset (PRD §6).

    For each dataset: resolves the glob to an explicit file list (one S3
    `LIST` call via `glob()`), introspects the first resolved file's
    columns via `DESCRIBE`, creates a table with those exact columns, then
    registers every resolved file onto it via `ducklake_add_data_files` -
    which reads only each file's footer to register it, without copying or
    rewriting any data. Assumes the release schema does not already exist;
    callers are responsible for the exists-check (see
    commands._ensure_release_schema).

    Runs as a single transaction: DuckLake DDL and `ducklake_add_data_files`
    are transactional, so a mid-loop failure (e.g. one dataset's glob
    resolving to nothing) rolls back the `CREATE SCHEMA` too, instead of
    leaving a partially-built schema that `list_cached_schemas` would then
    mistake for "already initialized" on the next call.

    Each dataset's glob resolves against real S3 (or a local fixture in
    tests), so building a full release can take a while (measured ~18s for
    a 55-dataset release) - a tqdm progress bar (stderr, never stdout -
    stdout is reserved for the JSON envelope) gives visible feedback for
    that wait.
    """
    _ensure_s3_access(conn, base_uri)
    logger.info(f"Building schema for release {release!r} ({len(datasets)} datasets)")
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f'CREATE SCHEMA {LAKE_ALIAS}."{release}"')
        # search_path lets every dataset's CREATE TABLE / ducklake_add_data_files
        # below use a bare table name - ducklake_add_data_files specifically
        # requires this: a schema-qualified string in its table argument
        # fails with a Catalog Error, but a bare name resolved via
        # search_path works.
        conn.execute("SET search_path = ?", [f'"{LAKE_ALIAS}"."{release}"'])
        progress = tqdm(
            datasets,
            desc=f'Building "{release}"',
            unit="dataset",
            file=sys.stderr,
        )
        for dataset in progress:
            glob_url = f"{base_uri}/{release}/output/{dataset.file_glob}"
            # release/dataset names and glob_url come from trusted
            # S3/croissant data, not user input; DuckDB has no
            # parameterized-identifier syntax to use instead.
            files = [
                row[0]
                for row in conn.execute(
                    f"SELECT file FROM glob('{glob_url}') ORDER BY file"  # noqa: S608
                ).fetchall()
            ]
            if not files:
                raise duckdb.Error(
                    f"No files matched glob for dataset {dataset.name!r}: "
                    f"{glob_url!r}"
                )
            columns = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{files[0]}')"  # noqa: S608
            ).fetchall()
            column_defs = ", ".join(f'"{col[0]}" {col[1]}' for col in columns)
            conn.execute(f'CREATE TABLE "{dataset.name}" ({column_defs})')  # noqa: S608
            for file_path in files:
                conn.execute(
                    "CALL ducklake_add_data_files(?, ?, ?)",
                    [LAKE_ALIAS, dataset.name, file_path],
                )
            logger.debug(
                f'Registered table "{release}"."{dataset.name}" '
                f"({len(files)} file(s))"
            )
    except Exception:
        conn.execute("ROLLBACK")
        logger.exception(f"Failed to build schema for release {release!r}; rolled back")
        raise
    else:
        conn.execute("COMMIT")
        logger.success(f"Built schema for release {release!r}")
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `uv run pytest tests/test_schema_builder.py -v`
Expected: PASS (all 6 tests)

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/otai/schema_builder.py tests/test_schema_builder.py && uv run ruff format --check src/otai/schema_builder.py tests/test_schema_builder.py`
Expected: no findings (fix with `uv run ruff format` if formatting-only).

- [ ] **Step 6: Commit**

```bash
git add src/otai/schema_builder.py tests/test_schema_builder.py
git commit -m "feat: build release schemas as DuckLake tables instead of parquet views"
```

---

### Task 3: Fix `commands.py`'s `search_path` and update catalog-manipulating tests

**Files:**
- Modify: `src/otai/commands.py:326`
- Modify: `tests/test_commands.py` (lines ~518-538, ~540-577, ~740-778 in the pre-change file — the `_build_schema_and_add_view` helper's call sites and the cross-release guardrail test)
- Modify: `tests/test_cli.py` (lines ~214-234 and ~356-382 in the pre-change file)

**Interfaces:**
- Consumes: `catalog.LAKE_ALIAS` (Task 1), `catalog.list_cached_schemas` (Task 1).
- Produces: no new interface — this task makes `commands.run_sql` work correctly against the Task 1/2 catalog, and brings the test suite in line.

This task has no isolated "write a failing unit test" step of its own, since the single line change in `commands.py` is exercised by the existing `run-sql` integration tests in `test_commands.py`/`test_cli.py` — those tests currently fail for a *different* reason (they still poke the catalog directly with pre-DuckLake SQL), so this task fixes both at once and treats "the full suite passes" as the pass/fail signal.

- [ ] **Step 1: Fix `commands.py`'s search_path**

In `src/otai/commands.py`, `run_sql`, find:

```python
        conn.execute("SET search_path = ?", [f'"{release}"'])
```

Replace with:

```python
        conn.execute("SET search_path = ?", [f'"{catalog.LAKE_ALIAS}"."{release}"'])
```

(`catalog` is already imported at the top of `commands.py`.)

- [ ] **Step 2: Run the run-sql tests to see the current failure mode**

Run: `uv run pytest tests/test_commands.py::TestRunSql -v`
Expected: some tests now PASS (e.g. `test_executes_against_latest_via_search_path`, which only touches real parquet-backed tables via `list_datasets`/`run_sql` and never pokes the catalog directly), but `test_row_cap_truncates_large_result`, `test_slow_query_times_out`, and the two-release row-cap test FAIL with a `duckdb.CatalogException` (e.g. "Table with name big does not exist") — because their helper still issues `CREATE VIEW "{release}".big ...` without the `lake.` prefix, landing in the wrong (default) catalog instead of the lake.

- [ ] **Step 3: Update `tests/test_commands.py`'s catalog-poking call sites**

In `_build_schema_and_add_view` (used by the row-cap/timeout tests), and its three call sites, change every `f'CREATE VIEW "{release}".<name> AS ...'` / `f'CREATE VIEW "{other}".<name> AS ...'` string to prefix the lake catalog. The helper itself is unchanged:

```python
    def _build_schema_and_add_view(self, tmp_path, base_uri, release, view_sql):
        # range() (and any table-valued function) is no longer allowed as a
        # guarded query's data source (sql_guard now allowlists plain
        # table/view names only - see test_sql_guard.py for that fix's
        # rationale). To still exercise real row-cap/timeout behavior, add
        # a view wrapping range() directly on the catalog, exactly as
        # schema_builder itself wraps read_parquet() in a view - the
        # guarded query then only ever sees a plain view name.
        commands.list_datasets(
            tmp_path,
            release=release,
            fetch_xml=self._fetch_xml(),
            fetch_croissant=self._fetch_croissant(),
            base_uri=base_uri,
            now=self.NOW,
        )
        conn = catalog.connect_catalog(tmp_path)
        try:
            conn.execute(view_sql)
        finally:
            conn.close()

    def test_row_cap_truncates_large_result(self, tmp_path, fixture_release_layout):
        base_uri, release, _dataset_rows = fixture_release_layout
        self._build_schema_and_add_view(
            tmp_path,
            base_uri,
            release,
            f'CREATE VIEW {catalog.LAKE_ALIAS}."{release}".big AS '
            "SELECT * FROM range(2500) AS t(n)",
        )

        result = commands.run_sql(
            tmp_path,
            "SELECT * FROM big",
            fetch_xml=self._fetch_xml(),
            fetch_croissant=self._fetch_croissant(),
            base_uri=base_uri,
            now=self.NOW,
            row_cap=1000,
        )

        assert result["ok"] is True
        assert result["data"]["row_count"] == 1000
        assert result["data"]["truncated"] is True

    def test_slow_query_times_out(self, tmp_path, fixture_release_layout):
        base_uri, release, _dataset_rows = fixture_release_layout
        self._build_schema_and_add_view(
            tmp_path,
            base_uri,
            release,
            f'CREATE VIEW {catalog.LAKE_ALIAS}."{release}".slow_a AS '
            "SELECT * FROM range(100000000)",
        )
        conn = catalog.connect_catalog(tmp_path)
        try:
            conn.execute(
                f'CREATE VIEW {catalog.LAKE_ALIAS}."{release}".slow_b AS '
                "SELECT * FROM range(100000)"
            )
        finally:
            conn.close()

        result = commands.run_sql(
            tmp_path,
            "SELECT count(*) FROM slow_a a, slow_b b",
            fetch_xml=self._fetch_xml(),
            fetch_croissant=self._fetch_croissant(),
            base_uri=base_uri,
            now=self.NOW,
            timeout_seconds=0.2,
        )

        assert result["ok"] is False
```

And in the cross-release guardrail test further down the file:

```python
        conn = catalog.connect_catalog(tmp_path)
        try:
            conn.execute(
                f'CREATE VIEW {catalog.LAKE_ALIAS}."{other}".big AS '
                "SELECT * FROM range(2500) AS t(n)"
            )
        finally:
            conn.close()
```

(This is the only edit needed in that test — the surrounding `commands.run_sql` calls and assertions are unchanged, since schema-qualified cross-release references like `"{other}".target` resolve correctly with no catalog prefix, verified empirically during planning.)

- [ ] **Step 4: Update `tests/test_cli.py`'s catalog-poking call sites**

Replace the raw `duckdb.connect` + unfiltered `information_schema.schemata` check:

```python
def test_list_datasets_builds_catalog_schema_on_first_run(
    tmp_path, fixture_release_layout
):
    base_uri, release, _dataset_rows = fixture_release_layout
    cache_dir = tmp_path / "cache"

    _invoke_with_fixtures(["list-datasets"], cache_dir, base_uri)

    conn = catalog.connect_catalog(cache_dir)
    try:
        assert release in catalog.list_cached_schemas(conn)
    finally:
        conn.close()
```

(Drop the local `import duckdb` that was only used by the old version of this test — `catalog` is already imported at the top of the file.)

And prefix the lake catalog in the timeout end-to-end test:

```python
def test_run_sql_timeout_flag_actually_shortens_execution(
    tmp_path, fixture_release_layout
):
    # Real end-to-end confirmation (not mocked): a view wrapping range()
    # (created outside the guard, same pattern used in test_commands.py/
    # test_sql_guard.py) is cheap to set up but genuinely slow to query -
    # a short --timeout must cause it to time out for real.
    base_uri, release, _dataset_rows = fixture_release_layout
    cache_dir = tmp_path / "cache"

    _invoke_with_fixtures(["list-datasets"], cache_dir, base_uri)
    conn = catalog.connect_catalog(cache_dir)
    try:
        conn.execute(
            f'CREATE VIEW {catalog.LAKE_ALIAS}."{release}".slow_a AS '
            "SELECT * FROM range(100000000)"
        )
        conn.execute(
            f'CREATE VIEW {catalog.LAKE_ALIAS}."{release}".slow_b AS '
            "SELECT * FROM range(100000)"
        )
    finally:
        conn.close()

    result = _invoke_with_fixtures(
        ["run-sql", "SELECT count(*) FROM slow_a a, slow_b b", "--timeout", "0.2"],
        cache_dir,
        base_uri,
    )

    assert result.exit_code != 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
```

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -v`
Expected: PASS (all tests, including every test in `test_commands.py`, `test_cli.py`, `test_catalog.py`, `test_schema_builder.py`, `test_sql_guard.py` — `test_sql_guard.py` needed no changes at all, since it tests guardrail logic against a synthetic in-memory connection unrelated to the catalog).

- [ ] **Step 6: Lint**

Run: `uv run ruff check . && uv run ruff format --check . && uv run ty check`
Expected: no findings.

- [ ] **Step 7: Commit**

```bash
git add src/otai/commands.py tests/test_commands.py tests/test_cli.py
git commit -m "fix: qualify search_path with the lake catalog for DuckLake-backed schemas"
```

---

### Task 4: Pre-install the DuckLake extension so tests stay fully offline

**Files:**
- Modify: `Makefile`
- Modify: `.github/workflows/check.yaml`

**Interfaces:** none — this is environment setup, not code.

Unlike `httpfs` (only installed when `base_uri` starts with `s3://`, which tests never do), the `ducklake` extension is needed for *every* catalog attach, including fixture-backed tests using `file://` URIs — `INSTALL ducklake` performs a real network fetch the first time it runs in a given environment. To keep `make test`/CI runs offline (PRD §10, CLAUDE.md), the extension must be pre-installed once (cached in DuckDB's local extension directory) as part of environment setup, not fetched implicitly during a test run.

- [ ] **Step 1: Add the pre-install step to `make dev`**

In `Makefile`, change:

```makefile
dev: .git/hooks/pre-commit ## Install dev dependencies and pre-commit hook
	@uv sync --all-groups
	@echo "dev dependencies installed"
```

to:

```makefile
dev: .git/hooks/pre-commit ## Install dev dependencies and pre-commit hook
	@uv sync --all-groups
	@uv run python -c "import duckdb; duckdb.connect().execute('INSTALL ducklake')"
	@echo "dev dependencies installed"
```

- [ ] **Step 2: Verify it actually caches the extension locally**

Run: `make dev`
Expected: completes with no errors, ending in "dev dependencies installed". Then confirm no network call happens on a second run:

Run: `uv run python -c "import duckdb; duckdb.connect().execute('INSTALL ducklake'); print('ok')"`
Expected: prints `ok` quickly (no noticeable network delay — the extension is already cached).

- [ ] **Step 3: Add the same pre-install step to CI**

In `.github/workflows/check.yaml`, under the `test` job, add a step before "Run tests":

```yaml
      - name: Pre-install DuckLake extension
        run: uv run python -c "import duckdb; duckdb.connect().execute('INSTALL ducklake')"

      - name: Run tests
        run: uv run --frozen pytest -rxs
```

- [ ] **Step 4: Run the full suite once more to confirm nothing regressed**

Run: `uv run pytest -v`
Expected: PASS (all tests)

- [ ] **Step 5: Commit**

```bash
git add Makefile .github/workflows/check.yaml
git commit -m "chore: pre-install the DuckLake extension so tests stay offline"
```

---

### Task 5: Update `CLAUDE.md`'s architecture description

**Files:**
- Modify: `CLAUDE.md`

**Interfaces:** none — documentation only.

- [ ] **Step 1: Update the initialization-pipeline step 3 description**

Find this paragraph in `CLAUDE.md`:

```markdown
3. **Lazily materialize the DuckDB schema** — if the release's schema isn't
   already in the shared catalog file (`~/.cache/otai/catalog.duckdb`, one
   schema namespace per release), `schema_builder.build_release_schema()`
   creates it: `CREATE SCHEMA "<release>"` + one `CREATE VIEW` per dataset
   over `read_parquet(<base_uri>/<release>/output/<glob>)`.
```

Replace with:

```markdown
3. **Lazily materialize the DuckLake schema** — if the release's schema
   isn't already in the shared local DuckLake catalog
   (`~/.cache/otai/catalog.duckdb`, attached via the `ducklake` extension
   as database `lake`, one schema namespace per release),
   `schema_builder.build_release_schema()` creates it: `CREATE SCHEMA
   lake."<release>"` + one `CREATE TABLE` per dataset, with that dataset's
   resolved parquet files (`<base_uri>/<release>/output/<glob>`) registered
   onto it via `ducklake_add_data_files` — no data is copied, DuckLake only
   records metadata about files that stay exactly where they are on S3.
```

- [ ] **Step 2: Update the `schema_builder.py` module-map bullet**

Find:

```markdown
- `schema_builder.py` — DuckDB schema/view construction; takes an
  injectable `base_uri` (defaults to the real S3 bucket) so tests point it
  at local fixture parquet files via a `file://` URI instead. Shows a `tqdm`
  progress bar while building (a full release build measured ~18s for 55
  datasets) and logs via `loguru` — both on stderr, never stdout.
```

Replace with:

```markdown
- `schema_builder.py` — DuckLake schema/table construction; takes an
  injectable `base_uri` (defaults to the real S3 bucket) so tests point it
  at local fixture parquet files via a `file://` URI instead. Forces
  anonymous S3 access (an explicit `config`-provider secret, scoped to the
  bucket) rather than relying on DuckDB's default credential chain, so
  ambient AWS credentials in the caller's shell are never used. Shows a
  `tqdm` progress bar while building (a full release build measured ~18s
  for 55 datasets) and logs via `loguru` — both on stderr, never stdout.
```

- [ ] **Step 3: Update the `catalog.py` module-map bullet**

Find:

```markdown
- `catalog.py` — the shared DuckDB catalog file. `connect_catalog()`
```

Replace with:

```markdown
- `catalog.py` — the shared local DuckLake catalog. `connect_catalog()`
```

(leave the rest of that bullet's sentence about retry/read-only-peek behavior unchanged — it still applies verbatim.)

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: describe the DuckLake-backed catalog in CLAUDE.md"
```
