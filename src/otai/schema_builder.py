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

S3_ANONYMOUS_SECRET_NAME = "otai_s3_anonymous"  # noqa: S105


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
            f"CREATE OR REPLACE SECRET {S3_ANONYMOUS_SECRET_NAME} "
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
                raise duckdb.Error(  # noqa: TRY301
                    f"No files matched glob for dataset {dataset.name!r}: {glob_url!r}"
                )
            columns = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{files[0]}')"  # noqa: S608
            ).fetchall()
            column_defs = ", ".join(f'"{col[0]}" {col[1]}' for col in columns)
            conn.execute(f'CREATE TABLE "{dataset.name}" ({column_defs})')
            for file_path in files:
                conn.execute(
                    "CALL ducklake_add_data_files(?, ?, ?)",
                    [LAKE_ALIAS, dataset.name, file_path],
                )
            logger.debug(
                f'Registered table "{release}"."{dataset.name}" ({len(files)} file(s))'
            )
    except Exception:
        conn.execute("ROLLBACK")
        logger.exception(f"Failed to build schema for release {release!r}; rolled back")
        raise
    else:
        conn.execute("COMMIT")
        logger.success(f"Built schema for release {release!r}")
