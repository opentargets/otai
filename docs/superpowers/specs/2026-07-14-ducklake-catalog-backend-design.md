# DuckLake-backed local catalog (experimental, opt-in)

Status: approved for implementation planning
Date: 2026-07-14

## Background

`otai` currently materializes each Open Targets release's dataset catalog as
one DuckDB schema per release inside a shared local file
(`~/.cache/otai/catalog.duckdb`, see `catalog.py`/`schema_builder.py`): one
`CREATE VIEW ... SELECT * FROM read_parquet('<glob>')` per dataset. The
schema itself is cached forever once built (releases are immutable), but the
view body still holds the *glob string*, not a resolved file list — so every
`run-sql` execution against an already-cached schema still triggers a fresh
S3 `LIST` at query-plan time to expand that glob. Schema caching saves the
`CREATE VIEW` step, not the listing.

We evaluated [DuckLake](https://ducklake.select/docs/stable/duckdb/introduction),
DuckDB Labs' SQL-catalog-backed lakehouse format, as a possible replacement.
Findings from that evaluation:

- DuckLake's core value (ACID multi-writer transactions, schema evolution,
  compaction) targets *producer*-side problems. `otai` is a purely read-only
  consumer of someone else's immutable, externally-versioned release data —
  most of that value doesn't apply to us.
- The one benefit that *does* apply: DuckLake's catalog stores exact
  resolved file paths (and column stats), so attaching a DuckLake catalog
  avoids the repeated glob-expansion `LIST` call that today's `CREATE VIEW`
  approach pays on every query.
- If Open Targets (the producer) ever published a DuckLake catalog
  alongside their existing flat-parquet + `croissant.json` release layout,
  `otai` could attach it directly and delete its own glob-resolution logic
  entirely. That's a separate, future, producer-side decision — out of
  scope here. `croissant.json` would still be needed regardless, since
  DuckLake has no concept of Croissant's semantic layer (field
  descriptions, cross-dataset `references`).
- Until that producer-side decision is made, `otai` can still get the same
  benefit by building its *own* local DuckLake catalog: resolving each
  dataset's glob once (same one-time S3 `LIST` cost as today's first
  schema build) and registering the resolved files into a DuckLake catalog
  via `ducklake_add_data_files`, which reads only file footers to register
  — it does not copy or rewrite the underlying parquet data.

This spec covers that self-built, local-only, opt-in mode: a real code path
inside `otai`, off by default, that can be run end-to-end against real
commands for comparison against the existing backend before anything is
decided about making it the default or extending it to a producer-published
catalog.

## Goals

- Add a second catalog backend, `ducklake`, selectable via
  `OTAI_CATALOG_BACKEND` (default remains the existing plain-DuckDB
  backend, `duckdb`).
- One root DuckLake catalog, stored locally as a DuckDB file, containing
  one DuckLake schema per release — directly mirroring the existing
  one-DuckDB-schema-per-release model, so schema drift between releases
  (renamed/added/removed datasets or fields) is a non-issue: each release's
  tables are fully independent namespaces.
- Built and updated lazily, the same way as today: a release's schema is
  created once, on first use, and reused forever after (release data is
  immutable); the same catalog file accumulates more release schemas over
  time as different releases get queried.
- No data copy: registered files stay exactly where they are on S3;
  DuckLake only records metadata about them.
- Force fully anonymous S3 access — no ambient AWS credentials from the
  environment, credentials file, or instance metadata are ever consulted,
  regardless of what's present in the shell. This applies to **both** the
  new DuckLake path and the existing httpfs path (a latent gap in the
  current code, described below).

## Non-goals

- Making `ducklake` the default backend, or removing the existing one.
- Publishing anything to S3, or changing what Open Targets (the producer)
  publishes. This is entirely local, for testing.
- Compaction, time travel, or any other DuckLake feature beyond the
  file-registration/multi-schema mechanics needed here.

## Architecture

### Config

`config.py` gains `get_catalog_backend()`, reading `OTAI_CATALOG_BACKEND`
(`"duckdb"` default, `"ducklake"` opt-in), following the existing
`OTAI_CACHE_DIR`/`OTAI_BASE_URI`/`OTAI_LOG_LEVEL` pattern.

### Module structure

Two new modules mirror the existing pair's function signatures exactly,
rather than introducing a formal backend interface/Protocol — there are
exactly two concrete implementations, and the module-level functions
already serve as the interface:

- `ducklake_catalog.py` (parallels `catalog.py`):
  - `connect_catalog(cache_dir)` — attaches
    `ducklake:<cache_dir>/ducklake_catalog.duckdb AS lake (DATA_PATH
    '<cache_dir>/ducklake_data/')`, creating the file on first use. Since
    every registered file is referenced via `ducklake_add_data_files`
    rather than written by DuckLake itself, `DATA_PATH` is a required
    formality that's never actually populated in this design.
  - `try_connect_readonly(cache_dir)` — same read-only-peek purpose as
    today's version, attached with `(READ_ONLY)`.
  - `list_cached_schemas(conn)` — lists release schemas already present in
    the lake.
- `ducklake_schema_builder.py` (parallels `schema_builder.py`):
  - `build_release_schema(conn, release, datasets, base_uri)` — same
    signature and same transactional build-or-rollback shape as today's
    version, described in detail below.

`commands.py`'s existing call sites (`_ensure_release_schema`,
`_cached_schemas_readonly`, and the `catalog`/`schema_builder` references
in `list_releases`/`list_datasets`/`run_sql`) pick which module to call
based on `config.get_catalog_backend()` — a thin dispatch, not a rewrite of
those functions' logic.

### Schema build flow (DuckLake backend)

For a release not yet present in the lake:

1. `CREATE SCHEMA lake."<release>"`.
2. For each dataset in the release's parsed croissant list:
   a. Resolve `glob_url = f"{base_uri}/{release}/output/{dataset.file_glob}"`
      to an explicit file list via DuckDB's `glob()` function — one S3
      `LIST` call, identical in cost to what today's first schema build
      already pays per dataset.
   b. If the resolved list is empty, fail the same way today's code does
      (empty glob → build failure → rollback).
   c. Introspect columns from the first resolved file via `DESCRIBE SELECT
      * FROM read_parquet('<file>')`, and `CREATE TABLE
      lake."<release>"."<dataset>"` with those exact columns. Introspecting
      the real file (rather than mapping Croissant's JSON-LD `dataType`
      values to SQL types) guarantees the table matches what
      `ducklake_add_data_files` will actually accept, with no separate
      type-mapping table to maintain.
   d. Register every resolved file (including the one used for
      introspection) via `ducklake_add_data_files`, which reads only each
      file's footer — no data is copied or rewritten.
3. Same transaction wrapping as today's `schema_builder.py`: a mid-loop
   failure rolls back the whole release's schema, not just the failed
   dataset, so a partially-built release is never mistaken for "already
   cached" on a later call.

One implementation detail to verify empirically rather than assume from
documentation: the exact `ducklake_add_data_files` argument form for a
schema-qualified table name (e.g. whether it wants `'lake'`,
`'"<release>"."<dataset>"'` as separate arguments or a single qualified
string).

## Security: forced anonymous S3 access

Today, `schema_builder.py`'s `_ensure_httpfs` only does `INSTALL
httpfs; LOAD httpfs` — no explicit S3 secret is ever created. DuckDB's
default resolution, absent an explicit secret, falls through to its
`credential_chain` provider, which searches environment variables, the
shared AWS config/credentials files, SSO, and instance metadata, in that
order. This "works" today only because most environments have none of
that configured, so it silently ends up anonymous. If the user's shell
ever has `AWS_PROFILE` or `~/.aws/credentials` set (for unrelated work),
DuckDB would sign requests with that real identity instead — not
acceptable for a tool whose entire premise is read-only access to a public
bucket.

Fix, applied identically to **both** the existing httpfs path and the new
DuckLake path via one small shared helper:

```sql
CREATE OR REPLACE SECRET otai_s3_anonymous (
    TYPE s3,
    PROVIDER config,
    SCOPE 's3://open-targets-public-data-releases'
);
```

`PROVIDER config` with no `KEY_ID`/`SECRET` supplied never consults
ambient environment/profile/instance-metadata credentials, unlike
`credential_chain`. This must be verified empirically as the first
implementation step — e.g. run with a deliberately bogus
`AWS_ACCESS_KEY_ID` set in the environment and confirm requests still
succeed anonymously rather than failing on a bad signature — rather than
trusted from documentation alone.

A hard rule documented in both new modules: **never call DuckLake's
compaction or cleanup functions** (`merge_adjacent_files`,
`expire_snapshots` / `cleanup_old_files`) anywhere in `otai`. Per DuckLake's
own docs, registering a file via `ducklake_add_data_files` transfers
"ownership" of that file to DuckLake, and those operations can delete
registered files — but the files in question are Open Targets' public S3
objects, not otai's to own or delete.

## Error handling

Same `catalog_error` vocabulary as today for attach/build failures — no
new error types. New edge case specific to this backend:
`ducklake_add_data_files` rejects a file whose columns don't match the
target table's. This shouldn't occur in practice, since the table's
columns are introspected from a file in that same glob, but if a dataset's
glob ever spans files with a heterogeneous schema, it surfaces as an
ordinary build failure and rollback — not a special-cased error path.

## Testing

New `tests/test_ducklake_catalog.py` and
`tests/test_ducklake_schema_builder.py`, mirroring the structure of
`tests/test_catalog.py` and `tests/test_schema_builder.py`: real DuckLake
operations (attach, schema/table creation, `ducklake_add_data_files`)
against the existing tiny fixture parquet files via a `file://` `base_uri`
— no mocking of DuckDB or DuckLake itself, consistent with the project's
fully-offline, real-DuckDB testing philosophy (PRD §10).

The forced-anonymous-S3-secret behavior needs its own verification (see
Security section) — likely a small dedicated test or manual check with a
bogus `AWS_ACCESS_KEY_ID` set, run before relying on it elsewhere.
