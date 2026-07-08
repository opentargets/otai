# otai — Open Targets Agentic Query Tool

## 1. Summary

`otai` is a CLI tool, paired with a Claude Code Skill, that lets Claude answer natural-language questions about Open Targets Platform release data by generating and executing SQL against the platform's parquet files hosted on a public S3 bucket. Phase 1 targets local, single-user use inside Claude Code — no hosted service, no standalone agent loop.

## 2. Goals / Non-Goals

**Goals**
- Answer natural-language questions about Open Targets data (targets, diseases, associations, evidence, drugs, etc.) by querying the real release parquet data directly from S3, with no local data materialization.
- Keep the schema/data-access layer release-aware: support the current release by default, and any specific past release on demand.
- Make the tool usable both by Claude (via a Skill) and by a human directly from a terminal.

**Non-Goals (phase 1)**
- No standalone agentic loop / Claude Agent SDK integration — Claude Code's own harness is the orchestrator.
- No MCP server.
- No hosted API/service, chat UI, or Slack bot.
- No local materialization of Open Targets data — DuckDB views always read live from S3.
- No multi-release support in `list-datasets`/`describe-dataset` (single release per call).

## 3. Architecture

```
Claude Code session
   └─ Skill (.claude/skills/otai/SKILL.md)
        └─ invokes: uvx --from <repo-path> otai <subcommand> [args] [--format table]
             └─ otai CLI (Python, Typer)
                  └─ DuckDB (httpfs, anonymous S3 access)
                       └─ s3://open-targets-public-data-releases/platform/<release>/output/*.parquet
```

- The CLI is the actual engine: schema catalog management, DuckDB view creation, query execution, guardrails.
- The Skill is a thin instruction layer: tells Claude which subcommands exist, when to call them, and how to react to errors. It carries no independent logic.
- Distribution: `uvx --from <repo-path> otai ...` — no persistent global install; local code changes are picked up on every invocation.

## 4. Data Source

- Public S3 bucket, anonymous/unsigned access, no credentials required: `s3://open-targets-public-data-releases/platform/<release>/output/`
- Releases are versioned folders (e.g. `25.12`, `26.03`, `26.06`); "latest" = lexically max.
- Each release self-hosts a [Croissant](http://mlcommons.org/croissant/1.0) schema descriptor at `s3://open-targets-public-data-releases/platform/<release>/croissant.json` (~640KB).
- The croissant file describes ~56 datasets (`recordSet`), each with:
  - a `fileSet` glob pattern (e.g. `association_by_datasource_direct/*.parquet`) relative to the release's `output/` prefix
  - a list of `field`s with name, description, dataType, and (where applicable) `references` to another dataset's field (cross-dataset relationships) and `subField`s for nested/repeated struct columns

## 5. Local State & Caching

- **Croissant cache**: fetched croissant.json is cached locally per release, e.g. `~/.cache/otai/<release>/croissant.json`. Never re-fetched once cached (release data is immutable).
- **"Latest release" resolution**: determined by listing the S3 bucket; cached with a 24h TTL (Open Targets releases quarterly, so daily staleness is a non-issue). No explicit refresh command in phase 1 — re-checked automatically once the cache expires.
- **DuckDB catalog**: a single shared DuckDB file at a predefined disk location (e.g. `~/.cache/otai/catalog.duckdb`). Each release gets its own DuckDB **schema** namespace inside that one file (e.g. `CREATE SCHEMA "26.06"`), so multiple releases can coexist side by side and be joined across in a single query.
- Field descriptions and relationships are **not** stored in DuckDB (no `COMMENT ON`). They live only in the cached croissant.json and are read via a built-in parser whenever `describe-dataset` needs them.

## 6. Initialization Pipeline (implicit, runs before every command)

1. Locate the shared DuckDB file at its predefined path; attach if it exists, create if not.
2. Resolve which release(s) are needed:
   - `list-releases`: none (just lists the bucket).
   - `list-datasets` / `describe-dataset`: from `--release` (default `latest`).
   - `run-sql`: from schema-qualified table references found in the query text (e.g. `"26.03".target`), plus `latest` as the default search path for unqualified names.
3. For each needed release, check whether its schema already exists in the DuckDB file. If not:
   a. Fetch (if not already cached) that release's croissant.json.
   b. Parse it and build the schema: `CREATE SCHEMA "<release>"`, then for each dataset, `CREATE VIEW "<release>".<dataset> AS SELECT * FROM read_parquet('s3://.../<dataset>/*.parquet')`.
4. Proceed with the requested command.

There is **no standalone `otai init` command** — this pipeline is purely implicit and runs as the first step of every subcommand.

## 7. CLI Subcommands

All commands emit a consistent JSON envelope by default; `--format table` renders human-readable output instead.

**Success:**
```json
{"ok": true, "data": { ... }}
```
**Failure:**
```json
{"ok": false, "error": {"type": "guardrail_violation | timeout | release_not_found | sql_error", "message": "..."}}
```

### `otai list-releases`
Lists available releases from the S3 bucket, flagging which is `latest` and which are already cached locally. No `--release` flag.

### `otai list-datasets [--release X]`
Lists all datasets (recordSets) for one release (default `latest`) with their one-line descriptions. Single-value `--release` only.

### `otai describe-dataset <name> [--release X]`
Returns the full field list for one dataset in one release (default `latest`): column names, types, descriptions, and cross-dataset relationships — parsed directly from the cached croissant.json. Single-value `--release` only.

### `otai run-sql "<query>"`
Executes a read-only DuckDB SQL query against the views. No `--release` flag:
- Unqualified table names resolve against the `latest` schema (via DuckDB `search_path`).
- Schema-qualified references (e.g. `"26.03".target`) target that specific release explicitly, and trigger lazy-init of that release's schema if not already built.
- This gives natural support for cross-release queries (e.g. joining `"26.06".target` against `"26.03".target`) without any extra flag.

**Guardrails on `run-sql`:**
- **Read-only enforcement**: naive heuristic — first keyword must be `SELECT`/`WITH`; reject multiple statements (e.g. via semicolon). No `sqlglot`/AST-based validation — threat model is low (local tool, LLM-generated queries, not adversarial input), so a lightweight check is sufficient.
- **Row cap**: results truncated at ~1,000 rows; response indicates truncation occurred.
- **Query timeout**: queries running longer than ~30–60s are killed and return a `timeout` error.
- **No cost/EXPLAIN pre-check** — out of scope for phase 1.
- **Release scoping is structural, not a separate allow-list check**: since only `latest`'s schema is in the default search path, any other release is only reachable via explicit schema-qualification in the SQL — DuckDB's own scoping enforces this without additional query-text validation beyond identifying which schemas to lazy-init.

## 8. Skill Design

- Lives at `.claude/skills/otai/SKILL.md`, in-repo (ships and versions together with the CLI).
- Invokes the CLI via `uvx --from <repo-path> otai <subcommand> ...`.
- Encodes the following behavioral rules for Claude:
  1. Call `list-datasets` before writing SQL when unsure which dataset(s) are relevant — never guess a table/schema name.
  2. Always `describe-dataset` on a table before joining, since ids and relationships aren't guessable from names alone.
  3. Always include a `LIMIT` in exploratory/preview queries unless the question needs a full aggregate.
  4. Schema-qualify table names explicitly when a question concerns a non-latest release (e.g. `"26.03".target`) or spans multiple releases; leave unqualified for latest.
  5. On a `run-sql` error, branch on `error.type`: `timeout` → narrow the query and retry; `sql_error`/`guardrail_violation` → fix the SQL; `release_not_found` → check `list-releases` before retrying.
  6. Cite the release(s) queried and the actual SQL executed in the final answer to the user.

## 9. Tech Stack

- **Language**: Python
- **Package/dependency manager**: `uv`
- **CLI framework**: `typer`
- **Query engine**: DuckDB (`httpfs` extension, anonymous/unsigned S3 access)
- **Distribution**: `uvx --from <repo-path> otai ...` (no persistent global install required)

## 10. Testing Strategy

Fully offline — no real network calls in tests.

- **Python-level HTTP calls** (croissant.json fetch, S3 release-listing): mocked with standard Python mocking tools.
- **DuckDB / parquet layer**: the schema-builder takes an injectable base URI, defaulting to `s3://open-targets-public-data-releases/...` in production. Tests point it at a local directory of small fixture parquet files instead (`file://` paths), so DuckDB's real `read_parquet()` code path runs unmodified against local fixtures — no S3 or mock-S3-server involved.
- **Guardrail logic** (non-`SELECT` rejection, timeout, row cap, JSON envelope shape): unit tested against a local/in-memory DuckDB with synthetic tables, independent of any Open Targets data or fixtures.

## 11. Explicitly Deferred (not phase 1)

- MCP server exposing the four operations as native tools.
- Standalone Python agentic loop via the Claude Agent SDK (would enable the tool to run outside Claude Code).
- Explicit `otai init` command (initialization stays purely implicit).
- `sqlglot`-based SQL validation (naive heuristic deemed sufficient for the threat model).
- Cost/`EXPLAIN` pre-check before query execution.
- Local materialization of any dataset (views only, always).
- Multi-release support in `list-datasets` / `describe-dataset`.
