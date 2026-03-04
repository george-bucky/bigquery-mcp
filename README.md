# bigquery-mcp (Rust)

A Rust Model Context Protocol (MCP) server for BigQuery.

This is a breaking v2 rebuild focused on lower latency, lower BigQuery/API fanout, and structured JSON tool responses.

## What Changed

- Rebuilt in Rust (`tokio` + `reqwest`) with MCP over stdio.
- Tool outputs are structured JSON via `structuredContent`.
- Added in-process TTL caching with per-key request coalescing.
- Removed expensive `ORDER BY RAND()` query-history pattern.
- Replaced N+1 table metadata fetch with a single INFORMATION_SCHEMA query.
- Region is fully dynamic (no hardcoded region in query SQL).

## Tools

- `get_datasets`
- `get_all_dataset_descriptions`
- `get_dataset_description`
- `get_tables`
- `get_columns`
- `get_query_history`

## Prerequisites

- Rust toolchain (`cargo`, `rustc`)
- BigQuery access via Application Default Credentials (ADC)

## Configuration

Required:

- `BIGQUERY_PROJECT_ID`

Optional:

- `BIGQUERY_REGION` (default: `europe-west2`)
- `BQ_CACHE_TTL_DATASETS_SECS` (default: `60`)
- `BQ_CACHE_TTL_DATASET_DETAILS_SECS` (default: `300`)
- `BQ_CACHE_TTL_TABLES_SECS` (default: `180`)
- `BQ_CACHE_TTL_COLUMNS_SECS` (default: `300`)
- `BQ_CACHE_TTL_QUERY_HISTORY_SECS` (default: `30`)
- `BQ_MAX_CONCURRENCY` (default: `16`)
- `BQ_QUERY_TIMEOUT_SECS` (default: `15`)
- `BQ_HUMAN_EMAIL_DOMAIN` (optional, only include human users from this domain)
- `BIGQUERY_API_BASE_URL` (default: `https://bigquery.googleapis.com/bigquery/v2`, useful for tests/mocks)

## Run

```bash
cargo run --release
```

The server uses stdio transport and is MCP-compatible.

## Quick MCP Setup (Cursor)

Point your MCP server command at the built binary, e.g.:

```bash
/path/to/bigquery-mcp/target/release/bigquery-mcp-rs
```

## Development

```bash
cargo fmt
cargo test
```

## Notes

- This implementation intentionally does not preserve Python output formatting compatibility.
- The legacy Python implementation is still present in the repo but is no longer the primary runtime.
