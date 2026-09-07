# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A one-way, one-shot MongoDB → PostgreSQL sync tool for Aurify's ERP. It reads from a Mongo database (the application's operational store) and upserts into a Postgres database (used for reporting/analytics), for two independent environments: **SAAS** and **UAT**. There is no server process, scheduler, or API in this repo — each run does one full pass and exits; recurrence comes from an external cron trigger (currently GitHub Actions, see below).

## Commands

Install dependencies:
```
python -m pip install -r requirements.txt
```

Run a full sync (both environments — UAT is currently active, SAAS is commented out in `runner.py`):
```
python runner.py
```

Run a single environment directly:
```
python saas.py
python uat.py
```

Required environment variables (loaded via `.env`, see `common_sync.py`'s `load_dotenv()`), per environment (`_SAAS` / `_UAT` suffix):
`MONGO_URI_*`, `MONGO_DB_*`, `PG_HOST_*` (default `localhost`), `PG_PORT_*` (default `5432`), `PG_DATABASE_*`, `PG_USER_*` (default `postgres`), `PG_PASSWORD_*`, `INITIAL_RUN_*` (`"true"`/`"false"`, default false).

There are no tests, linter, or build step configured in this repo.

## Architecture

Three thin entrypoints (`runner.py`, `saas.py`, `uat.py`) all delegate to the single engine in `common_sync.py` via `run_migration(name, mongo_uri, mongo_db_name, pg_host, pg_port, pg_database, pg_user, pg_password, initial_run)`. `name` (`"saas"` or `"uat"`) tags log lines and namespaces the per-instance lock file (`erp_sync_<name>.lock` in the OS temp dir), so both environments can run concurrently without contention. If a lock is already held by a live PID, `run_migration` logs and returns rather than raising/exiting — safe to invoke repeatedly from cron.

`run_migration` performs, in order, against one Mongo/Postgres pair:
1. **Branches first** (`sync_branches_custom`) — needed before anything else because branch↔organization relationship maps (`build_relationship_maps`) are used to backfill missing `organization_id`/`branch_id` on other tables.
2. **Generic table sync** — `get_tables_list` reads all base tables from Postgres `information_schema`, and for every table not in `SPECIAL_TABLES` or `NESTED_ARRAY_SYNC_CONFIG`, matches it to a Mongo collection by normalized name (`table_name` vs. collection name, both lowercased with underscores stripped) and syncs it through `sync_collection`.
3. **Special-case syncs** — nested-array unnesting (`sync_all_nested_arrays_generic`, driven by `NESTED_ARRAY_SYNC_CONFIG`), plus one dedicated `sync_<table>_custom`/`sync_<table>` function per table in `SPECIAL_TABLES` (e.g. `sync_metal_transactions`, `sync_accounts_custom`, `sync_deal_orders_custom`, `sync_role_permissions`) for tables whose PG shape doesn't map cleanly onto a single Mongo document.
4. **Mandatory Postgres refresh functions** — truncates snapshot tables (`ai_stock_ledger`, `ai_user_permissions`) and calls a fixed list of stored procs (`refresh_ai_tables()`, `refresh_reg_tables()`, `refresh_deal_strategy_tables()`, etc.) that must already exist in the target Postgres database (they live in the DB, not in this repo).

Each table's sync is wrapped in its own try/rollback/commit, so one table failing doesn't abort the rest of the run. Failures are logged (with a `[table_name]` prefix) and swallowed; check log output, not return values, to know what actually failed.

### Generic path: `sync_collection`

For tables not special-cased, `sync_collection` maps a Mongo collection onto a Postgres table:
- Column name mapping is `FIELD_NAME_OVERRIDES[table_name][mongo_key]` if present, else `to_snake_case(mongo_key)`. A key is only written if the resulting column exists in the Postgres schema (`_get_columns_schema`).
- Values pass through `clean_and_serialize`: ObjectId/DatetimeMS/naive-datetime normalization, dict/list → JSON string, and numeric coercion (strips non-numeric characters, caps at 1e12, falls back to `0`) when the target column's Postgres type is numeric/integer/etc.
- Incremental window: if the PG table has `updated_at`, `created_at`, or `sync_at`, it queries `MAX(that column)` and re-pulls Mongo docs with `updatedAt >= last_sync`; otherwise (or on `INITIAL_RUN`, or for `transactions` specifically) it pulls everything.
- A handful of table-specific field-extraction rules live inline in `sync_collection` (e.g. `registries`/`transactions`/`entries` party-id extraction, `contacts` KYC/country unpacking, `fixing_prices` inheriting org/branch/division from its parent `metal_transactions` row via `metal_transaction_lookup`). These are the first place to look when a new Mongo field needs bespoke handling before falling back to writing a whole custom sync function.
- Upsert is `INSERT ... ON CONFLICT (id) DO UPDATE SET ...` (skipped entirely for tables with an integer `id`, which are treated as insert-only/append logs) via `psycopg2.extras.execute_values`, batched at `BATCH_SIZE` (500).

### Adding or changing a synced table

- If the Postgres table name (underscores stripped) matches a Mongo collection name and the fields map 1:1 with simple camelCase→snake_case renaming, no code change is needed — `sync_collection` will pick it up automatically once the table exists in Postgres.
- If only the field names differ, add an entry to `FIELD_NAME_OVERRIDES`.
- If the table needs multiple source documents, array-flattening, cross-table lookups, or non-trivial keys, write a dedicated `sync_<table>_custom(mongo_db, pg_conn, default_cron_query)` function following the existing pattern (own cursor, own try/rollback/commit/finally, batched `execute_values`), add the table name to `SPECIAL_TABLES` so the generic loop skips it, and call it explicitly from `run_migration` in the special-case section.
- Simple parent→child array tables (one Mongo array field exploded into child rows keyed by conflict columns) can usually be expressed as a `NESTED_ARRAY_SYNC_CONFIG` entry instead of a bespoke function — see existing entries (e.g. `account_cash_balances`, `derivative_positions`) for the shape.

## CI

`.github/workflows/uat-sync.yml` runs `runner.py` against UAT every 15 minutes via `schedule: cron`, using repo secrets for `*_UAT` env vars (`concurrency.cancel-in-progress: false` so overlapping scheduled runs queue rather than cancel — the lock file in `common_sync.py` is the actual overlap guard). SAAS has no workflow currently and is disabled in `runner.py`.

## Self-hosted deployment (systemd)

`deploy/systemd/` has oneshot service + timer units for running `uat.py`/`saas.py` directly on a Linux server every 15 minutes, as an alternative to (or in addition to) the GitHub Actions cron. Each environment gets its own service+timer pair so they can be enabled/monitored independently rather than going through `runner.py`. See the unit files' comments for the one behavioral gotcha (`PrivateTmp` must stay unset so the app's own overlap lock file in the shared temp dir persists across runs).
