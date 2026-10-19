"""Daily hard-delete reconciliation, separate from the frequent fetching job.

Preview: python delete_reconciliation.py --env uat --tables accounts
Delete:  python delete_reconciliation.py --env uat --tables accounts --apply
Daily:   python delete_reconciliation.py --env uat --all-tables --apply --refresh

Only select direct Mongo mirrors whose PostgreSQL text id is str(Mongo _id).
Mongo IDs must be strings or ObjectIds. Each selected table is scanned in
bounded batches; there is no persisted checkpoint. The daily systemd timer
applies deletions automatically; the CLI defaults to preview.
Full PostgreSQL parent/child rows are saved as JSONB in
public.deleted_records_archive before deletion, in the same transaction.
The archive table is created automatically on apply, never during dry-run.
This preserves PostgreSQL fields, not Mongo fields that were never synced.
Unknown foreign-key dependencies fail instead of implicitly cascading.
Apply briefly blocks PostgreSQL writes to the affected tables. Mongo reads
and PostgreSQL writes are not atomic across databases: do not run during
Mongo collection replacement/restore or reuse of deleted IDs. Analytics are
rebuilt with --refresh, or by the fetching runner's next refresh.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# common_sync.py and .env sit next to this file in the project root.
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from bson import ObjectId
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient, ReadPreference

logger = logging.getLogger(__name__)
ARCHIVE_TABLE = "deleted_records_archive"
EXTRA_CHILDREN = {
    "metaltransactions": [("metal_transaction_items", "transaction_id")],
    "opportunities": [("opportunity_stage_history", "opportunity_id")],
    "organizationroles": [("role_permissions", "role_id"), ("user_branch_roles", "role_id")],
    "usermanagements": [("user_branch_roles", "user_id")],
}


def _relation(table):
    return sql.Identifier("public", table)


def _source_collection(table, collection_map):
    key = "divisionmasters" if table == "divisions" else table.lower().replace("_", "")
    if table == "organizations" and "organizations" not in collection_map:
        key = "organizationmasters"
    return collection_map.get(key)


def select_reconciliation_tables(pg_conn, mongo_collections_map, tables_list):
    """Filter the fetching runner's base tables to supported direct mirrors.

    Custom source tables such as accounts/branches are included; derived tables
    are cleaned through their parent mappings, not compared by their own ids.
    """
    from common_sync import _get_columns_schema, NESTED_ARRAY_SYNC_CONFIG

    derived = set(NESTED_ARRAY_SYNC_CONFIG)
    derived.update(child for children in EXTRA_CHILDREN.values() for child, _ in children)
    selected = []
    with pg_conn.cursor() as cursor:
        for table in dict.fromkeys(tables_list):
            if table == ARCHIVE_TABLE or table in derived or table.startswith("ai_"):
                continue
            if not _source_collection(table, mongo_collections_map):
                logger.info("[%s] Deletion check skipped: no mapped Mongo collection", table)
                continue
            if _get_columns_schema(cursor, table).get("id") not in ("text", "character varying"):
                logger.info("[%s] Deletion check skipped: unsupported source id type", table)
                continue
            selected.append(table)
    return selected


def _missing_ids(collection, ids):
    # Test both BSON representations; presence of either preserves the PG row.
    values = list(ids)
    values.extend(ObjectId(value) for value in ids if ObjectId.is_valid(value))
    present = {str(doc["_id"]) for doc in collection.find(
        {"_id": {"$in": values}}, {"_id": 1}, max_time_ms=15000
    )}
    return [value for value in ids if value not in present]


def _check_dependencies(cursor, table, children):
    for target in [table] + [child for child, _ in children]:
        cursor.execute("""SELECT ns.nspname, c.relname, a.attname,
                   cardinality(f.conkey), referenced.attname
            FROM pg_constraint f
            JOIN pg_class c ON c.oid = f.conrelid
            JOIN pg_namespace ns ON ns.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = f.conkey[1]
            JOIN pg_class parent ON parent.oid = f.confrelid
            JOIN pg_namespace pns ON pns.oid = parent.relnamespace
            JOIN pg_attribute referenced ON referenced.attrelid = parent.oid
                AND referenced.attnum = f.confkey[1]
            WHERE f.contype = 'f' AND pns.nspname = 'public'
                AND parent.relname = %s""", (target,))
        for schema, child, column, width, referenced in cursor.fetchall():
            if (target != table or schema != "public" or width != 1
                    or referenced != "id" or (child, column) not in children):
                raise ValueError(f"Review dependency before deletion: {schema}.{child}.{column} -> {target}")


def _ensure_archive_table(cursor):
    cursor.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
        archive_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        sync_name TEXT NOT NULL,
        source_table TEXT NOT NULL,
        record_id TEXT NOT NULL,
        row_data JSONB NOT NULL,
        archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reason TEXT NOT NULL DEFAULT 'missing_in_mongodb'
    )""").format(_relation(ARCHIVE_TABLE)))


def _archive_and_delete(cursor, table, children, missing, sync_name):
    """Caller owns the transaction and table locks; any failure must roll back.

    Archive every affected row before deleting any of them. Children without
    their own id use the parent FK as record_id; row_data retains all fields
    (including composite keys), and archive_id uniquely identifies each copy.
    """
    targets = children + [(table, "id")]
    archived_counts = []
    for target, fk in targets:
        cursor.execute(sql.SQL("""INSERT INTO {}
            (sync_name, source_table, record_id, row_data, reason)
            SELECT %s, %s, COALESCE(to_jsonb(source_row)->>'id', source_row.{}::text),
                   to_jsonb(source_row), 'missing_in_mongodb'
            FROM {} AS source_row WHERE source_row.{} = ANY(%s)""").format(
                _relation(ARCHIVE_TABLE), sql.Identifier(fk), _relation(target), sql.Identifier(fk)),
            (sync_name, target, missing))
        archived_counts.append(cursor.rowcount)
    for (target, fk), archived in zip(targets, archived_counts):
        cursor.execute(sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
            _relation(target), sql.Identifier(fk)), (missing,))
        if cursor.rowcount != archived:
            raise RuntimeError(f"{target}: archive/delete count mismatch; rolling back batch")
    return archived_counts[-1]


def reconcile_deleted_records(mongo_db, pg_conn, mongo_collections_map,
                              tables_list, log_prefix="", *, dry_run=True, batch_size=500,
                              sync_name="default"):
    """Return deleted parent count (zero in dry-run). Errors abort the run.

    Pass an idle connection: this function commits/rolls back each batch.
    Earlier committed batches remain committed if a later batch fails.
    """
    from common_sync import _get_columns_schema, NESTED_ARRAY_SYNC_CONFIG

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if pg_conn.autocommit:
        raise ValueError("Reconciliation requires autocommit=False")
    if pg_conn.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
        raise ValueError("Reconciliation requires an idle PostgreSQL connection")
    derived = set(NESTED_ARRAY_SYNC_CONFIG)
    derived.update(child for children in EXTRA_CHILDREN.values() for child, _ in children)
    plan = []
    # Validate the whole selected scope before any destructive work.
    with pg_conn:
        with pg_conn.cursor() as cursor:
            for table in dict.fromkeys(tables_list):
                if table == ARCHIVE_TABLE or table in derived or table.startswith("ai_"):
                    raise ValueError(f"{table}: select a direct Mongo mirror, not a derived table")
                schema = _get_columns_schema(cursor, table)
                if schema.get("id") not in ("text", "character varying"):
                    raise ValueError(f"{table}: requires text/varchar Mongo source id")
                collection_name = _source_collection(table, mongo_collections_map)
                if not collection_name:
                    raise ValueError(f"{table}: no Mongo source collection")
                normalized = collection_name.lower().replace("_", "")
                children = list(EXTRA_CHILDREN.get(normalized, []))
                children += [(child, config["parent_fk_column"])
                             for child, config in NESTED_ARRAY_SYNC_CONFIG.items()
                             if config["mongo_collection"].lower().replace("_", "") == normalized]
                existing_children = []
                for child, fk in children:
                    child_schema = _get_columns_schema(cursor, child)
                    if not child_schema:
                        continue
                    if child_schema.get(fk) not in ("text", "character varying"):
                        raise ValueError(f"{child}.{fk}: unsupported parent id type")
                    existing_children.append((child, fk))
                _check_dependencies(cursor, table, existing_children)
                plan.append((table, collection_name, existing_children))
            if plan and not dry_run:
                _ensure_archive_table(cursor)

    total_deleted = 0
    for table, collection_name, children in plan:
        last_id = None
        while True:
            deleted = 0
            with pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("SET LOCAL lock_timeout = '5s'")
                    cursor.execute("SET LOCAL statement_timeout = '30s'")
                    if not dry_run:
                        for target in sorted({table, *(child for child, _ in children)}):
                            cursor.execute(sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(_relation(target)))
                        _check_dependencies(cursor, table, children)
                    query = sql.SQL("SELECT id FROM {} ").format(_relation(table))
                    params = []
                    if last_id is not None:
                        query += sql.SQL("WHERE id > %s ")
                        params.append(last_id)
                    query += sql.SQL("ORDER BY id LIMIT %s")
                    params.append(batch_size)
                    cursor.execute(query, params)
                    ids = [row[0] for row in cursor.fetchall()]
                    if not ids:
                        break
                    if any(not isinstance(value, str) for value in ids):
                        raise ValueError(f"{table}: null or non-string source id")
                    if collection_name not in mongo_db.list_collection_names():
                        raise ValueError(f"Mongo collection disappeared: {collection_name}")
                    collection = mongo_db[collection_name].with_options(read_preference=ReadPreference.PRIMARY)
                    missing = _missing_ids(collection, ids)
                    if missing and not dry_run:
                        missing = _missing_ids(collection, missing)
                        if collection_name not in mongo_db.list_collection_names():
                            raise ValueError(f"Mongo collection disappeared: {collection_name}")
                        if missing:
                            deleted = _archive_and_delete(
                                cursor, table, children, missing, sync_name
                            )
                    logger.info("%s[%s] %s checked=%d missing=%d deleted=%d sample=%s",
                                log_prefix, table, "DRY RUN" if dry_run else "APPLY",
                                len(ids), len(missing), deleted, missing[:10])
                    last_id = ids[-1]
            total_deleted += deleted
            if len(ids) < batch_size:
                break
    logger.info("%sReconciliation complete: %d parent rows deleted", log_prefix, total_deleted)
    return total_deleted


def run_deletion_reconciliation(name, mongo_uri, mongo_db_name, pg_host, pg_port,
                               pg_database, pg_user, pg_password, *, tables=None,
                               dry_run=True, batch_size=500, all_tables=False,
                               refresh=False):
    """Standalone run. Daily mode selects direct mirrors and refreshes reports."""
    import common_sync as cs

    if bool(tables) == bool(all_tables):
        raise ValueError("Specify tables or all_tables=True, exclusively")
    if refresh and dry_run:
        raise ValueError("Refresh requires apply mode")
    if not mongo_uri or not mongo_db_name or not pg_database:
        raise ValueError("Mongo URI/database and PostgreSQL database are required")
    # Same local pipeline lock as fetching; table locks also coordinate PG writers.
    lock_file = cs.get_lock_file(name)
    acquired = False
    pg_conn = None
    try:
        cs.acquire_lock(lock_file)
        acquired = True
        with MongoClient(mongo_uri, serverSelectionTimeoutMS=10000,
                         connectTimeoutMS=10000, socketTimeoutMS=20000) as client:
            mongo_db = client[mongo_db_name]
            collection_map = {}
            ambiguous = set()
            for collection in mongo_db.list_collection_names():
                key = collection.lower().replace("_", "")
                if key in collection_map:
                    ambiguous.add(key)
                collection_map[key] = collection
            # Drop ambiguous names so their tables are skipped (or rejected when
            # requested explicitly) instead of failing every other table.
            for key in ambiguous:
                logger.warning("Ambiguous normalized collection name %s; its table is skipped", key)
                del collection_map[key]
            pg_conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_database,
                                      user=pg_user, password=pg_password, connect_timeout=10)
            with pg_conn.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", ("mongo-delete-reconciliation",))
                if not cursor.fetchone()[0]:
                    raise RuntimeError("Another deletion reconciliation is running")
            pg_conn.commit()
            try:
                if all_tables:
                    tables = select_reconciliation_tables(pg_conn, collection_map,
                        cs.get_tables_list(pg_conn, exclude_tables=cs.EXCLUDE_TABLES))
                    pg_conn.commit()
                result = reconcile_deleted_records(mongo_db, pg_conn, collection_map, tables,
                    f"{name}: ", dry_run=dry_run, batch_size=batch_size, sync_name=name)
            except Exception:
                pg_conn.rollback()
                logger.exception("Daily reconciliation incomplete; refreshing available data if requested")
                if refresh:
                    cs.refresh_postgres_reports(pg_conn)
                raise
            if refresh and not cs.refresh_postgres_reports(pg_conn):
                raise RuntimeError("Deletion completed, but report refresh failed")
            return result
    finally:
        if pg_conn is not None:
            pg_conn.close()
        if acquired:
            cs.release_lock(lock_file)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=("uat", "saas"), required=True)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tables", nargs="+")
    scope.add_argument("--all-tables", action="store_true", help="Select all supported direct Mongo mirrors")
    parser.add_argument("--refresh", action="store_true", help="Refresh PostgreSQL reports after cleanup (requires --apply)")
    parser.add_argument("--apply", action="store_true", help="Physically delete confirmed missing rows")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("batch-size must be positive")
    if args.refresh and not args.apply:
        parser.error("--refresh requires --apply")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv(PROJECT_ROOT / ".env")
    suffix = args.env.upper()
    run_deletion_reconciliation(
        name=args.env, mongo_uri=os.getenv(f"MONGO_URI_{suffix}"),
        mongo_db_name=os.getenv(f"MONGO_DB_{suffix}"),
        pg_host=os.getenv(f"PG_HOST_{suffix}", "localhost"),
        pg_port=os.getenv(f"PG_PORT_{suffix}", "5432"),
        pg_database=os.getenv(f"PG_DATABASE_{suffix}"),
        pg_user=os.getenv(f"PG_USER_{suffix}", "postgres"),
        pg_password=os.getenv(f"PG_PASSWORD_{suffix}"), tables=args.tables,
        dry_run=not args.apply, batch_size=args.batch_size,
        all_tables=args.all_tables, refresh=args.refresh,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Deletion reconciliation failed; current batch rolled back")
        raise SystemExit(1)
