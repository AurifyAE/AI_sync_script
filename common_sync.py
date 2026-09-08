import os
import sys
import json
import logging
import tempfile
import re
import psutil
from datetime import datetime, timezone, timedelta

from pymongo import MongoClient
from bson import ObjectId, DatetimeMS
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

_log_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(instance)s] %(message)s",
    handlers=[_log_handler]
)
logger = logging.getLogger(__name__)

_CURRENT_INSTANCE = "default"


class _InstanceLogFilter(logging.Filter):
    def filter(self, record):
        record.instance = _CURRENT_INSTANCE
        return True

_log_handler.addFilter(_InstanceLogFilter())


class SyncAlreadyRunningError(Exception):
    """Raised instead of sys.exit() so a stuck lock never kills the caller
    (cron process, or a long-running scheduler/FastAPI process running
    several pipelines back to back)."""
    pass

load_dotenv()


INITIAL_RUN = False
MONGO_URI   = None
MONGO_DB    = None
PG_HOST     = None
PG_PORT     = None
PG_DATABASE = None
PG_USER     = None
PG_PASSWORD = None

SYNC_WINDOW_MINUTES = 2880
BATCH_SIZE          = 500
LOCK_STALE_SECONDS  = 3600
EXCLUDE_TABLES      = []


def get_lock_file(name: str) -> str:
    """Each pipeline (migration_1, migration_2, ...) gets its own lock file,
    so running two pipelines back to back (or from two different processes)
    never blocks on the other one's lock."""
    return os.path.join(tempfile.gettempdir(), f"erp_sync_{name}.lock")


SPECIAL_TABLES = {
    "metal_transaction_items", "role_permissions", "user_branch_roles", 
    "divisions", "branches", "stock_buffers", "accounts", "company_accounts","organizations", "opportunity_stage_history"
    }


FIELD_NAME_OVERRIDES = {
    "registries": {
        "party": "party_id",
        "partyId": "party_id",
        "partyCode": "party_code",
        "division": "division_id",
        "divisionId": "division_id",
        "assetType": "currency_code",
        "currencyCode": "currency_code",
        "organizationId": "organization_id",
        "OrganizationId": "organization_id",
        "branchId": "branch_id"
    },
    "fixing_prices": {
        "transaction": "transaction_id",
        "metalRate": "metal_rate_id",
        "organizationId": "organization_id",
        "OrganizationId": "organization_id",
        "orgId": "organization_id",
        "branchId": "branch_id",
        "division": "division_id",
        "divisionId": "division_id"
    },
    "accounts": {
        "vatStatus": "vat_status",
        "vatNumber": "vat_number",
        "tradeLicense": "trade_license_number",
        "tradeLicenseNumber": "trade_license_number"
    },
    "metal_transactions": {
        "party": "party_id",
        "partyId": "party_id",
        "partyCode": "party_code",
        "salesman": "salesman_id",
        "division": "division_id",
        "divisionId": "division_id",
        "divisionCode": "division_code"
    },
    "deal_orders": {
        "party": "party_id",
        "partyId": "party_id",
        "partyCode": "party_code",
        "partyName": "party_name",
        "assetType": "currency_code",
        "currencyCode": "currency_code",
        "division": "division_id",
        "divisionId": "division_id",
        "orderDate": "voucher_date",     
        "voucherDate": "voucher_date",
        "grossWeight": "gross_weight",
        "pure_weight": "pure_weight",
        "pureWeight": "pure_weight",
        "premiumPercent": "premium_percent",
        "premium": "premium_percent"
    },
    "channels": {
        "organizationId": "organization_id",
        "OrganizationId": "organization_id",
        "orgId": "organization_id",
        "organization": "organization_id",
        "branchId": "branch_id"
    },
    "entries": {
        "party": "party_id",
        "partyId": "party_id",
        "partyCode": "party_code",
        "partyCurrency": "party_currency_id",
        "division": "division_id",
        "divisionId": "division_id",
        "organizationId": "organization_id",
        "OrganizationId": "organization_id",
        "branchId": "branch_id"
    },
    "inventory_logs": {
        "party": "party_id",
        "partyId": "party_id",
        "partyCode": "party_code",
        "division": "division_id",
        "divisionId": "division_id",
        "organizationId": "organization_id",
        "OrganizationId": "organization_id",
        "branchId": "branch_id"
    },
    "metal_stocks": {
        "brand": "brand_id",
        "brandId": "brand_id",
        "category": "category_id",
        "categoryId": "category_id",
        "subCategory": "sub_category_id",
        "subCategoryId": "sub_category_id",
        "type": "type_id",
        "typeId": "type_id",
        "country": "country_id",
        "countryId": "country_id",
        "karat": "karat_id",
        "karatId": "karat_id",
    },
    "transactions": {
        "branch": "branch_id",
        "branchId": "branch_id",
        "voucherNo": "voucher_no",
        "transactionDate": "transaction_date",
        "transactionType": "transaction_type",
        "transactionId": "transaction_id",
        "transactionModel": "transaction_model",
        "transactionStatus": "transaction_status",
        "account": "account_id",
        "accountId": "account_id",
        "creditAmount": "credit_amount",
        "debitAmount": "debit_amount",
        "currency": "currency_id",
        "currencyId": "currency_id",
        "currencyRate": "currency_rate",
        "createdBy": "created_by",
        "updatedBy": "updated_by",
        "entryStatus": "entry_status",
        "transactionReference": "transaction_reference",
        "isOpening": "is_opening",
        "createdAt": "created_at",
        "updatedAt": "updated_at"
    },
    "contacts": {
        "source": "source_id",
        "sourceId": "source_id",
    },
}


def to_snake_case(name: str) -> str:
    s = re.sub(r'([A-Z])', r'_\1', name)
    return s.lower().lstrip('_')


def clean_and_serialize(value, expected_type=None):
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, DatetimeMS):
        try:
            return value.as_datetime().replace(tzinfo=timezone.utc)
        except Exception:
            return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if expected_type in ['numeric', 'double precision', 'real', 'integer', 'bigint']:
        try:
            # If there are non-numeric characters, an attempt is made to convert them into numbers.
            if isinstance(value, str):
                value = re.sub(r'[^0-9.-]', '', value)
            val_float = float(value)
            if abs(val_float) >= 1e12:
                return None
            return val_float if expected_type in ['numeric', 'double precision', 'real'] else int(val_float)
        except (ValueError, TypeError):
            return 0
    return value


def to_str_id(value):
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return str(value.get("$oid") or value.get("_id") or value.get("id"))
    return str(value)


def acquire_lock(lock_file: str):
    if os.path.exists(lock_file):
        try:
            with open(lock_file, "r") as f:
                old_pid = int(f.read().strip())
            if psutil.pid_exists(old_pid):
                raise SyncAlreadyRunningError(
                    f"Lock held by active process (PID {old_pid}) at {lock_file}."
                )
            else:
                logger.warning(
                    f"Stale lock file found (PID {old_pid} is dead). Removing and continuing."
                )
                os.remove(lock_file)
        except (ValueError, OSError) as e:
            logger.warning(f"Could not read lock file ({e}). Removing and continuing.")
            os.remove(lock_file)
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    logger.info("Lock acquired.")


def release_lock(lock_file: str):
    if os.path.exists(lock_file):
        os.remove(lock_file)
    logger.info("Lock released.")


def get_tables_list(pg_conn, exclude_tables=None):
    exclude_tables = set(exclude_tables or [])
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE 'ai_%'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in pg_cursor.fetchall() if row[0] not in exclude_tables]
    pg_cursor.close()
    return tables


def build_relationship_maps(pg_conn):
    pg_cursor = pg_conn.cursor()
    branch_to_org = {}
    org_to_branch = {}
    try:
        pg_cursor.execute("SELECT id, organization_id FROM branches WHERE organization_id IS NOT NULL AND id IS NOT NULL")
        rows = pg_cursor.fetchall()
        for branch_id, org_id in rows:
            branch_to_org[branch_id] = org_id
            if org_id not in org_to_branch:
                org_to_branch[org_id] = branch_id
    except Exception as e:
        logger.warning(f"Could not build maps: {e}")
    finally:
        pg_cursor.close()
    return branch_to_org, org_to_branch


def _get_columns_schema(pg_cursor, table_name):
    pg_cursor.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
    """, (table_name,))
    return {row[0]: row[1] for row in pg_cursor.fetchall()}


def _upsert_batch(pg_cursor, table_name, columns_list, rows):
    columns_str = ", ".join([f'"{c}"' for c in columns_list])
    update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns_list if c not in ("id", "created_at", "sync_at")])
    sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
    execute_values(pg_cursor, sql, rows, page_size=BATCH_SIZE)

NESTED_ARRAY_SYNC_CONFIG = {
    "account_cash_balances": {
        "mongo_collection": "accounts",
        "array_path": ["balances", "cashBalance"],
        "parent_fk_column": "account_id",
        "conflict_target": "account_id, currency_code",
        "mappings": {
            "currency_code": "code",
            "amount": "amount",
            "party_id": "_id"
        }
    },
    "account_metal_balances": {
        "mongo_collection": "accounts",
        "array_path": ["balances", "preciousMetalBalance"],
        "parent_fk_column": "account_id",
        "conflict_target": "account_id, division_id",
        "mappings": {
            "division_id": "division", 
            "total_grams": "totalGrams"
        }
    },
    "derivative_positions": {
        "mongo_collection": "derivatives",
        "array_path": ["openPositions"],
        "parent_fk_column": "derivative_id",
        "conflict_target": "id",
        "mappings": {
            "status": "status",
            "quantity": "quantity",
            "fixing_rate": "fixingRate"
        }
    },
    "income_expense_items": {
        "mongo_collection": "incomeexpenses",
        "array_path": ["items"],
        "parent_fk_column": "income_expense_id",
        "conflict_target": "id",
        "mappings": {
            "account_id": "accountId",
            "amount": "totalAmount",
            "vat_percentage": "vatPercentage",
            "vat_amount": "vatAmount"
        }
    },
    "opening_balance_entries": {
        "mongo_collection": "openingbalances",
        "array_path": ["entries"],
        "parent_fk_column": "opening_id",
        "conflict_target": "id",
        "mappings": {
            "party_id": "partyId",
            "asset_type": "assetType",
            "transaction_type": "transactionType",
            "value": "value",
            "description": "description"
        }
    },
    "transaction_fixing_orders": {
        "mongo_collection": "transactionfixings",
        "array_path": ["orders"],
        "parent_fk_column": "fixing_id",
        "conflict_target": "id",
        "mappings": {
            "gross_weight": "grossWeight",
            "pure_weight": "pureWeight",
            "purity": "purity",
            "one_gram_rate": "oneGramRate",
            "bid_value": "bidValue",
            "price": "price",
            "currency_code": "currencyCode"
        }
    }
}

def sync_all_nested_arrays_generic(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    
    for pg_table, config in NESTED_ARRAY_SYNC_CONFIG.items():
        try:
            logger.info(f"   [{pg_table}] Dynamic nested sync starting...")
            schema = _get_columns_schema(pg_cursor, pg_table)
            id_is_int = schema.get("id") in ['integer', 'bigint']
            fk_type = schema.get(config["parent_fk_column"])
            fk_is_int = fk_type in ['integer', 'bigint']
            
            mongo_coll = config["mongo_collection"]
            array_path = config["array_path"]
            fk_column = config["parent_fk_column"]
            conflict_target = config.get("conflict_target", "id")
            mappings = config["mappings"]

            if id_is_int and conflict_target == "id" and "source_key" in schema:
                conflict_target = "source_key"
            
            mongo_query = {} if INITIAL_RUN else default_cron_query
            mongo_cursor = mongo_db[mongo_coll].find(mongo_query)
            
            batch_rows = []
            total_synced = 0
            
            conflict_cols = [c.strip() for c in conflict_target.split(",")]
            
            for doc in mongo_cursor:
                parent_id = str(doc.get("_id"))
                if fk_is_int:
                    continue 
                    
                nested_array = doc
                for path_key in array_path:
                    nested_array = nested_array.get(path_key, []) if isinstance(nested_array, dict) else []
                
                if not nested_array or not isinstance(nested_array, list):
                    continue
                
                local_unique_rows = {}
                
                for idx, item in enumerate(nested_array):
                    row_data = {}
                    
                    if not id_is_int:
                        row_data["id"] = f"{parent_id}_{idx}"
                    elif "source_key" in schema:
                        row_data["source_key"] = (
                            to_str_id(item.get("_id"))
                            or f"{parent_id}_{idx}"
                        )

                    row_data[fk_column] = parent_id
                    
                    for pg_col, mongo_field in mappings.items():
                        if pg_col in schema:
                            val = item.get(mongo_field)
                            row_data[pg_col] = clean_and_serialize(val, schema[pg_col])
                    
                    try:
                        unique_key = "-".join([str(row_data.get(c)) for c in conflict_cols])
                    except Exception:
                        unique_key = f"{parent_id}_{idx}"
                        
                    local_unique_rows[unique_key] = row_data
                
                for row_data in local_unique_rows.values():
                    columns = list(row_data.keys())
                    values = tuple(row_data[c] for c in columns)
                    
                    columns_str = ", ".join([f'"{c}"' for c in columns])
                    update_cols = [c for c in columns if c not in conflict_cols and c not in ("created_at", "sync_at")]
                    
                    if update_cols:
                        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                        sql = f'INSERT INTO "{pg_table}" ({columns_str}) VALUES %s ON CONFLICT ({conflict_target}) DO UPDATE SET {update_str};'
                    else:
                        sql = f'INSERT INTO "{pg_table}" ({columns_str}) VALUES %s ON CONFLICT ({conflict_target}) DO NOTHING;'
                    
                    batch_rows.append(values)
                    
                    if len(batch_rows) >= BATCH_SIZE and sql:
                        execute_values(pg_cursor, sql, batch_rows)
                        total_synced += len(batch_rows)
                        batch_rows = []
            
            if batch_rows and sql:
                execute_values(pg_cursor, sql, batch_rows)
                total_synced += len(batch_rows)
                
            pg_conn.commit()
            logger.info(f"   [{pg_table}] Successfully synced {total_synced} nested rows.")
            
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"   [{pg_table}] Dynamic nested sync FAILED: {e}")
            
    pg_cursor.close()

def sync_opportunity_stage_history(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()

    try:
        pg_cursor.execute(
            'SELECT EXISTS (SELECT 1 FROM "opportunity_stage_history" LIMIT 1);'
        )
        table_has_rows = pg_cursor.fetchone()[0]

        mongo_query = (
            {}
            if INITIAL_RUN or not table_has_rows
            else default_cron_query
        )

        mongo_cursor = mongo_db["opportunities"].find(
            mongo_query,
            {"stageHistory": 1}
        )

        affected_opportunity_ids = []
        history_rows = []
        sync_time = datetime.now(timezone.utc)

        for opportunity in mongo_cursor:
            opportunity_id = to_str_id(opportunity.get("_id"))
            if not opportunity_id:
                continue

            affected_opportunity_ids.append(opportunity_id)

            stage_history = opportunity.get("stageHistory") or []
            if not isinstance(stage_history, list):
                continue

            for history_item in stage_history:
                if not isinstance(history_item, dict):
                    continue

                history_rows.append((
                    opportunity_id,
                    history_item.get("stage"),
                    clean_and_serialize(history_item.get("enteredAt")),
                    clean_and_serialize(history_item.get("note")),
                    to_str_id(history_item.get("updatedBy")),
                    sync_time,
                ))

        affected_opportunity_ids = list(set(affected_opportunity_ids))

        if not affected_opportunity_ids:
            logger.info(
                "  [opportunity_stage_history] "
                "No affected opportunities found."
            )
            return

        pg_cursor.execute(
            """
            DELETE FROM "opportunity_stage_history"
            WHERE opportunity_id = ANY(%s);
            """,
            (affected_opportunity_ids,)
        )

        if history_rows:
            execute_values(
                pg_cursor,
                """
                INSERT INTO "opportunity_stage_history"
                    (
                        opportunity_id,
                        stage,
                        entered_at,
                        note,
                        updated_by,
                        sync_at
                    )
                VALUES %s;
                """,
                history_rows,
                page_size=BATCH_SIZE
            )

        pg_conn.commit()

        logger.info(
            "  [opportunity_stage_history] Synced %s history rows "
            "for %s opportunities.",
            len(history_rows),
            len(affected_opportunity_ids)
        )

    except Exception as e:
        pg_conn.rollback()
        logger.error(
            f"  [opportunity_stage_history] FAILED: {e}"
        )
    finally:
        pg_cursor.close()

def sync_organizations_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        mongo_query = {} if INITIAL_RUN else default_cron_query
        
        coll_name = "organizations" if "organizations" in mongo_db.list_collection_names() else "organizationmasters"
        mongo_cursor = mongo_db[coll_name].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            config_obj = doc.get("configuration") or {}
            
            # --- Base Currency Extraction ---
            base_currency_list = config_obj.get("baseCurrency") or []
            base_currency_id = None
            if isinstance(base_currency_list, list) and len(base_currency_list) > 0:
                first_currency = base_currency_list[0]
                if isinstance(first_currency, dict):
                    curr_val = first_currency.get("currency")
                    base_currency_id = str(curr_val) if curr_val else None

            # --- Account Configurations Extraction ---
            account_configs = config_obj.get("accountConfiguration") or []
            first_config_key = None
            first_account_type = None

            if isinstance(account_configs, list) and len(account_configs) > 0:
                first_item = account_configs[0]
                if isinstance(first_item, dict):
                    first_config_key = first_item.get("configKey")
                    first_account_type = first_item.get("accountType")

            # Postgres Columns Matching Data
            row_data = {
                "id": str(doc.get("_id")),
                "name": doc.get("companyName") or doc.get("orgName") or doc.get("name") or "Unknown Organization",
                "email": doc.get("email"),
                "org_code": doc.get("orgCode") or doc.get("org_code"),
                "country": doc.get("country"),
                "base_currency_id": base_currency_id,
                "no_of_branches": doc.get("noOfBranches") or config_obj.get("noOfBranches"),
                "no_of_users": doc.get("noOfUsers") or config_obj.get("noOfUsers"),
                "is_active": doc.get("isActive", True),
                "is_deleted": doc.get("isDeleted", False),
                "created_at": doc.get("createdAt"),
                "last_login": doc.get("lastLogin"),
                "config_key": first_config_key,
                "account_type": to_str_id(first_account_type) if first_account_type else None,
                "account_configurations": json.dumps(account_configs, default=str),
                "sync_at": datetime.now(timezone.utc)
            }
            all_rows.append(row_data)

        if not all_rows: 
            return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        
        sql = f'INSERT INTO "organizations" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [organizations] Synced {len(all_rows)} records successfully.")
        
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [organizations] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()


def sync_collection(
    mongo_db, pg_conn, pg_table_name: str, mongo_collection_name: str, default_cron_query: dict,
    branch_to_org: dict, org_to_branch: dict, branch_to_div: dict = None,
    metal_transaction_lookup: dict = None
    ):
    pg_cursor = pg_conn.cursor()
    try:
        pg_columns_schema = _get_columns_schema(pg_cursor, pg_table_name)
        if not pg_columns_schema:
            return

        id_is_int = pg_columns_schema.get("id") in ['integer', 'bigint']
        overrides = FIELD_NAME_OVERRIDES.get(pg_table_name, {})

        if INITIAL_RUN or pg_table_name == "transactions":
            mongo_query = {}
        else:
            timestamp_col = "updated_at" if "updated_at" in pg_columns_schema else ("created_at" if "created_at" in pg_columns_schema else "sync_at")
            pg_cursor.execute(f'SELECT MAX("{timestamp_col}") FROM "{pg_table_name}";')
            last_sync = pg_cursor.fetchone()[0]
            mongo_query = {"updatedAt": {"$gte": last_sync}} if last_sync else default_cron_query

        mongo_cursor = mongo_db[mongo_collection_name].find(mongo_query)
        all_docs = []

        for doc in mongo_cursor:
            row_data = {}
            if not id_is_int:
                row_data["id"] = str(doc.get("_id"))
                
            if "sync_at" in pg_columns_schema:
                row_data["sync_at"] = datetime.now(timezone.utc)

            # 1. CUSTOM EXTRACTION FOR PARTY & PARTY_CODE
            if pg_table_name == "registries":
                raw_party = doc.get("party") or doc.get("partyId") or doc.get("account")
                if isinstance(raw_party, dict):
                    row_data["party_id"] = str(raw_party.get("$oid") or raw_party.get("_id") or raw_party.get("id"))
                elif raw_party:
                    row_data["party_id"] = str(raw_party)

            if pg_table_name == "transactions":
                row_data["branch_id"] = to_str_id(doc.get("branch") or doc.get("branchId"))
                row_data["account_id"] = to_str_id(doc.get("account") or doc.get("accountId"))
                row_data["currency_id"] = to_str_id(doc.get("currency") or doc.get("currencyId"))
                row_data["created_by"] = to_str_id(doc.get("createdBy") or doc.get("created_by"))
                row_data["updated_by"] = to_str_id(doc.get("updatedBy") or doc.get("updated_by"))

                raw_party_code = doc.get("partyCode") or doc.get("accountCode") or doc.get("party_code")
                if raw_party_code:
                    row_data["party_code"] = str(raw_party_code)

            if pg_table_name in ["entries", "inventory_logs"]:
                raw_party = doc.get("party") or doc.get("partyId") or doc.get("customer") or doc.get("customerId") or doc.get("account")
                if isinstance(raw_party, dict):
                    row_data["party_id"] = str(raw_party.get("$oid") or raw_party.get("_id") or raw_party.get("id"))
                elif raw_party:
                    row_data["party_id"] = str(raw_party)
            if pg_table_name == "transaction_fixings" and "currency_code" in pg_columns_schema:
                fixing_orders = doc.get("orders") or []
                if isinstance(fixing_orders, list) and fixing_orders:
                    first_order = fixing_orders[0] or {}
                    if isinstance(first_order, dict):
                        row_data["currency_code"] = first_order.get("currencyCode")
            
            if pg_table_name == "contacts" and "country" in pg_columns_schema:
                addr = doc.get("address") or {}
                if isinstance(addr, dict) and addr.get("country"):
                    row_data["country"] = addr.get("country")
            
            if pg_table_name == "contacts":
                kyc_obj = doc.get("kyc") or {}
                if isinstance(kyc_obj, dict):
                    if "passport_number" in pg_columns_schema and kyc_obj.get("passportNo"):
                        row_data["passport_number"] = kyc_obj.get("passportNo")
                    if "emirates_id" in pg_columns_schema and kyc_obj.get("emiratesId"):
                        row_data["emirates_id"] = kyc_obj.get("emiratesId")

            # 2. GENERAL FIELD MAPPING LOOP
            for key, value in doc.items():
                if key == "_id":
                    continue
                col = overrides.get(key) or to_snake_case(key)
                # Metal stock aliases (e.g. brand / brandId) can coexist.
                # An earlier null must not hide a later populated alias.
                replace_null_alias = (
                    pg_table_name == "metal_stocks"
                    and col in overrides.values()
                    and row_data.get(col) is None
                    and value is not None
                )
                if col in pg_columns_schema and (col not in row_data or replace_null_alias):
                    row_data[col] = clean_and_serialize(value, pg_columns_schema[col])

            # 3. ORGANIZATIONS, BRANCH, DIVISION LOGICS
            if pg_table_name == "organizations" and not row_data.get("name"):
                row_data["name"] = doc.get("orgName") or doc.get("companyName") or doc.get("displayName") or "Unknown Organization"

            if "organization_id" in pg_columns_schema and not row_data.get("organization_id") and row_data.get("branch_id"):
                org = branch_to_org.get(row_data["branch_id"])
                if org:
                    row_data["organization_id"] = org

            if "branch_id" in pg_columns_schema and not row_data.get("branch_id") and row_data.get("organization_id"):
                branch = org_to_branch.get(row_data["organization_id"])
                if branch:
                    row_data["branch_id"] = branch

            if "division_id" in pg_columns_schema and not row_data.get("division_id"):
                raw_div = doc.get("division") or doc.get("divisionId") or doc.get("Division") or doc.get("division_id")
                
                if isinstance(raw_div, dict):
                    row_data["division_id"] = to_str_id(raw_div.get("_id") or raw_div.get("id"))
                    if "division_name" in pg_columns_schema and raw_div.get("name"):
                        row_data["division_name"] = str(raw_div.get("name"))
                elif raw_div:
                    row_data["division_id"] = to_str_id(raw_div)
                
                elif branch_to_div and row_data.get("branch_id"):
                    row_data["division_id"] = branch_to_div.get(row_data.get("branch_id"))

            if "division_name" in pg_columns_schema and not row_data.get("division_name"):
                raw_div_name = doc.get("divisionName") or doc.get("division_name")
                if raw_div_name:
                    row_data["division_name"] = str(raw_div_name)
            
            # fixing_prices-inte row_data build cheytha shesham:
            if pg_table_name == "fixing_prices" and row_data.get("transaction_id") and not row_data.get("organization_id"):
                parent = metal_transaction_lookup.get(row_data["transaction_id"])
                if parent:
                    row_data["organization_id"], row_data["branch_id"], row_data["division_id"] = parent

            all_docs.append(row_data)

        if not all_docs:
            logger.info(f"  [{pg_table_name}] No new/updated records in this window.")
            return

        columns_list = [c for c in pg_columns_schema.keys() if any(c in doc for doc in all_docs)]
        columns_str = ", ".join([f'"{c}"' for c in columns_list])
        
        if not id_is_int:
            update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns_list if c not in ("id", "created_at", "sync_at")])
            sql = f'INSERT INTO "{pg_table_name}" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        else:
            sql = f'INSERT INTO "{pg_table_name}" ({columns_str}) VALUES %s;'

        batch_rows = []
        total_synced = 0

        for row_data in all_docs:
            batch_rows.append(tuple(row_data.get(col, None) for col in columns_list))
            if len(batch_rows) >= BATCH_SIZE:
                execute_values(pg_cursor, sql, batch_rows, page_size=BATCH_SIZE)
                total_synced += len(batch_rows)
                batch_rows = []

        if batch_rows:
            execute_values(pg_cursor, sql, batch_rows, page_size=BATCH_SIZE)
            total_synced += len(batch_rows)

        pg_conn.commit()
        logger.info(f"  [{pg_table_name}] Synced {total_synced} records.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [{pg_table_name}] FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_branches_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["branches"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            addr = doc.get("orgAddress", {}) or {}
            
            raw_currency = doc.get("currency")
            currency_val = to_str_id(raw_currency) if isinstance(raw_currency, ObjectId) else raw_currency

            row_data = {
                "id": str(doc.get("_id")),
                "organization_id": to_str_id(doc.get("organizationId")),
                "branch_name": doc.get("branchName"),
                "is_active": doc.get("isActive", True),
                "is_deleted": doc.get("isDeleted", False),
                "vat_enabled": doc.get("vatControl", False),   
                "kyc_enabled": doc.get("allowKYC", False),    
                "currency": currency_val,
                "address_building": addr.get("officeBuildingName"),
                "address_shop_number": addr.get("officeShopNumber"),
                "address_street": addr.get("streetArea"),
                "address_city": addr.get("city"),
                "address_emirate": addr.get("emirate"),
                "address_country": addr.get("country"),
                "address_po_box": addr.get("poBox"),
                "sync_at": datetime.now(timezone.utc),
            }
            all_rows.append(row_data)

        if not all_rows: return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        sql = f'INSERT INTO "branches" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [branches] Synced {len(all_rows)} records.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [branches] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_divisions_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["divisionmasters"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            row_data = {
                "id": str(doc.get("_id")),
                "code": doc.get("code"),
                "description": doc.get("description"),
                "organization_id": to_str_id(doc.get("OrganizationId")),
                "branch_id": to_str_id(doc.get("branchId")),
                "is_active": doc.get("isActive", True),
                "sync_at": datetime.now(timezone.utc),
            }
            all_rows.append(row_data)

        if not all_rows:
            logger.info("  [divisions] No new/updated records in this window.")
            return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        sql = f'INSERT INTO "divisions" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [divisions] Synced {len(all_rows)} records.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [divisions] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_deal_strategies_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["dealstrategies"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            lbma = doc.get("lbma", {}) if isinstance(doc.get("lbma"), dict) else {}
            uaegd = doc.get("uaegd", {}) if isinstance(doc.get("uaegd"), dict) else {}
            local = doc.get("local", {}) if isinstance(doc.get("local"), dict) else {}

            lbma_val = clean_and_serialize(lbma.get("value"), 'numeric')
            uaegd_val = clean_and_serialize(uaegd.get("value"), 'numeric')
            local_val = clean_and_serialize(local.get("value"), 'numeric')

            rates = [r for r in [lbma_val, uaegd_val, local_val] if r is not None]
            best_rate = max(rates) if rates else None

            row_data = {
                "id": str(doc.get("_id")),
                "organization_id": to_str_id(doc.get("organizationId") or doc.get("OrganizationId")),
                "branch_id": to_str_id(doc.get("branchId")),
                "strategy_date": clean_and_serialize(doc.get("date") or doc.get("strategyDate")),
                "data": clean_and_serialize(doc.get("data")), 
                "lbma_value": lbma_val,
                "lbma_type": clean_and_serialize(lbma.get("type")),
                "uaegd_value": uaegd_val,
                "uaegd_type": clean_and_serialize(uaegd.get("type")),
                "local_value": local_val,
                "local_type": clean_and_serialize(local.get("type")),
                "best_rate": best_rate,
                "sync_at": datetime.now(timezone.utc),
            }
            all_rows.append(row_data)

        if not all_rows: return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        sql = f'INSERT INTO "deal_strategies" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [deal_strategies] Synced {len(all_rows)} records with nested rates.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [deal_strategies] FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_deal_orders_custom(mongo_db, pg_conn, default_cron_query, branch_to_org):
    pg_cursor = pg_conn.cursor()
    try:
        pg_cursor.execute("SELECT id, customer_name FROM accounts WHERE id IS NOT NULL;")
        account_lookup = {str(r[0]): r[1] for r in pg_cursor.fetchall()}

        pg_cursor.execute("SELECT id, currency_code FROM currency_masters WHERE id IS NOT NULL;")
        currency_lookup = {str(r[0]): r[1] for r in pg_cursor.fetchall()}

        pg_cursor.execute("""
            SELECT b.id, cm.currency_code FROM branches b
            JOIN currency_masters cm ON cm.id = b.currency
        """)
        branch_currency_lookup = {str(r[0]): r[1] for r in pg_cursor.fetchall()}

        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["dealorders"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            stock_items = doc.get("stockItems") or doc.get("stockitems") or []
            
            gross_weight = sum(float(item.get("grossWeight") or item.get("gross_weight") or 0) for item in stock_items)
            pure_weight = sum(float(item.get("pureWeight") or item.get("pure_weight") or 0) for item in stock_items)
            
            premium_rates = []
            for item in stock_items:
                premium_discount = item.get("premiumDiscount") or item.get("premium_discount") or {}
                rate = premium_discount.get("rate")
                if rate is not None:
                    try:
                        premium_rates.append(float(rate))
                    except (ValueError, TypeError):
                        pass
            
            premium_percent = sum(premium_rates) / len(premium_rates) if premium_rates else None

            raw_party_code = doc.get("partyCode") or doc.get("party_code") or doc.get("partyId")
            m_party_code_str = to_str_id(raw_party_code) if raw_party_code else None
            
            p_name = account_lookup.get(m_party_code_str) if m_party_code_str else None
            raw_base_currency = doc.get("baseCurrency") or doc.get("base_currency")
            m_base_currency_str = to_str_id(raw_base_currency) if raw_base_currency else None
            currency_code = currency_lookup.get(m_base_currency_str) if m_base_currency_str else None

            if not currency_code:
                b_id = to_str_id(doc.get("branchId") or doc.get("branch_id"))
                currency_code = branch_currency_lookup.get(b_id) if b_id else None
            
            total_sum = doc.get("totalSummary") or doc.get("totalsummary") or {}
            total_amount = clean_and_serialize(total_sum.get("totalAmount") or total_sum.get("total_amount"), 'numeric')

            org_id = to_str_id(doc.get("organizationId") or doc.get("organization_id"))
            branch_id = to_str_id(doc.get("branchId") or doc.get("branch_id"))

            if not org_id and branch_id:
                org_id = branch_to_org.get(branch_id)  
            salesman_id = to_str_id(doc.get("salesmanId") or doc.get("salesman_id"))

            row_data = {
                "id": str(doc.get("_id")),
                "organization_id": org_id,
                "branch_id": branch_id,
                "order_number": doc.get("orderNumber") or doc.get("order_number"),
                "order_type": doc.get("orderType") or doc.get("order_type"),
                "status": doc.get("status"),
                "division_id": to_str_id(doc.get("divisionId") or doc.get("division")),
                "party_id": m_party_code_str,
                "party_name": p_name,
                "currency_code": currency_code,
                "party_currency_id": m_base_currency_str,
                "gross_weight": gross_weight if gross_weight > 0 else None,
                "pure_weight": pure_weight if pure_weight > 0 else None,
                "premium_percent": premium_percent,
                "total_amount": total_amount,
                "voucher_date": clean_and_serialize(doc.get("orderDate")), 
                "salesman_id": salesman_id,
                "salesman_name": doc.get("salesmanName"),
                "sync_at": datetime.now(timezone.utc)
            }
            all_rows.append(row_data)

        if not all_rows: 
            logger.info("   [deal_orders] No rows to sync in this batch.")
            return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        
        sql = f'INSERT INTO "deal_orders" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"   [deal_orders] Successfully synced {len(all_rows)} rows. NULL issue resolved.")
        
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"   [deal_orders] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_stock_buffers_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        pg_cursor.execute('SELECT id FROM "organizations";')
        valid_org_ids = {row[0] for row in pg_cursor.fetchall()}

        user_org_map = {}
        for user in mongo_db["usermanagements"].find({}, {"OrganizationId": 1, "organizationId": 1}):
            org_val = user.get("OrganizationId") or user.get("organizationId")
            user_org_map[str(user.get("_id"))] = to_str_id(org_val)

        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["stockbuffers"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            created_by = to_str_id(doc.get("createdBy"))
            org_id = None

            if created_by in valid_org_ids:
                org_id = created_by
            elif created_by in user_org_map:
                org_id = user_org_map[created_by]

            if not org_id:
                logger.warning(
                    f"  [stock_buffers] Could not resolve organization_id for doc {doc.get('_id')}, skipping."
                    )
                continue

            row_data = {
                "id": str(doc.get("_id")),
                "organization_id": org_id,
                "buffer_goal_gms": clean_and_serialize(doc.get("bufferGoal"), 'numeric'),
                "balance_when_set_gms": clean_and_serialize(doc.get("balanceWhenSet"), 'numeric'),
                "buffer_date": clean_and_serialize(doc.get("date")),
                "sync_at": datetime.now(timezone.utc),
            }
            all_rows.append(row_data)

        if not all_rows:
            logger.info("  [stock_buffers] No new/updated records in this window.")
            return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        sql = f'INSERT INTO "stock_buffers" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [stock_buffers] Synced {len(all_rows)} records.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [stock_buffers] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_accounts_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        pg_columns_schema = _get_columns_schema(pg_cursor, "accounts")
        if not pg_columns_schema:
            return

        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["accounts"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            # 1. Nested Objects
            vat_gst = doc.get("vatGstDetails") or {}
            if not isinstance(vat_gst, dict): vat_gst = {}

            kyc_obj = doc.get("kyc") or doc.get("kycDetails") or {}
            if isinstance(kyc_obj, list) and len(kyc_obj) > 0:
                kyc_obj = kyc_obj[0]
            if not isinstance(kyc_obj, dict): kyc_obj = {}

            # 2. Extract Primary Address Details
            addresses = doc.get("addresses") or []
            primary_addr = {}
            if isinstance(addresses, list) and len(addresses) > 0:
                primary_addr = addresses[0] if isinstance(addresses[0], dict) else {}

            email_val = doc.get("email") or primary_addr.get("email")
            phone_val = (
                doc.get("phone1") or doc.get("mobile") or doc.get("companyPhone1") or
                primary_addr.get("phoneNumber1") or primary_addr.get("telephone")
            )
            country_val = doc.get("country") or primary_addr.get("country")

            # 3. Compliance & Name Extractions
            vat_num = (
                vat_gst.get("vatNumber") or 
                vat_gst.get("gstOrVatNumber") or 
                doc.get("vatNumber") or doc.get("vat_number")
            )
            vat_stat = (
                vat_gst.get("vatStatus") or 
                doc.get("vatStatus") or doc.get("vat_status")
            )
            trade_lic = (
                kyc_obj.get("tradeLicense") or 
                doc.get("tradeLicense") or doc.get("tradeLicenseNumber") or doc.get("licenseNo")
            )

            cust_name = (
                doc.get("customerName") or doc.get("accountName") or 
                doc.get("companyName") or doc.get("name") or doc.get("title")
            )

            # 4. Account Type Extraction (Fix for accountType/account_type)
            acc_type_raw = doc.get("accountType") or doc.get("account_type")
            acc_type_val = to_str_id(acc_type_raw) if acc_type_raw else None

            raw_row = {
                "id": str(doc.get("_id")),
                "organization_id": to_str_id(doc.get("OrganizationId") or doc.get("organizationId")),
                "branch_id": to_str_id(doc.get("branchId") or doc.get("branch_id")),
                "account_id": doc.get("accountCode") or doc.get("accountId"),
                "account_code": doc.get("accountCode") or doc.get("accountId"),
                "customer_id": to_str_id(doc.get("customerId")),
                
                # Account Type Matching
                "account_type": acc_type_val,
                "account_type_id": acc_type_val,
                
                "customer_name": cust_name,
                "name": cust_name,
                "email": email_val,
                "phone_number": phone_val,
                "country": country_val,
                "party_type": doc.get("partyType"),
                "is_supplier": doc.get("isSupplier", False),
                
                # Compliance & VAT / KYC Fields
                "vat_status": vat_stat,
                "vat_number": vat_num,
                "trade_license_number": trade_lic,
                "is_active": doc.get("isActive", True),
                "is_deleted": doc.get("isDeleted", False),
                "sync_at": datetime.now(timezone.utc)
            }

            row_data = {k: clean_and_serialize(v, pg_columns_schema[k]) for k, v in raw_row.items() if k in pg_columns_schema}
            all_rows.append(row_data)

        if not all_rows: return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "created_at", "sync_at")])
        sql = f'INSERT INTO "accounts" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [accounts] Custom sync finished: {len(all_rows)} records processed.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [accounts] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()


def sync_company_accounts_custom(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        table_name = "company_accounts"
        pg_columns_schema = _get_columns_schema(pg_cursor, table_name)
        if not pg_columns_schema:
            table_name = "companyaccounts"
            pg_columns_schema = _get_columns_schema(pg_cursor, table_name)
        
        if not pg_columns_schema:
            return

        needs_created_at_backfill = False
        if "created_at" in pg_columns_schema:
            pg_cursor.execute(
                f'''SELECT
                    NOT EXISTS (SELECT 1 FROM "{table_name}")
                    OR EXISTS (SELECT 1 FROM "{table_name}" WHERE created_at IS NULL);'''
            )
            needs_created_at_backfill = bool(pg_cursor.fetchone()[0])

        mongo_query = {} if (INITIAL_RUN or needs_created_at_backfill) else default_cron_query
        if needs_created_at_backfill:
            logger.info(f"  [{table_name}] Backfilling missing created_at values from all Mongo documents.")
        mongo_cursor = mongo_db["companyaccounts"].find(mongo_query)
        all_rows = []

        for doc in mongo_cursor:
            kyc_obj = doc.get("kyc") or {}
            if not isinstance(kyc_obj, dict): kyc_obj = {}

            vat_gst = doc.get("vatGstDetails") or {}
            if not isinstance(vat_gst, dict): vat_gst = {}

            trade_lic = (
                kyc_obj.get("tradeLicense") or 
                doc.get("tradeLicense") or doc.get("trade_license")
            )
            kyc_vat_num = (
                kyc_obj.get("gstOrVatNumber") or 
                vat_gst.get("vatNumber") or 
                doc.get("vatNumber")
            )

            created_at = (
                doc.get("createdAt")
                or doc.get("created_at")
                or doc.get("creationDate")
            )
            if created_at is None and isinstance(doc.get("_id"), ObjectId):
                created_at = doc["_id"].generation_time

            raw_row = {
                "id": str(doc.get("_id")),
                "account_id": doc.get("accountId"),
                "company_name": doc.get("companyName"),
                "company_email": doc.get("companyEmail"),
                "company_phone1": doc.get("companyPhone1"),
                "account_status": doc.get("accountStatus"),
                
                # Compliance Fixes
                "kyc_trade_license": trade_lic,
                "kyc_vat_number": kyc_vat_num,
                "is_lead": doc.get("isLead", False),
                "created_at": clean_and_serialize(created_at),
                "source_id": to_str_id(doc.get("source")),
                "sync_at": datetime.now(timezone.utc)
            }

            row_data = {k: v for k, v in raw_row.items() if k in pg_columns_schema}
            all_rows.append(row_data)

        if not all_rows: return

        managed_columns = list(all_rows[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in managed_columns])
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in managed_columns if c not in ("id", "sync_at")])
        sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        
        batch = [tuple(r.get(c) for c in managed_columns) for r in all_rows]
        execute_values(pg_cursor, sql, batch, page_size=BATCH_SIZE)
        pg_conn.commit()
        logger.info(f"  [{table_name}] Custom sync finished: {len(all_rows)} records processed.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [company_accounts] Custom sync FAILED: {e}")
    finally:
        pg_cursor.close()


def sync_metal_transactions(mongo_db, pg_conn, default_cron_query):
    pg_cursor = pg_conn.cursor()
    try:
        pg_columns_schema = _get_columns_schema(pg_cursor, "metal_transactions")
        if not pg_columns_schema:
            return

        # 1. Division Lookup
        pg_cursor.execute("SELECT id, description FROM divisions WHERE id IS NOT NULL;")
        division_lookup = {r[0]: r[1] for r in pg_cursor.fetchall()}

        mongo_query = {} if INITIAL_RUN else default_cron_query
        mongo_cursor = mongo_db["metaltransactions"].find(mongo_query)
        all_txs = []

        for tx in mongo_cursor:
            stock_items = tx.get("stockItems") or tx.get("stockitems") or []
            first_item = stock_items[0] if stock_items else {}
            
            # --- Total Summary Extraction ---
            total_sum = tx.get("totalSummary") or tx.get("totalsummary") or {}
            if not isinstance(total_sum, dict):
                total_sum = {}

            sub_total_val = (
                total_sum.get("itemSubTotal") or total_sum.get("item_sub_total") or 
                tx.get("itemSubTotal") or tx.get("subTotal")
            )
            vat_val = (
                total_sum.get("itemTotalVat") or total_sum.get("item_total_vat") or 
                tx.get("itemTotalVat") or tx.get("vatAmount") or 0.0
            )
            net_amount_val = (
                total_sum.get("netAmount") or total_sum.get("net_amount") or 
                tx.get("netAmount") or tx.get("net_amount")
            )
            total_amount_val = (
                total_sum.get("totalAmount") or total_sum.get("total_amount") or 
                total_sum.get("itemTotalAmount") or tx.get("totalAmount") or net_amount_val
            )

            # --- Metal Rate Extraction ---
            metal_rate_unit = tx.get("metalRateUnit") or tx.get("metal_rate_unit") or {}
            metal_rate_val = None
            if isinstance(metal_rate_unit, dict):
                metal_rate_val = metal_rate_unit.get("rate") or metal_rate_unit.get("amount")
            elif isinstance(metal_rate_unit, (int, float)):
                metal_rate_val = metal_rate_unit

            if not metal_rate_val:
                metal_rate_val = tx.get("metalRate") or tx.get("metal_rate")

            if not metal_rate_val and first_item:
                rate_req = first_item.get("metalRateRequirements") or {}
                rate_in_gram = rate_req.get("rateInGram")
                if isinstance(rate_in_gram, dict):
                    metal_rate_val = rate_in_gram.get("amount") or rate_in_gram.get("rate")
                else:
                    metal_rate_val = rate_in_gram

            # --- Currency Rates ---
            currency_rate_val = (
                tx.get("itemCurrencyRate") or tx.get("partyCurrencyRate") or 
                tx.get("currencyRate") or tx.get("currency_rate") or 
                first_item.get("currencyRate") or 1.0
            )

            # --- Status Flags ---
            is_fixed = tx.get("fixed") if tx.get("fixed") is not None else tx.get("isFixed")
            if is_fixed is None: is_fixed = tx.get("is_fixed", False)

            is_hedge = tx.get("hedge") if tx.get("hedge") is not None else tx.get("isHedge")
            if is_hedge is None: is_hedge = tx.get("is_hedge", False)

            is_deleted = tx.get("isDeleted") if tx.get("isDeleted") is not None else not tx.get("isActive", True)

            # --- Division ---
            raw_division = tx.get("division") or tx.get("divisionId") or tx.get("division_id")
            m_division_id = to_str_id(raw_division)
            # --- Party Currency ---
            party_curr_id = tx.get("partyCurrency") or tx.get("partyCurrencyId") or tx.get("currencyId")

            raw_row_data = {
                "id": to_str_id(tx.get("_id")),
                "organization_id": to_str_id(tx.get("OrganizationId") or tx.get("organizationId") or tx.get("organization_id")),
                "branch_id": to_str_id(tx.get("branchId") or tx.get("branch_id")),
                "voucher_number": tx.get("voucherNumber") or tx.get("voucher_number"),
                "voucher_type": tx.get("voucherType") or tx.get("voucher_type"),
                "transaction_type": tx.get("transactionType") or tx.get("transaction_type"),
                "voucher_date": clean_and_serialize(tx.get("voucherDate")),
                "status": tx.get("status"),
                "remarks": tx.get("remarks"),
                "notes": tx.get("notes"),
                
                # Flags
                "is_fixed": bool(is_fixed),
                "is_hedge": bool(is_hedge),
                "is_deleted": bool(is_deleted),
                
                # Foreign Keys
                "party_id": to_str_id(tx.get("partyCode") or tx.get("party_id")),
                "party_currency_id": to_str_id(party_curr_id),
                "division_id": m_division_id,
                "currency_code": first_item.get("currencyCode") or tx.get("currencyCode"),
                "salesman_id": to_str_id(tx.get("salesman")),
                
                # Financials
                "currency_rate": clean_and_serialize(currency_rate_val, 'numeric'),
                "metal_rate": clean_and_serialize(metal_rate_val, 'numeric'),
                "item_sub_total": clean_and_serialize(sub_total_val, 'numeric'),
                "item_total_vat": clean_and_serialize(vat_val, 'numeric'),
                "net_amount": clean_and_serialize(net_amount_val, 'numeric'),
                "total_amount": clean_and_serialize(total_amount_val, 'numeric'),
                "item_total_amount": clean_and_serialize(total_amount_val, 'numeric'),
                
                # Stock Item Summary
                "gross_weight": clean_and_serialize(first_item.get("grossWeight"), 'numeric'),
                "pure_weight": clean_and_serialize(first_item.get("pureWeight"), 'numeric'),
                "purity": clean_and_serialize(first_item.get("purity"), 'numeric'),
                "pieces": clean_and_serialize(first_item.get("pieces"), pg_columns_schema.get("pieces")),
                
                "sync_at": datetime.now(timezone.utc)
            }

            row_data = {k: v for k, v in raw_row_data.items() if k in pg_columns_schema}

            all_txs.append(row_data)

        if not all_txs:
            logger.info("  [metal_transactions] No documents found.")
            return

        columns_list = list(all_txs[0].keys())
        columns_str = ", ".join([f'"{c}"' for c in columns_list])
        
        update_cols = [c for c in columns_list if c not in ("id", "created_at")]
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        
        sql = f'INSERT INTO "metal_transactions" ({columns_str}) VALUES %s ON CONFLICT (id) DO UPDATE SET {update_str};'
        
        batch_rows = []
        total_synced = 0
        for r_data in all_txs:
            batch_rows.append(tuple(r_data.get(col, None) for col in columns_list))
            if len(batch_rows) >= BATCH_SIZE:
                execute_values(pg_cursor, sql, batch_rows, page_size=BATCH_SIZE)
                total_synced += len(batch_rows)
                batch_rows = []

        if batch_rows:
            execute_values(pg_cursor, sql, batch_rows, page_size=BATCH_SIZE)
            total_synced += len(batch_rows)

        pg_conn.commit()
        logger.info(f"  [metal_transactions] Synced {total_synced} transactions successfully.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [metal_transactions] FAILED: {e}")
    finally:
        pg_cursor.close()

def sync_metal_transaction_items(mongo_db, pg_conn, default_cron_query, initial_run, log_prefix):
    pg_cursor = pg_conn.cursor()
    try:
        pg_columns_schema = _get_columns_schema(pg_cursor, "metal_transaction_items")
        if not pg_columns_schema:
            return

        id_is_int = pg_columns_schema.get("id") in ("integer", "bigint")
        if pg_columns_schema.get("transaction_id") in ("integer", "bigint"):
            logger.warning(
                f"{log_prefix}  [metal_transaction_items] Skipped: transaction_id must accept Mongo string ids."
            )
            return

        pg_cursor.execute('SELECT MAX(voucher_date) FROM "metal_transaction_items";')
        last_voucher_date = pg_cursor.fetchone()[0]
        if initial_run or last_voucher_date is None:
            mongo_query = {}
        else:
            mongo_query = {
                "$or": [
                    default_cron_query,
                    {"voucherDate": {"$gte": last_voucher_date}},
                ]
            }

        mongo_cursor = mongo_db["metaltransactions"].find(mongo_query)
        all_items = []
        affected_transaction_ids = []

        def numeric_value(value):
            """Unwrap Mongo amount/rate objects before writing numerics."""
            if isinstance(value, dict):
                for key in ("amount", "rate", "value", "percentage", "currentBidValue"):
                    if value.get(key) is not None:
                        return numeric_value(value[key])
                return 0
            return clean_and_serialize(value, "numeric")

        for tx in mongo_cursor:
            transaction_id = to_str_id(tx.get("_id"))
            if not transaction_id:
                continue
            affected_transaction_ids.append(transaction_id)

            stock_items = tx.get("stockItems") or tx.get("stockitems") or []
            if not stock_items:
                continue

            total_sum = tx.get("totalSummary") or tx.get("totalsummary") or {}

            for item_index, item in enumerate(stock_items):
                source_item_id = (
                    to_str_id(item.get("_id"))
                    or f"{transaction_id}_{item_index}"
                )
                rate_req = item.get("metalRateRequirements", {}) or {}
                item_total = item.get("itemTotal", {}) or {}
                making_unit = item.get("makingUnit", {}) or {}
                premium_disc = item.get("premiumDiscount", {}) or {}
                vat_details = item.get("vat", {}) or {}

                row_data = {
                    "source_item_id": source_item_id,
                    "transaction_id": transaction_id,
                    "organization_id": to_str_id(tx.get("OrganizationId") or tx.get("organizationId") or tx.get("organization_id")),
                    "branch_id": to_str_id(tx.get("branchId") or tx.get("branch_id")),
                    "voucher_number": tx.get("voucherNumber"),
                    "transaction_type": tx.get("transactionType"),
                    "voucher_date": clean_and_serialize(tx.get("voucherDate")),
                    "status": tx.get("status"),
                    "is_fixed": tx.get("fixed", False),
                    "is_hedge": tx.get("hedge", False),
                    "is_deleted": tx.get("isDeleted", False),
                    "party_id": to_str_id(tx.get("partyCode")),
                    "division_id": to_str_id(tx.get("division")),
                    "division_code": tx.get("divisionCode"),
                    "division_name": tx.get("divisionName"),
                    "stock_code": to_str_id(item.get("stockCode") or item.get("stock_code")),
                    "stock_description": item.get("description"),
                    "gross_weight": numeric_value(item.get("grossWeight")),
                    "pure_weight": numeric_value(item.get("pureWeight")),
                    "purity": numeric_value(item.get("purity")),
                    "pieces": clean_and_serialize(item.get("pieces"), pg_columns_schema.get("pieces")),
                    "weight_in_oz": numeric_value(item.get("weightInOz")),
                    "currency_code": item.get("currencyCode"),
                    "currency_rate": numeric_value(item.get("currencyRate")),
                    "fx_gain": numeric_value(item.get("FXGain")),
                    "fx_loss": numeric_value(item.get("FXLoss")),
                    "rate_in_gram": numeric_value(rate_req.get("rateInGram")),
                    "bid_value": numeric_value(rate_req.get("currentBidValue")),
                    "base_amount": numeric_value(item_total.get("baseAmount")),
                    "rate_amount": numeric_value(item_total.get("baseAmount")),
                    "making_unit": making_unit.get("unit"),
                    "making_rate": numeric_value(making_unit.get("makingRate")),
                    "making_amount": numeric_value(making_unit.get("makingAmount")),
                    "premium_amount": numeric_value(premium_disc.get("amount")),
                    "premium_rate": numeric_value(premium_disc.get("rate")),
                    "premium_type": premium_disc.get("type"),
                    "vat_percentage": numeric_value(vat_details.get("percentage")),
                    "vat_amount": numeric_value(vat_details.get("amount")),
                    "making_charges_total": numeric_value(item_total.get("makingChargesTotal")),
                    "sub_total": numeric_value(item_total.get("subTotal")),
                    "item_total_amount": numeric_value(item_total.get("itemTotalAmount")),
                    "item_status": item.get("itemStatus"),
                    "total_amount": numeric_value(total_sum.get("totalAmount")),
                    "item_total_vat": numeric_value(total_sum.get("itemTotalVat")),
                }

                if not id_is_int:
                    row_data["id"] = to_str_id(item.get("_id")) or f"{transaction_id}_{item_index}"

                all_items.append({
                    key: value for key, value in row_data.items()
                    if key in pg_columns_schema
                })

        unique_transaction_ids = list(set(affected_transaction_ids))
        if not unique_transaction_ids:
            logger.info(f"{log_prefix}  [metal_transaction_items] No affected transactions found.")
            return

        if id_is_int:
            pg_cursor.execute(
                'DELETE FROM "metal_transaction_items" WHERE transaction_id = ANY(%s);',
                (unique_transaction_ids,),
            )

        if not all_items:
            pg_conn.commit()
            logger.info(
                f"{log_prefix}  [metal_transaction_items] Removed stale items for "
                f"{len(unique_transaction_ids)} transactions."
            )
            return

        managed_columns = list(all_items[0].keys())
        columns_str = ", ".join([f'"{column}"' for column in managed_columns])

        if not id_is_int:
            update_str = ", ".join([
                f'"{column}" = EXCLUDED."{column}"'
                for column in managed_columns
                if column not in ("id", "sync_at", "transaction_id")
            ])
            sql = (
                f'INSERT INTO "metal_transaction_items" ({columns_str}) VALUES %s '
                f'ON CONFLICT (id) DO UPDATE SET {update_str};'
            )
        else:
            sql = f'INSERT INTO "metal_transaction_items" ({columns_str}) VALUES %s;'

        batch_rows = []
        total_synced = 0
        for row_data in all_items:
            batch_rows.append(tuple(row_data.get(column) for column in managed_columns))
            if len(batch_rows) >= BATCH_SIZE:
                execute_values(pg_cursor, sql, batch_rows, page_size=BATCH_SIZE)
                total_synced += len(batch_rows)
                batch_rows = []

        if batch_rows:
            execute_values(pg_cursor, sql, batch_rows, page_size=BATCH_SIZE)
            total_synced += len(batch_rows)

        pg_conn.commit()
        logger.info(f"{log_prefix}  [metal_transaction_items] Synced {total_synced} items.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"{log_prefix}  [metal_transaction_items] FAILED sync: {e}")
    finally:
        pg_cursor.close()


def sync_role_permissions(mongo_db, pg_conn):
    pg_cursor = pg_conn.cursor()
    try:
        pg_cursor.execute('TRUNCATE TABLE "role_permissions" RESTART IDENTITY;')
        rows = []

        for role in mongo_db["organizationroles"].find({}):
            role_id = str(role.get("_id"))
            permissions = role.get("permissions", [])
            if not isinstance(permissions, list):
                continue
            for perm in permissions:
                module_id = perm.get("moduleName") or perm.get("moduleId") if isinstance(perm, dict) else perm
                if not module_id:
                    continue
                module_id = to_str_id(module_id)  
                rows.append((role_id, module_id))

        if rows:
            execute_values(
                pg_cursor,
                'INSERT INTO "role_permissions" (role_id, module_id) VALUES %s ON CONFLICT DO NOTHING;',
                rows
            )
            pg_conn.commit()
            logger.info(f"  [role_permissions] Synced {len(rows)} mappings.")
        else:
            pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [role_permissions] FAILED: {e}")
    finally:
        pg_cursor.close()


def sync_user_branch_roles(mongo_db, pg_conn):
    pg_cursor = pg_conn.cursor()
    try:
        pg_cursor.execute('TRUNCATE TABLE "user_branch_roles" RESTART IDENTITY;')
        rows = []

        for user in mongo_db["usermanagements"].find({}):
            user_id = str(user.get("_id"))
            branches_arr = user.get("branches", [])
            if not isinstance(branches_arr, list):
                continue
            for b_node in branches_arr:
                if not isinstance(b_node, dict):
                    continue
                branch_id = to_str_id(b_node.get("branch")) if b_node.get("branch") else None
                roles_arr = b_node.get("roles", [])
                if not (branch_id and isinstance(roles_arr, list)):
                    continue
                for role_id in roles_arr:
                    if role_id:
                        rows.append((user_id, branch_id, to_str_id(role_id)))

        if rows:
            execute_values(
                pg_cursor,
                'INSERT INTO "user_branch_roles" (user_id, branch_id, role_id) VALUES %s ON CONFLICT DO NOTHING;',
                rows
            )
            pg_conn.commit()
            logger.info(f"  [user_branch_roles] Synced {len(rows)} combinations.")
        else:
            pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"  [user_branch_roles] FAILED: {e}")
    finally:
        pg_cursor.close()


def run_migration(name, mongo_uri, mongo_db_name, pg_host, pg_port, pg_database,
                   pg_user, pg_password, initial_run=False, sync_window_minutes=None):
    """
    Runs the FULL sync (branches, standard tables, nested arrays, metal
    transactions, role permissions, user-branch-roles, all the *_custom
    tables, and the mandatory Postgres refresh functions) for ONE
    Mongo -> Postgres pair.
    """
    global INITIAL_RUN, MONGO_URI, MONGO_DB, PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, _CURRENT_INSTANCE

    _CURRENT_INSTANCE = name
    INITIAL_RUN = initial_run
    MONGO_URI, MONGO_DB = mongo_uri, mongo_db_name
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD = pg_host, pg_port, pg_database, pg_user, pg_password
    window_minutes = sync_window_minutes if sync_window_minutes is not None else SYNC_WINDOW_MINUTES

    lock_file = get_lock_file(name)
    try:
        acquire_lock(lock_file)
    except SyncAlreadyRunningError as e:
        logger.warning(str(e))
        return
    except Exception as e:
        logger.error(f"Could not acquire lock: {e}")
        return

    mongo_client = None
    pg_conn = None
    try:
        time_filter = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        default_cron_query = {"updatedAt": {"$gte": time_filter}}

        mongo_client = MongoClient(mongo_uri, datetime_conversion='DATETIME_AUTO', serverSelectionTimeoutMS=10000)
        mongo_db = mongo_client[mongo_db_name]
        mongo_collections_map = {n.lower().replace("_", ""): n for n in mongo_db.list_collection_names()}

        pg_conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_database,
                                    user=pg_user, password=pg_password, connect_timeout=10)

        logger.info("-" * 60)
        logger.info("Syncing branches first...")
        sync_branches_custom(mongo_db, pg_conn, default_cron_query)
        branch_to_org, org_to_branch = build_relationship_maps(pg_conn)
        pg_cursor_temp = pg_conn.cursor()
        pg_cursor_temp.execute("SELECT id, organization_id, branch_id, division_id FROM metal_transactions;")
        metal_transaction_lookup = {str(r[0]): (r[1], r[2], r[3]) for r in pg_cursor_temp.fetchall()}
        pg_cursor_temp.close()

        tables_list = get_tables_list(pg_conn, exclude_tables=EXCLUDE_TABLES)

        logger.info("-" * 60)
        logger.info("Syncing standard mapped collections...")
        for table_name in tables_list:
            if table_name in SPECIAL_TABLES or table_name in NESTED_ARRAY_SYNC_CONFIG:
                continue
            cleaned_pg_name = table_name.lower().replace("_", "")
            if cleaned_pg_name in mongo_collections_map:
                sync_collection(
                    mongo_db, pg_conn,
                    table_name,
                    mongo_collections_map[cleaned_pg_name],
                    default_cron_query, branch_to_org, org_to_branch,
                    metal_transaction_lookup=metal_transaction_lookup
                )

        logger.info("-" * 60)
        logger.info("Running nested-array sync fixes...")
        sync_all_nested_arrays_generic(mongo_db, pg_conn, default_cron_query)
        sync_opportunity_stage_history(mongo_db, pg_conn, default_cron_query)
        sync_metal_transactions(mongo_db, pg_conn, default_cron_query)
        sync_role_permissions(mongo_db, pg_conn)
        sync_user_branch_roles(mongo_db, pg_conn)
        sync_divisions_custom(mongo_db, pg_conn, default_cron_query)
        sync_stock_buffers_custom(mongo_db, pg_conn, default_cron_query)
        sync_deal_strategies_custom(mongo_db, pg_conn, default_cron_query)
        sync_deal_orders_custom(mongo_db, pg_conn, default_cron_query, branch_to_org)
        sync_accounts_custom(mongo_db, pg_conn, default_cron_query)
        sync_company_accounts_custom(mongo_db, pg_conn, default_cron_query)
        sync_organizations_custom(mongo_db, pg_conn, default_cron_query)
        sync_metal_transaction_items(mongo_db, pg_conn, default_cron_query, INITIAL_RUN, f"{name}:")

        logger.info("-" * 60)
        logger.info("Executing mandatory PostgreSQL refresh functions...")
        try:
            pg_cursor = pg_conn.cursor()
            logger.info("Truncating snapshot tables...")
            pg_cursor.execute('TRUNCATE TABLE "ai_stock_ledger" RESTART IDENTITY;')
            pg_cursor.execute('TRUNCATE TABLE "ai_user_permissions" RESTART IDENTITY;')
            pg_conn.commit()

            refresh_functions = [
                "refresh_ai_tables()",
                "refresh_ai_own_stock_position()",
                "refresh_reg_tables()",
                "refresh_monthly_summary()",
                "check_ml_retrain_needed()",
                "refresh_deal_strategy_tables()",
                "refresh_inventory_view()",
                "refresh_risk_dashboard_currency_breakdown()",
                "refresh_party_cancellation_stats()",
                "refresh_party_branch_org_balances()",
                "refresh_extended_reporting_tables()"
            ]

            for fn in refresh_functions:
                try:
                    logger.info(f"Executing: SELECT {fn};")
                    pg_cursor.execute(f"SELECT {fn};")
                    pg_conn.commit()
                except Exception as fn_err:
                    pg_conn.rollback()
                    logger.error(f"Failed to execute {fn}: {fn_err}")

            pg_conn.commit()
            pg_cursor.close()
            logger.info("All AI analytics completed successfully.")
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"Post-sync database refresh functions FAILED: {e}")

        logger.info("Migration completed successfully.")

    except Exception:
        logger.exception("Migration FAILED with an unexpected error.")
    finally:
        if pg_conn is not None:
            pg_conn.close()
        if mongo_client is not None:
            mongo_client.close()
        release_lock(lock_file)
        _CURRENT_INSTANCE = "default"
