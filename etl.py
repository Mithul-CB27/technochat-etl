"""
etl.py — Extract from DigitalOcean PostgreSQL → Push to Supabase
Orders  : incremental by updated_at — PK (order_id, variant_id)
Products: full replace every run    — PK variant_id
"""

import os
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "etl.env"))

TARGET_DB_URL = os.getenv("TARGET_DB_URL")
SOURCE_PASS   = os.getenv("SOURCE_DB_PASSWORD")

SOURCE_DB_URL = (
    "postgresql://doadmin:{password}"
    "@db-postgresql-blr1-99203-do-user-14584069-0.b.db.ondigitalocean.com"
    ":25060/technosport-prod"
    "?sslmode=require"
).format(password=SOURCE_PASS)

# ── Logging ───────────────────────────────────────────────────────────────────
log_path = os.path.join(BASE_DIR, "etl.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# QUERIES
# ═══════════════════════════════════════════════════════════════════════════════

QUERY_ORDERS = """
SELECT
  o.display_id                                      AS order_id,
  o.created_at                                      AS order_date,
  (o.shipped_at::date   + INTERVAL '1 day')::date   AS dispatch_date,
  (o.delivered_at::date + INTERVAL '1 day')::date   AS delivered_date,
  c.id                                              AS customer_id,
  c.first_name                                      AS retailer_name,
  c.phone                                           AS contact,
  p.ts_style                                        AS style,
  pv.id                                             AS variant_id,
  p.id                                              AS product_id,
  li.description                                    AS size,
  li.quantity,
  td.business_name                                  AS distributor,
  CAST(
    TO_CHAR(CAST(li.unit_price AS DECIMAL(10,2)) * li.quantity / 100, 'FM999999999.00')
    AS NUMERIC
  )                                                 AS line_item_amount,
  CAST(
    TO_CHAR((COALESCE(ps.amount, 0) + COALESCE(ll.discount_value, 0)) / 100.0, 'FM999999999.00')
    AS NUMERIC
  )                                                 AS total_amount,
  ps.data ->> 'mode'                                AS mode,
  a.city,
  a.province,
  c.ts_pincode,
  CASE
    WHEN (ct.context ->> 'user_agent') ILIKE 'okhttp%%' THEN 'android'
    WHEN (ct.context ->> 'user_agent') ILIKE 'iphone'   THEN 'iphone'
    ELSE 'web'
  END                                               AS platform
FROM "order" o
LEFT JOIN customer c          ON c.id  = o.customer_id
LEFT JOIN address a           ON a.id  = o.billing_address_id
LEFT JOIN tc_distributor td   ON td.id = o.distributor_id
LEFT JOIN payment_session ps  ON ps.cart_id = o.cart_id
LEFT JOIN line_item li        ON li.order_id = o.id
LEFT JOIN product_variant pv  ON pv.id = li.variant_id
LEFT JOIN product p           ON p.id  = pv.product_id
LEFT JOIN cart ct             ON ct.id = o.cart_id
LEFT JOIN (
    SELECT reference_id, SUM(discount_value) AS discount_value
    FROM loyalty_ledger
    GROUP BY reference_id
) ll ON ll.reference_id = o.id
WHERE o.ts_order_status NOT IN ('not placed', 'payment pending')
  AND ps.provider_id   = 'payu'
  AND c.ts_pincode    != '888888'
  AND o.updated_at     > :last_sync_at
ORDER BY o.updated_at ASC
"""

QUERY_PRODUCTS = """
WITH price_base AS (
    SELECT DISTINCT ON (variant_id)
        variant_id,
        (amount::numeric / 100) AS price,
        currency_code,
        created_at
    FROM money_amount
    WHERE region_id     IS NULL
      AND price_list_id IS NULL
      AND (min_quantity IS NULL OR min_quantity = 1)
    ORDER BY variant_id, created_at DESC
),
category_split AS (
    SELECT
        pcp.product_id,
        split_part(pc.handle, '/', 1) AS category_level_1,
        split_part(pc.handle, '/', 2) AS category_level_2,
        split_part(pc.handle, '/', 3) AS category_level_3
    FROM product_category_product pcp
    JOIN product_category pc ON pc.id = pcp.product_category_id
)
SELECT
    p.id         AS product_id,
    p.ts_style   AS style,
    p.title      AS product_title,
    p.created_at AS launch_date,
    pv.id        AS variant_id,
    pv.title     AS size,
    pb.price,
    MAX(cs.category_level_1) AS category_level_1,
    MAX(cs.category_level_2) AS category_level_2,
    MAX(cs.category_level_3) AS category_level_3
FROM product p
JOIN  product_variant pv    ON p.id  = pv.product_id
LEFT JOIN price_base pb     ON pb.variant_id = pv.id
LEFT JOIN category_split cs ON p.id  = cs.product_id
WHERE p.deleted_at  IS NULL
  AND pv.deleted_at IS NULL
GROUP BY p.id, p.ts_style, p.title, p.created_at, pv.id, pv.title, pb.price
ORDER BY p.id, pv.id
"""


# ═══════════════════════════════════════════════════════════════════════════════
# DB CONNECTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_engines():
    log.info("Connecting to DigitalOcean PostgreSQL...")
    source = create_engine(
        SOURCE_DB_URL,
        connect_args={"connect_timeout": 30, "sslmode": "require"},
        pool_pre_ping=True,
    )
    log.info("Connecting to Supabase...")
    target = create_engine(
        TARGET_DB_URL,
        connect_args={"connect_timeout": 30},
        pool_pre_ping=True,
    )
    return source, target


def test_connections(source_engine, target_engine):
    try:
        with source_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✓ DigitalOcean connected")
    except Exception as e:
        raise ConnectionError(f"Source DB failed: {e}")
    try:
        with target_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✓ Supabase connected")
    except Exception as e:
        raise ConnectionError(f"Target DB failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC LOG
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_sync_log(target_engine):
    with target_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS _etl_sync_log (
                table_name   VARCHAR PRIMARY KEY,
                last_sync_at TIMESTAMP,
                rows_synced  INTEGER,
                status       VARCHAR,
                error        TEXT
            )
        """))


def get_last_sync_at(target_engine) -> str:
    try:
        with target_engine.connect() as conn:
            row = conn.execute(text("""
                SELECT last_sync_at FROM _etl_sync_log
                WHERE table_name = 'orders'
                  AND status = 'success'
                  AND last_sync_at IS NOT NULL
            """)).fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception:
        pass
    return '2000-01-01 00:00:00'


def log_sync(table_name, rows, status, error, target_engine):
    with target_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO _etl_sync_log (table_name, last_sync_at, rows_synced, status, error)
            VALUES (:t, NOW(), :r, :s, :e)
            ON CONFLICT (table_name) DO UPDATE SET
                last_sync_at = NOW(),
                rows_synced  = :r,
                status       = :s,
                error        = :e
        """), {"t": table_name, "r": rows, "s": status, "e": error})


# ═══════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ═══════════════════════════════════════════════════════════════════════════════

def extract_orders(source_engine, last_sync_at: str) -> pd.DataFrame:
    log.info(f"[orders] Fetching orders updated after {last_sync_at}...")
    with source_engine.connect() as conn:
        df = pd.read_sql(text(QUERY_ORDERS), conn,
                         params={"last_sync_at": last_sync_at})
    log.info(f"[orders] Extracted {len(df):,} rows")
    return df


def extract_products(source_engine) -> pd.DataFrame:
    log.info("[products] Extracting all products...")
    with source_engine.connect() as conn:
        df = pd.read_sql(text(QUERY_PRODUCTS), conn)
    log.info(f"[products] Extracted {len(df):,} rows")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# CLEAN
# ═══════════════════════════════════════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    # Normalise column names
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    if table_name == "orders":
        for col in ["line_item_amount", "total_amount", "quantity"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in ["order_date", "dispatch_date", "delivered_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
                df[col] = df[col].dt.tz_localize(None)

        for col in ["city", "province", "retailer_name", "distributor",
                    "platform", "style", "size", "mode"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "None": None, "": None})

        # Drop duplicate (order_id, variant_id) rows — keep last
        before = len(df)
        df = df.drop_duplicates(subset=["order_id", "variant_id"], keep="last")
        if len(df) < before:
            log.info(f"[orders] Dropped {before - len(df):,} duplicate (order_id, variant_id) rows")

    if table_name == "products":
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

        if "launch_date" in df.columns:
            df["launch_date"] = pd.to_datetime(df["launch_date"], errors="coerce", utc=True)
            df["launch_date"] = df["launch_date"].dt.tz_localize(None)

        for col in ["style", "product_title", "size",
                    "category_level_1", "category_level_2", "category_level_3"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "None": None, "": None})

    # Replace all remaining NaN / NaT with None
    df = df.where(pd.notnull(df), other=None)

    log.info(f"[{table_name}] Clean done — {len(df):,} rows ready")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD
# ═══════════════════════════════════════════════════════════════════════════════

def pg_type(dtype) -> str:
    s = str(dtype)
    if "int"      in s: return "BIGINT"
    if "float"    in s: return "NUMERIC"
    if "datetime" in s: return "TIMESTAMP"
    if "bool"     in s: return "BOOLEAN"
    return "TEXT"


def ensure_main_table(table_name, df, pk_cols, target_engine):
    """Create main table with PK if it doesn't exist yet."""
    col_defs = ", ".join([f'"{c}" {pg_type(df[c].dtype)}' for c in df.columns])
    conflict  = ", ".join([f'"{c}"' for c in pk_cols])
    with target_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {col_defs},
                PRIMARY KEY ({conflict})
            )
        """))
    log.info(f"[{table_name}] Table ready — PK ({conflict})")


def load_orders(df: pd.DataFrame, target_engine) -> int:
    """
    1. Ensure main table exists with PK (order_id, variant_id)
    2. Write to staging table via pandas (no constraints, fast)
    3. DELETE rows from main where PK matches staging
    4. INSERT all staging rows into main
    5. Drop staging
    """
    pk_cols  = ["order_id", "variant_id"]
    cols     = list(df.columns)
    col_list = ", ".join([f'"{c}"' for c in cols])
    staging  = "_staging_orders"

    ensure_main_table("orders", df, pk_cols, target_engine)

    log.info(f"[orders] Writing {len(df):,} rows to staging table...")
    df.to_sql(
        name=staging,
        con=target_engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi"
    )
    log.info(f"[orders] Staging loaded — swapping into main...")

    pk_match = " AND ".join([
        f'"orders"."{c}" = "{staging}"."{c}"' for c in pk_cols
    ])
    with target_engine.begin() as conn:
        result = conn.execute(text(
            f'DELETE FROM "orders" USING "{staging}" WHERE {pk_match}'
        ))
        log.info(f"[orders] Deleted {result.rowcount:,} stale rows")

        conn.execute(text(
            f'INSERT INTO "orders" ({col_list}) SELECT {col_list} FROM "{staging}"'
        ))
        log.info(f"[orders] Inserted {len(df):,} rows into main")

        conn.execute(text(f'DROP TABLE IF EXISTS "{staging}"'))

    log.info(f"[orders] ✓ {len(df):,} rows loaded")
    return len(df)


def load_products(df: pd.DataFrame, target_engine) -> int:
    """Full replace every run — products table is small."""
    df.to_sql(
        name="products",
        con=target_engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi"
    )
    log.info(f"[products] ✓ Replaced with {len(df):,} rows")
    return len(df)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def run_etl():
    log.info("=" * 60)
    log.info(f"ETL started — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        source_engine, target_engine = get_engines()
        test_connections(source_engine, target_engine)
    except Exception as e:
        log.error(f"Startup failed: {e}")
        return

    ensure_sync_log(target_engine)

    # ── ORDERS ────────────────────────────────────────────────────────────
    try:
        last_sync_at = get_last_sync_at(target_engine)
        log.info(f"[orders] Last sync: {last_sync_at}")

        df_orders = extract_orders(source_engine, last_sync_at)
        df_orders = clean_dataframe(df_orders, "orders")

        if not df_orders.empty:
            rows = load_orders(df_orders, target_engine)
            log_sync("orders", rows, "success", None, target_engine)
        else:
            log.info("[orders] No new or updated orders — nothing to sync")
            log_sync("orders", 0, "success", None, target_engine)

    except Exception as e:
        log.error(f"[orders] FAILED: {e}")
        log_sync("orders", 0, "failed", str(e), target_engine)

    # ── PRODUCTS ──────────────────────────────────────────────────────────
    try:
        df_products = extract_products(source_engine)
        df_products = clean_dataframe(df_products, "products")
        rows = load_products(df_products, target_engine)
        log_sync("products", rows, "success", None, target_engine)

    except Exception as e:
        log.error(f"[products] FAILED: {e}")
        log_sync("products", 0, "failed", str(e), target_engine)

    log.info("=" * 60)
    log.info(f"ETL complete — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)


if __name__ == "__main__":
    run_etl()
