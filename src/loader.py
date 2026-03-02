"""
Snowflake loader — connects via RSA key-pair auth and manages
scraped data in the RETAIL_ANALYZER database.
"""

import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

DATABASE = "RETAIL_ANALYZER"
SCHEMA = "GOOGLE_SHOPPING"
SETUP_SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "setup.sql"

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_WAREHOUSE",
]


def check_env():
    """Validate that all required environment variables are set."""
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        print("[!] Missing required environment variables:")
        for v in missing:
            print(f"      - {v}")
        sys.exit(1)


def get_connection():
    """Build a Snowflake connection using key-pair auth from env vars."""
    check_env()

    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    params = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_bytes,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )

    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        params["role"] = role

    return snowflake.connector.connect(**params)


def run_setup(conn):
    """Execute the DDL statements from sql/setup.sql."""
    print("[*] Running Snowflake setup DDL from sql/setup.sql ...")
    sql_text = SETUP_SQL_PATH.read_text()

    cur = conn.cursor()
    try:
        for raw_stmt in sql_text.split(";"):
            lines = [
                ln for ln in raw_stmt.splitlines()
                if ln.strip() and not ln.strip().startswith("--")
            ]
            stmt = "\n".join(lines).strip()
            if not stmt:
                continue
            cur.execute(stmt)
        print("[+] Snowflake setup complete.\n")
    finally:
        cur.close()


def _has_table(cur, table_name):
    """Check whether a table exists in the current schema."""
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = %s",
        (table_name,),
    )
    return cur.fetchone()[0] > 0


def _has_column(cur, table_name, column_name):
    """Check whether a column exists on a table in the current schema."""
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = %s "
        "AND COLUMN_NAME = %s",
        (table_name, column_name),
    )
    return cur.fetchone()[0] > 0


def _get_or_create_query_id(cur, query_text):
    """Upsert into SEARCH_QUERIES and return the QUERY_ID."""
    cur.execute(
        "SELECT QUERY_ID FROM SEARCH_QUERIES WHERE QUERY_TEXT = %s",
        (query_text,),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO SEARCH_QUERIES (QUERY_TEXT) VALUES (%s)",
        (query_text,),
    )
    cur.execute(
        "SELECT QUERY_ID FROM SEARCH_QUERIES WHERE QUERY_TEXT = %s",
        (query_text,),
    )
    return cur.fetchone()[0]


def upload_and_load(conn, query, results_path, images_dir, country, language):
    """Upload files to Snowflake stages and load data into tables.

    Automatically detects whether the database uses the normalized schema
    (SEARCH_QUERIES + QUERY_ID) or the legacy schema (SEARCH_QUERY VARCHAR).

    Returns:
        (run_id, loaded_count) tuple.
    """
    results_path = Path(results_path).resolve()
    images_dir = Path(images_dir).resolve()

    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")

        normalized = _has_table(cur, "SEARCH_QUERIES")
        query_id = None

        if normalized:
            query_id = _get_or_create_query_id(cur, query)

        # Upload JSON
        put_json = (
            f"PUT 'file://{results_path}' @SCRAPE_DATA "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        print(f"[*] Uploading {results_path.name} to @SCRAPE_DATA ...")
        cur.execute(put_json)
        print("[+] JSON uploaded.")

        # Upload images
        if images_dir.is_dir() and any(images_dir.iterdir()):
            put_images = (
                f"PUT 'file://{images_dir}/*' @PRODUCT_IMAGES "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            print("[*] Uploading images to @PRODUCT_IMAGES ...")
            cur.execute(put_images)
            print("[+] Images uploaded.")
        else:
            print("[*] No images to upload — skipping.")

        # Create scrape-run record
        has_qid_col = _has_column(cur, "SCRAPE_RUNS", "QUERY_ID")
        has_sq_run_col = _has_column(cur, "SCRAPE_RUNS", "SEARCH_QUERY")

        if has_qid_col and has_sq_run_col:
            cur.execute(
                "INSERT INTO SCRAPE_RUNS (QUERY_ID, SEARCH_QUERY, COUNTRY, LANGUAGE, SOURCE_FILE) "
                "VALUES (%s, %s, %s, %s, %s)",
                (query_id, query, country, language, results_path.name),
            )
        elif has_qid_col:
            cur.execute(
                "INSERT INTO SCRAPE_RUNS (QUERY_ID, COUNTRY, LANGUAGE, SOURCE_FILE) "
                "VALUES (%s, %s, %s, %s)",
                (query_id, country, language, results_path.name),
            )
        else:
            cur.execute(
                "INSERT INTO SCRAPE_RUNS (SEARCH_QUERY, COUNTRY, LANGUAGE, SOURCE_FILE) "
                "VALUES (%s, %s, %s, %s)",
                (query, country, language, results_path.name),
            )

        cur.execute("SELECT MAX(RUN_ID) FROM SCRAPE_RUNS")
        run_id = cur.fetchone()[0]
        print(f"[+] Created scrape run #{run_id}")

        # Load products from staged JSON
        json_filename = results_path.name
        print("[*] Loading products into PRODUCTS table ...")

        has_sq_col = _has_column(cur, "PRODUCTS", "SEARCH_QUERY")
        if has_sq_col:
            cur.execute(f"""
                INSERT INTO PRODUCTS (
                    RUN_ID, SEARCH_QUERY, TITLE, PRICE, PRICE_NUMERIC,
                    ORIGINAL_PRICE, ORIGINAL_PRICE_NUMERIC,
                    DISCOUNT, SELLER, RATING, RATING_NUMERIC,
                    REVIEWS, LINK, IMAGE_PATH, IMAGE_STAGE_PATH, SHIPPING
                )
                SELECT
                    {run_id},
                    '{query.replace("'", "''")}',
                    $1:title::VARCHAR,
                    $1:price::VARCHAR,
                    TRY_CAST(REPLACE(REPLACE($1:price::VARCHAR, '$', ''), ',', '')
                             AS NUMBER(10,2)),
                    NULLIF($1:original_price::VARCHAR, ''),
                    TRY_CAST(REPLACE(REPLACE(NULLIF($1:original_price::VARCHAR, ''), '$', ''), ',', '')
                             AS NUMBER(10,2)),
                    NULLIF($1:discount::VARCHAR, ''),
                    NULLIF($1:seller::VARCHAR, ''),
                    NULLIF($1:rating::VARCHAR, ''),
                    TRY_CAST(SPLIT_PART(NULLIF($1:rating::VARCHAR, ''), '/', 1)
                             AS NUMBER(3,1)),
                    NULLIF($1:reviews::VARCHAR, ''),
                    NULLIF($1:link::VARCHAR, ''),
                    NULLIF($1:image_path::VARCHAR, ''),
                    NULLIF($1:image_path::VARCHAR, ''),
                    NULLIF($1:shipping::VARCHAR, '')
                FROM @SCRAPE_DATA/{json_filename} (FILE_FORMAT => 'JSON_FORMAT')
            """)
        else:
            cur.execute(f"""
                INSERT INTO PRODUCTS (
                    RUN_ID, TITLE, PRICE, PRICE_NUMERIC,
                    ORIGINAL_PRICE, ORIGINAL_PRICE_NUMERIC,
                    DISCOUNT, SELLER, RATING, RATING_NUMERIC,
                    REVIEWS, LINK, IMAGE_PATH, IMAGE_STAGE_PATH, SHIPPING
                )
                SELECT
                    {run_id},
                    $1:title::VARCHAR,
                    $1:price::VARCHAR,
                    TRY_CAST(REPLACE(REPLACE($1:price::VARCHAR, '$', ''), ',', '')
                             AS NUMBER(10,2)),
                    NULLIF($1:original_price::VARCHAR, ''),
                    TRY_CAST(REPLACE(REPLACE(NULLIF($1:original_price::VARCHAR, ''), '$', ''), ',', '')
                             AS NUMBER(10,2)),
                    NULLIF($1:discount::VARCHAR, ''),
                    NULLIF($1:seller::VARCHAR, ''),
                    NULLIF($1:rating::VARCHAR, ''),
                    TRY_CAST(SPLIT_PART(NULLIF($1:rating::VARCHAR, ''), '/', 1)
                             AS NUMBER(3,1)),
                    NULLIF($1:reviews::VARCHAR, ''),
                    NULLIF($1:link::VARCHAR, ''),
                    NULLIF($1:image_path::VARCHAR, ''),
                    NULLIF($1:image_path::VARCHAR, ''),
                    NULLIF($1:shipping::VARCHAR, '')
                FROM @SCRAPE_DATA/{json_filename} (FILE_FORMAT => 'JSON_FORMAT')
            """)

        cur.execute(
            "SELECT COUNT(*) FROM PRODUCTS WHERE RUN_ID = %s", (run_id,)
        )
        loaded = cur.fetchone()[0]
        print(f"[+] Loaded {loaded} products (run #{run_id})")

        # Populate junction table (only if normalized schema is active)
        if normalized and _has_table(cur, "PRODUCT_QUERIES"):
            cur.execute(f"""
                INSERT INTO PRODUCT_QUERIES (PRODUCT_ID, QUERY_ID)
                SELECT PRODUCT_ID, {query_id}
                FROM PRODUCTS
                WHERE RUN_ID = {run_id}
            """)

        cur.execute(
            "UPDATE SCRAPE_RUNS SET PRODUCT_COUNT = %s WHERE RUN_ID = %s",
            (loaded, run_id),
        )

        return run_id, loaded

    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------

def fetch_active_products(conn, run_id=None, query_filter=None):
    """Return active products with their search query text.

    Each row is a dict with keys: PRODUCT_ID, TITLE, PRICE, SELLER, RATING,
    REVIEWS, SHIPPING, DISCOUNT, ORIGINAL_PRICE, LINK, QUERY_TEXT.

    Adapts automatically to the normalized or legacy schema.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")

        normalized = _has_table(cur, "SEARCH_QUERIES") and _has_column(
            cur, "SCRAPE_RUNS", "QUERY_ID"
        )

        if normalized:
            sql = """
                SELECT DISTINCT
                    p.PRODUCT_ID, p.TITLE, p.PRICE, p.SELLER, p.RATING,
                    p.REVIEWS, p.SHIPPING, p.DISCOUNT, p.ORIGINAL_PRICE,
                    p.LINK, sq.QUERY_TEXT
                FROM PRODUCTS p
                JOIN SCRAPE_RUNS r ON p.RUN_ID = r.RUN_ID
                JOIN SEARCH_QUERIES sq ON r.QUERY_ID = sq.QUERY_ID
                WHERE p.IS_ACTIVE = TRUE
            """
        else:
            sql = """
                SELECT
                    p.PRODUCT_ID, p.TITLE, p.PRICE, p.SELLER, p.RATING,
                    p.REVIEWS, p.SHIPPING, p.DISCOUNT, p.ORIGINAL_PRICE,
                    p.LINK, r.SEARCH_QUERY AS QUERY_TEXT
                FROM PRODUCTS p
                JOIN SCRAPE_RUNS r ON p.RUN_ID = r.RUN_ID
                WHERE p.IS_ACTIVE = TRUE
            """

        params = []

        if run_id is not None:
            sql += " AND p.RUN_ID = %s"
            params.append(run_id)

        if query_filter is not None:
            if normalized:
                sql += " AND LOWER(sq.QUERY_TEXT) LIKE %s"
            else:
                sql += " AND LOWER(r.SEARCH_QUERY) LIKE %s"
            params.append(f"%{query_filter.lower()}%")

        if normalized:
            sql += " ORDER BY sq.QUERY_TEXT, p.PRODUCT_ID"
        else:
            sql += " ORDER BY r.SEARCH_QUERY, p.PRODUCT_ID"

        cur.execute(sql, params)

        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_stale_queries(conn, max_stale_days=7):
    """Return search queries whose active products haven't been verified recently.

    A query is "stale" if any of its active products has LAST_VERIFIED_AT
    older than max_stale_days ago, or NULL (never verified).

    Returns a list of query text strings.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")

        normalized = _has_table(cur, "SEARCH_QUERIES") and _has_column(
            cur, "SCRAPE_RUNS", "QUERY_ID"
        )

        if normalized:
            sql = """
                SELECT DISTINCT sq.QUERY_TEXT
                FROM PRODUCTS p
                JOIN SCRAPE_RUNS r ON p.RUN_ID = r.RUN_ID
                JOIN SEARCH_QUERIES sq ON r.QUERY_ID = sq.QUERY_ID
                WHERE p.IS_ACTIVE = TRUE
                  AND (p.LAST_VERIFIED_AT IS NULL
                       OR p.LAST_VERIFIED_AT < DATEADD(day, -%s, CURRENT_TIMESTAMP()))
                ORDER BY sq.QUERY_TEXT
            """
        else:
            sql = """
                SELECT DISTINCT r.SEARCH_QUERY AS QUERY_TEXT
                FROM PRODUCTS p
                JOIN SCRAPE_RUNS r ON p.RUN_ID = r.RUN_ID
                WHERE p.IS_ACTIVE = TRUE
                  AND (p.LAST_VERIFIED_AT IS NULL
                       OR p.LAST_VERIFIED_AT < DATEADD(day, -%s, CURRENT_TIMESTAMP()))
                ORDER BY QUERY_TEXT
            """

        cur.execute(sql, (max_stale_days,))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def mark_inactive(conn, product_ids):
    """Batch-set IS_ACTIVE = FALSE for a list of product IDs."""
    if not product_ids:
        return

    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")

        placeholders = ", ".join(["%s"] * len(product_ids))
        cur.execute(
            f"UPDATE PRODUCTS SET IS_ACTIVE = FALSE, "
            f"LAST_VERIFIED_AT = CURRENT_TIMESTAMP() "
            f"WHERE PRODUCT_ID IN ({placeholders})",
            product_ids,
        )
    finally:
        cur.close()
