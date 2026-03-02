"""
Snowflake loader — manages scraped Google Shopping data in the
RETAIL_ANALYZER.GOOGLE_SHOPPING schema.
"""

from pathlib import Path

from shared.snowflake import DATABASE, get_connection, check_env, run_setup as _run_setup, get_setup_path  # noqa: F401

SCHEMA = "GOOGLE_SHOPPING"
SETUP_SQL_PATH = get_setup_path("google_shopping")


def run_setup(conn):
    _run_setup(conn, SETUP_SQL_PATH)


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

        put_json = (
            f"PUT 'file://{results_path}' @SCRAPE_DATA "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        print(f"[*] Uploading {results_path.name} to @SCRAPE_DATA ...")
        cur.execute(put_json)
        print("[+] JSON uploaded.")

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
