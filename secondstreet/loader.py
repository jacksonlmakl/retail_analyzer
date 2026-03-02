"""Snowflake loader for the SECOND_STREET schema."""

from pathlib import Path

from shared.snowflake import DATABASE, get_connection, run_setup as _run_setup, get_setup_path

SCHEMA = "SECOND_STREET"
SETUP_SQL_PATH = get_setup_path("secondstreet")


def run_setup(conn):
    _run_setup(conn, SETUP_SQL_PATH)


def _get_or_create_query_id(cur, query_text):
    cur.execute("SELECT QUERY_ID FROM SEARCH_QUERIES WHERE QUERY_TEXT = %s", (query_text,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO SEARCH_QUERIES (QUERY_TEXT) VALUES (%s)", (query_text,))
    cur.execute("SELECT QUERY_ID FROM SEARCH_QUERIES WHERE QUERY_TEXT = %s", (query_text,))
    return cur.fetchone()[0]


def upload_and_load(conn, query, results_path, images_dir, country="us", language="en"):
    results_path = Path(results_path).resolve()
    images_dir = Path(images_dir).resolve()

    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")

        query_id = _get_or_create_query_id(cur, query)

        cur.execute(f"PUT 'file://{results_path}' @SCRAPE_DATA AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        if images_dir.is_dir() and any(images_dir.iterdir()):
            cur.execute(f"PUT 'file://{images_dir}/*' @PRODUCT_IMAGES AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        cur.execute(
            "INSERT INTO SCRAPE_RUNS (QUERY_ID, COUNTRY, LANGUAGE, SOURCE_FILE) VALUES (%s, %s, %s, %s)",
            (query_id, country, language, results_path.name),
        )
        cur.execute("SELECT MAX(RUN_ID) FROM SCRAPE_RUNS")
        run_id = cur.fetchone()[0]

        json_filename = results_path.name
        cur.execute(f"""
            INSERT INTO PRODUCTS (
                RUN_ID, TITLE, PRICE, PRICE_NUMERIC,
                ORIGINAL_PRICE, ORIGINAL_PRICE_NUMERIC,
                DISCOUNT, LINK, IMAGE_PATH, IMAGE_STAGE_PATH,
                BRAND, CONDITION, CATEGORY, COLOR, SIZE_INFO
            )
            SELECT
                {run_id},
                $1:title::VARCHAR,
                $1:price::VARCHAR,
                TRY_CAST(REPLACE(REPLACE($1:price::VARCHAR, '$', ''), ',', '') AS NUMBER(10,2)),
                NULLIF($1:original_price::VARCHAR, ''),
                TRY_CAST(REPLACE(REPLACE(NULLIF($1:original_price::VARCHAR, ''), '$', ''), ',', '') AS NUMBER(10,2)),
                NULLIF($1:discount::VARCHAR, ''),
                $1:link::VARCHAR,
                NULLIF($1:image_path::VARCHAR, ''),
                NULLIF($1:image_path::VARCHAR, ''),
                NULLIF($1:brand::VARCHAR, ''),
                NULLIF($1:condition::VARCHAR, ''),
                NULLIF($1:category::VARCHAR, ''),
                NULLIF($1:color::VARCHAR, ''),
                NULLIF($1:size_info::VARCHAR, '')
            FROM @SCRAPE_DATA/{json_filename} (FILE_FORMAT => 'JSON_FORMAT')
        """)

        cur.execute("SELECT COUNT(*) FROM PRODUCTS WHERE RUN_ID = %s", (run_id,))
        loaded = cur.fetchone()[0]
        print(f"[+] Loaded {loaded} 2nd Street products (run #{run_id})")

        cur.execute(f"""
            INSERT INTO PRODUCT_QUERIES (PRODUCT_ID, QUERY_ID)
            SELECT PRODUCT_ID, {query_id} FROM PRODUCTS WHERE RUN_ID = {run_id}
        """)
        cur.execute("UPDATE SCRAPE_RUNS SET PRODUCT_COUNT = %s WHERE RUN_ID = %s", (loaded, run_id))
        return run_id, loaded
    finally:
        cur.close()
