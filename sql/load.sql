-- ============================================================
-- Google Shopping Scraper — Load Data & Images into Snowflake
-- ============================================================
-- Run these commands after executing snowflake_setup.sql.
--
-- Prerequisites:
--   - SnowSQL CLI installed, or use the Snowflake web UI
--   - results.json and images/ directory from a scraper run
-- ============================================================

USE DATABASE RETAIL_ANALYZER;
USE SCHEMA GOOGLE_SHOPPING;

-- ============================================================
-- STEP 1: Upload the JSON results file to the data stage
-- ============================================================
-- From SnowSQL CLI:
--   PUT file:///path/to/retail_analyzer/results.json @SCRAPE_DATA AUTO_COMPRESS=FALSE;
--
-- Or from the Snowflake web UI, use the stage upload feature.

PUT file://results.json @SCRAPE_DATA AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- ============================================================
-- STEP 2: Upload product images to the image stage
-- ============================================================
-- From SnowSQL CLI:
--   PUT file:///path/to/retail_analyzer/images/*.webp @PRODUCT_IMAGES AUTO_COMPRESS=FALSE;

PUT file://images/*.webp @PRODUCT_IMAGES AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify uploads
LIST @SCRAPE_DATA;
LIST @PRODUCT_IMAGES;

-- ============================================================
-- STEP 3: Upsert search query and create a scrape run record
-- ============================================================
-- Update the query text to match what you searched for.

INSERT INTO SEARCH_QUERIES (QUERY_TEXT)
SELECT 'mechanical keyboard'
WHERE NOT EXISTS (
    SELECT 1 FROM SEARCH_QUERIES WHERE QUERY_TEXT = 'mechanical keyboard'
);

SET query_id = (SELECT QUERY_ID FROM SEARCH_QUERIES WHERE QUERY_TEXT = 'mechanical keyboard');

INSERT INTO SCRAPE_RUNS (QUERY_ID, COUNTRY, LANGUAGE, SOURCE_FILE)
VALUES ($query_id, 'us', 'en', 'results.json');

SET run_id = (SELECT MAX(RUN_ID) FROM SCRAPE_RUNS);

-- ============================================================
-- STEP 4: Load products from the staged JSON file
-- ============================================================

INSERT INTO PRODUCTS (
    RUN_ID,
    TITLE,
    PRICE,
    PRICE_NUMERIC,
    ORIGINAL_PRICE,
    ORIGINAL_PRICE_NUMERIC,
    DISCOUNT,
    SELLER,
    RATING,
    RATING_NUMERIC,
    REVIEWS,
    LINK,
    IMAGE_PATH,
    IMAGE_STAGE_PATH,
    SHIPPING
)
SELECT
    $run_id,
    $1:title::VARCHAR,
    $1:price::VARCHAR,
    TRY_CAST(REPLACE($1:price::VARCHAR, '$', '') AS NUMBER(10,2)),
    NULLIF($1:original_price::VARCHAR, ''),
    TRY_CAST(REPLACE(NULLIF($1:original_price::VARCHAR, ''), '$', '') AS NUMBER(10,2)),
    NULLIF($1:discount::VARCHAR, ''),
    NULLIF($1:seller::VARCHAR, ''),
    NULLIF($1:rating::VARCHAR, ''),
    TRY_CAST(SPLIT_PART(NULLIF($1:rating::VARCHAR, ''), '/', 1) AS NUMBER(3,1)),
    NULLIF($1:reviews::VARCHAR, ''),
    NULLIF($1:link::VARCHAR, ''),
    NULLIF($1:image_path::VARCHAR, ''),
    NULLIF($1:image_path::VARCHAR, ''),
    NULLIF($1:shipping::VARCHAR, '')
FROM @SCRAPE_DATA/results.json (FILE_FORMAT => 'JSON_FORMAT');

-- ============================================================
-- STEP 5: Populate the product-query junction table
-- ============================================================

INSERT INTO PRODUCT_QUERIES (PRODUCT_ID, QUERY_ID)
SELECT PRODUCT_ID, $query_id
FROM PRODUCTS
WHERE RUN_ID = $run_id;

-- ============================================================
-- STEP 6: Verify the load
-- ============================================================

SELECT COUNT(*) AS PRODUCTS_LOADED FROM PRODUCTS WHERE RUN_ID = $run_id;

SELECT * FROM PRODUCTS_ANALYSIS WHERE RUN_ID = $run_id ORDER BY PRICE_NUMERIC;

-- ============================================================
-- Example queries
-- ============================================================

-- Average price by seller
SELECT
    SELLER,
    COUNT(*)            AS NUM_PRODUCTS,
    AVG(PRICE_NUMERIC)  AS AVG_PRICE,
    MIN(PRICE_NUMERIC)  AS MIN_PRICE,
    MAX(PRICE_NUMERIC)  AS MAX_PRICE
FROM PRODUCTS_ANALYSIS
GROUP BY SELLER
ORDER BY NUM_PRODUCTS DESC;

-- Best rated products
SELECT TITLE, PRICE, SELLER, RATING_NUMERIC, REVIEWS_NUMERIC
FROM PRODUCTS_ANALYSIS
WHERE RATING_NUMERIC IS NOT NULL
ORDER BY RATING_NUMERIC DESC, REVIEWS_NUMERIC DESC
LIMIT 10;

-- Products on sale
SELECT TITLE, PRICE, ORIGINAL_PRICE, DISCOUNT, DISCOUNT_PCT, SELLER
FROM PRODUCTS_ANALYSIS
WHERE DISCOUNT IS NOT NULL
ORDER BY DISCOUNT_PCT DESC;

-- Get a scoped image URL for a specific product
SELECT TITLE, BUILD_SCOPED_FILE_URL(@PRODUCT_IMAGES, IMAGE_STAGE_PATH) AS IMAGE_URL
FROM PRODUCTS
WHERE IMAGE_STAGE_PATH IS NOT NULL
LIMIT 5;
