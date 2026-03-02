-- ============================================================
-- Google Shopping Scraper — Snowflake Data Model Setup
-- ============================================================
-- Run this script in your Snowflake worksheet to create:
--   1. Database & schema
--   2. Search queries lookup table
--   3. Scrape runs tracking table
--   4. Products table
--   5. Product-query junction table
--   6. Internal stages & file format
--   7. Analysis view
-- ============================================================

-- 1. Database and schema
CREATE DATABASE IF NOT EXISTS RETAIL_ANALYZER;
USE DATABASE RETAIL_ANALYZER;

CREATE SCHEMA IF NOT EXISTS GOOGLE_SHOPPING;
USE SCHEMA GOOGLE_SHOPPING;

-- 2. Search queries — one row per unique search term
CREATE TABLE IF NOT EXISTS SEARCH_QUERIES (
    QUERY_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    QUERY_TEXT        VARCHAR(500)   NOT NULL,
    FIRST_SEARCHED_AT TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT UQ_QUERY_TEXT UNIQUE (QUERY_TEXT)
);

-- 3. Scrape runs — one row per execution of the scraper
CREATE TABLE IF NOT EXISTS SCRAPE_RUNS (
    RUN_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    QUERY_ID        NUMBER         NOT NULL REFERENCES SEARCH_QUERIES(QUERY_ID),
    COUNTRY         VARCHAR(10)    DEFAULT 'us',
    LANGUAGE        VARCHAR(10)    DEFAULT 'en',
    SCRAPED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    PRODUCT_COUNT   NUMBER,
    SOURCE_FILE     VARCHAR(500)
);

-- 4. Products — one row per scraped product
CREATE TABLE IF NOT EXISTS PRODUCTS (
    PRODUCT_ID      NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID          NUMBER         NOT NULL REFERENCES SCRAPE_RUNS(RUN_ID),
    TITLE           VARCHAR(1000)  NOT NULL,
    PRICE           VARCHAR(50),
    PRICE_NUMERIC   NUMBER(10,2),
    ORIGINAL_PRICE  VARCHAR(50),
    ORIGINAL_PRICE_NUMERIC NUMBER(10,2),
    DISCOUNT        VARCHAR(50),
    SELLER          VARCHAR(500),
    RATING          VARCHAR(20),
    RATING_NUMERIC  NUMBER(3,1),
    REVIEWS         VARCHAR(50),
    LINK            VARCHAR(4000),
    IMAGE_PATH      VARCHAR(500),
    IMAGE_STAGE_PATH VARCHAR(500),
    SHIPPING        VARCHAR(500),
    IS_ACTIVE       BOOLEAN        DEFAULT TRUE,
    LAST_VERIFIED_AT TIMESTAMP_NTZ,
    PRICE_UPDATED_AT TIMESTAMP_NTZ,
    LOADED_AT       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Product-query junction — which queries found which products
CREATE TABLE IF NOT EXISTS PRODUCT_QUERIES (
    PRODUCT_ID    NUMBER NOT NULL REFERENCES PRODUCTS(PRODUCT_ID),
    QUERY_ID      NUMBER NOT NULL REFERENCES SEARCH_QUERIES(QUERY_ID),
    FOUND_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PRODUCT_ID, QUERY_ID)
);

-- 6. Internal stages & file format
CREATE STAGE IF NOT EXISTS PRODUCT_IMAGES
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stores product thumbnail images scraped from Google Shopping';

CREATE FILE FORMAT IF NOT EXISTS JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

CREATE STAGE IF NOT EXISTS SCRAPE_DATA
    FILE_FORMAT = JSON_FORMAT
    COMMENT = 'Landing stage for scraper JSON output files';

-- 7. View with clean numeric fields for analysis
CREATE OR REPLACE VIEW PRODUCTS_ANALYSIS AS
SELECT
    p.PRODUCT_ID,
    p.RUN_ID,
    sq.QUERY_ID,
    sq.QUERY_TEXT AS SEARCH_QUERY,
    r.SCRAPED_AT,
    p.TITLE,
    p.PRICE,
    p.PRICE_NUMERIC,
    p.ORIGINAL_PRICE,
    p.ORIGINAL_PRICE_NUMERIC,
    p.DISCOUNT,
    CASE
        WHEN p.ORIGINAL_PRICE_NUMERIC > 0 AND p.PRICE_NUMERIC > 0
        THEN ROUND((1 - p.PRICE_NUMERIC / p.ORIGINAL_PRICE_NUMERIC) * 100, 1)
    END AS DISCOUNT_PCT,
    p.SELLER,
    p.RATING,
    p.RATING_NUMERIC,
    p.REVIEWS,
    CASE
        WHEN p.REVIEWS LIKE '%K' THEN TRY_CAST(REPLACE(p.REVIEWS, 'K', '') AS NUMBER) * 1000
        WHEN p.REVIEWS LIKE '%M' THEN TRY_CAST(REPLACE(p.REVIEWS, 'M', '') AS NUMBER) * 1000000
        ELSE TRY_CAST(p.REVIEWS AS NUMBER)
    END AS REVIEWS_NUMERIC,
    p.LINK,
    p.IMAGE_PATH,
    p.IMAGE_STAGE_PATH,
    BUILD_SCOPED_FILE_URL(@PRODUCT_IMAGES, p.IMAGE_STAGE_PATH) AS IMAGE_URL,
    p.SHIPPING,
    p.IS_ACTIVE,
    p.LAST_VERIFIED_AT,
    p.PRICE_UPDATED_AT,
    p.LOADED_AT
FROM PRODUCTS p
JOIN SCRAPE_RUNS r ON p.RUN_ID = r.RUN_ID
JOIN SEARCH_QUERIES sq ON r.QUERY_ID = sq.QUERY_ID;
