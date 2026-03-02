-- ============================================================
-- Migration 002: Add SEARCH_QUERY column to PRODUCTS table
-- ============================================================

USE DATABASE RETAIL_ANALYZER;
USE SCHEMA GOOGLE_SHOPPING;

ALTER TABLE PRODUCTS ADD COLUMN IF NOT EXISTS SEARCH_QUERY VARCHAR(500);

-- Backfill from SCRAPE_RUNS
UPDATE PRODUCTS p
SET p.SEARCH_QUERY = r.SEARCH_QUERY
FROM SCRAPE_RUNS r
WHERE p.RUN_ID = r.RUN_ID
  AND p.SEARCH_QUERY IS NULL;
