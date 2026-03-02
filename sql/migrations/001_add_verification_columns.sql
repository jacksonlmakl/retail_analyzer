-- ============================================================
-- Migration 001: Add product verification columns
-- ============================================================
-- Run this against an existing RETAIL_ANALYZER database to add
-- the columns used by verify_products.py.
-- ============================================================

USE DATABASE RETAIL_ANALYZER;
USE SCHEMA GOOGLE_SHOPPING;

ALTER TABLE PRODUCTS ADD COLUMN IF NOT EXISTS IS_ACTIVE        BOOLEAN       DEFAULT TRUE;
ALTER TABLE PRODUCTS ADD COLUMN IF NOT EXISTS LAST_VERIFIED_AT TIMESTAMP_NTZ;
ALTER TABLE PRODUCTS ADD COLUMN IF NOT EXISTS PRICE_UPDATED_AT TIMESTAMP_NTZ;

-- Backfill: mark all existing rows as active
UPDATE PRODUCTS SET IS_ACTIVE = TRUE WHERE IS_ACTIVE IS NULL;
