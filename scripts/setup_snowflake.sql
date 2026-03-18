-- ============================================
-- E-Commerce Pipeline: Snowflake Setup Script
-- Run this before loading any data
-- ============================================

USE ROLE ACCOUNTADMIN;

-- Step 1: Create virtual warehouse
CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Step 2: Create database
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;

-- Step 3: Create schemas
USE DATABASE ECOMMERCE_DB;

CREATE SCHEMA IF NOT EXISTS RAW;      -- raw CSV data loaded by Python
CREATE SCHEMA IF NOT EXISTS STAGING;  -- dbt staging models
CREATE SCHEMA IF NOT EXISTS MARTS;    -- dbt mart models (used by Power BI)

-- Step 4: Verify setup
SHOW SCHEMAS IN DATABASE ECOMMERCE_DB;

-- ============================================
-- Verification Queries (run after data load)
-- ============================================

-- Check all tables loaded correctly
-- SELECT 'RAW_ORDERS' as table_name, COUNT(*) as row_count FROM RAW.RAW_ORDERS UNION ALL
-- SELECT 'RAW_ORDER_ITEMS', COUNT(*) FROM RAW.RAW_ORDER_ITEMS UNION ALL
-- SELECT 'RAW_ORDER_PAYMENTS', COUNT(*) FROM RAW.RAW_ORDER_PAYMENTS UNION ALL
-- SELECT 'RAW_ORDER_REVIEWS', COUNT(*) FROM RAW.RAW_ORDER_REVIEWS UNION ALL
-- SELECT 'RAW_CUSTOMERS', COUNT(*) FROM RAW.RAW_CUSTOMERS UNION ALL
-- SELECT 'RAW_SELLERS', COUNT(*) FROM RAW.RAW_SELLERS UNION ALL
-- SELECT 'RAW_PRODUCTS', COUNT(*) FROM RAW.RAW_PRODUCTS UNION ALL
-- SELECT 'RAW_CATEGORY_TRANSLATION', COUNT(*) FROM RAW.RAW_CATEGORY_TRANSLATION UNION ALL
-- SELECT 'RAW_GEOLOCATION', COUNT(*) FROM RAW.RAW_GEOLOCATION
-- ORDER BY row_count DESC;

-- Expected row counts:
-- RAW_GEOLOCATION:          1,000,163
-- RAW_ORDER_ITEMS:            112,650
-- RAW_ORDER_PAYMENTS:         103,886
-- RAW_ORDERS:                  99,441
-- RAW_CUSTOMERS:               99,441
-- RAW_ORDER_REVIEWS:           99,224
-- RAW_PRODUCTS:                32,951
-- RAW_SELLERS:                  3,095
-- RAW_CATEGORY_TRANSLATION:        71