-- ============================================
-- VISTORA ML PROJECT - INITIAL SETUP
-- File: 01-setup/snowflake_setup.sql
-- Purpose: Create database, schema, and warehouse
-- ============================================

-- Step 1: Create Database
CREATE DATABASE IF NOT EXISTS VISTORA_ML_PROJECT
COMMENT = 'Main database for ML feature engineering project';

-- Step 2: Use the database
USE DATABASE VISTORA_ML_PROJECT;

-- Step 3: Create Schema
CREATE SCHEMA IF NOT EXISTS FEATURE_ENGINEERING
COMMENT = 'Schema for feature engineering workflow';

-- Step 4: Use the schema
USE SCHEMA FEATURE_ENGINEERING;

-- Step 5: Create Warehouse (Compute Resource)
CREATE WAREHOUSE IF NOT EXISTS ML_WAREHOUSE
WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for ML workloads';

-- Step 6: Use the warehouse
USE WAREHOUSE ML_WAREHOUSE;

-- Step 7: Verify Setup
SELECT 
    CURRENT_DATABASE() AS database_name,
    CURRENT_SCHEMA() AS schema_name,
    CURRENT_WAREHOUSE() AS warehouse_name,
    CURRENT_TIMESTAMP() AS setup_timestamp;

-- Success message
SELECT 'Setup completed successfully!' AS status;
