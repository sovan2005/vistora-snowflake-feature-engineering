-- ============================================
-- CREATE RAW DATA TABLES
-- File: 02-data-generation/create_tables.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- Table 1: Customer Transactions (Main table)
CREATE OR REPLACE TABLE RAW_CUSTOMER_TRANSACTIONS (
    transaction_id STRING PRIMARY KEY,
    customer_id STRING NOT NULL,
    transaction_date TIMESTAMP_NTZ NOT NULL,
    product_category STRING,
    product_name STRING,
    product_price DECIMAL(10,2),
    quantity INTEGER,
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    payment_method STRING,
    device_type STRING,
    session_duration_minutes INTEGER,
    page_views INTEGER,
    is_first_purchase BOOLEAN,
    referral_source STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table 2: Customer Demographic Data
CREATE OR REPLACE TABLE RAW_CUSTOMER_DEMOGRAPHICS (
    customer_id STRING PRIMARY KEY,
    customer_age INTEGER,
    gender STRING,
    customer_location STRING,
    account_created_date DATE,
    email_verified BOOLEAN,
    phone_verified BOOLEAN,
    customer_segment STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table 3: Product Catalog
CREATE OR REPLACE TABLE PRODUCT_CATALOG (
    product_id STRING PRIMARY KEY,
    product_name STRING,
    product_category STRING,
    base_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    inventory_count INTEGER,
    rating DECIMAL(3,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verification
SELECT 'Tables created successfully!' AS status;

-- Show all tables
SHOW TABLES IN SCHEMA FEATURE_ENGINEERING;