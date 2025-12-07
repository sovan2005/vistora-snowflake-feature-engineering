-- ============================================
-- GENERATE SAMPLE DATA
-- File: 02-data-generation/generate_sample_data.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- STEP 1: Generate Customer Demographics (200 customers)
-- ============================================

INSERT INTO RAW_CUSTOMER_DEMOGRAPHICS 
SELECT
    'CUST_' || LPAD(SEQ4(), 6, '0') AS customer_id,
    UNIFORM(18, 75, RANDOM()) AS customer_age,
    CASE UNIFORM(1, 3, RANDOM())
        WHEN 1 THEN 'Male'
        WHEN 2 THEN 'Female'
        ELSE 'Other'
    END AS gender,
    CASE UNIFORM(1, 10, RANDOM())
        WHEN 1 THEN 'Mumbai'
        WHEN 2 THEN 'Delhi'
        WHEN 3 THEN 'Bangalore'
        WHEN 4 THEN 'Hyderabad'
        WHEN 5 THEN 'Chennai'
        WHEN 6 THEN 'Kolkata'
        WHEN 7 THEN 'Pune'
        WHEN 8 THEN 'Ahmedabad'
        WHEN 9 THEN 'Jaipur'
        ELSE 'Lucknow'
    END AS customer_location,
    DATEADD(day, -UNIFORM(30, 730, RANDOM()), CURRENT_DATE()) AS account_created_date,
    UNIFORM(0, 1, RANDOM()) = 1 AS email_verified,
    UNIFORM(0, 1, RANDOM()) = 1 AS phone_verified,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'Premium'
        WHEN 2 THEN 'Regular'
        WHEN 3 THEN 'New'
        ELSE 'Inactive'
    END AS customer_segment,
    CURRENT_TIMESTAMP() AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 200));

SELECT COUNT(*) AS customers_created FROM RAW_CUSTOMER_DEMOGRAPHICS;

-- ============================================
-- STEP 2: Generate Product Catalog (50 products)
-- ============================================

INSERT INTO PRODUCT_CATALOG
SELECT
    'PROD_' || LPAD(SEQ4(), 4, '0') AS product_id,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'Laptop ' || UNIFORM(1, 10, RANDOM())
        WHEN 2 THEN 'Phone ' || UNIFORM(1, 10, RANDOM())
        WHEN 3 THEN 'Headphones ' || UNIFORM(1, 10, RANDOM())
        WHEN 4 THEN 'Watch ' || UNIFORM(1, 10, RANDOM())
        ELSE 'Tablet ' || UNIFORM(1, 10, RANDOM())
    END AS product_name,
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Fashion'
        WHEN 3 THEN 'Home & Kitchen'
        WHEN 4 THEN 'Books'
        WHEN 5 THEN 'Sports'
        WHEN 6 THEN 'Beauty'
        WHEN 7 THEN 'Toys'
        ELSE 'Grocery'
    END AS product_category,
    ROUND(UNIFORM(99, 89999, RANDOM()), 2) AS base_price,
    ROUND(UNIFORM(50, 50000, RANDOM()), 2) AS cost_price,
    UNIFORM(10, 1000, RANDOM()) AS inventory_count,
    ROUND(UNIFORM(3.0, 5.0, RANDOM()), 2) AS rating,
    CURRENT_TIMESTAMP() AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 50));

SELECT COUNT(*) AS products_created FROM PRODUCT_CATALOG;

-- ============================================
-- STEP 3: Generate Transactions (2000 transactions)
-- ============================================

INSERT INTO RAW_CUSTOMER_TRANSACTIONS
SELECT
    'TXN_' || LPAD(SEQ4(), 8, '0') AS transaction_id,
    'CUST_' || LPAD(UNIFORM(1, 200, RANDOM()), 6, '0') AS customer_id,
    DATEADD(
        minute, 
        -UNIFORM(1, 525600, RANDOM()), 
        CURRENT_TIMESTAMP()
    ) AS transaction_date,
    pc.product_category,
    pc.product_name,
    pc.base_price AS product_price,
    UNIFORM(1, 5, RANDOM()) AS quantity,
    CASE UNIFORM(1, 10, RANDOM())
        WHEN 1 THEN 10
        WHEN 2 THEN 15
        WHEN 3 THEN 20
        ELSE 0
    END AS discount_percentage,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Debit Card'
        WHEN 3 THEN 'UPI'
        WHEN 4 THEN 'Net Banking'
        ELSE 'Cash on Delivery'
    END AS payment_method,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'Mobile'
        WHEN 2 THEN 'Desktop'
        WHEN 3 THEN 'Tablet'
        ELSE 'App'
    END AS device_type,
    UNIFORM(1, 90, RANDOM()) AS session_duration_minutes,
    UNIFORM(3, 50, RANDOM()) AS page_views,
    UNIFORM(0, 1, RANDOM()) = 1 AS is_first_purchase,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN 'Google Search'
        WHEN 2 THEN 'Facebook'
        WHEN 3 THEN 'Instagram'
        WHEN 4 THEN 'Direct'
        WHEN 5 THEN 'Email'
        ELSE 'Referral'
    END AS referral_source,
    CURRENT_TIMESTAMP() AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 2000)) g
CROSS JOIN (SELECT * FROM PRODUCT_CATALOG ORDER BY RANDOM() LIMIT 1) pc;

SELECT COUNT(*) AS transactions_created FROM RAW_CUSTOMER_TRANSACTIONS;

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

-- Summary statistics
SELECT 
    'CUSTOMERS' AS entity,
    COUNT(*) AS total_count
FROM RAW_CUSTOMER_DEMOGRAPHICS

UNION ALL

SELECT 
    'PRODUCTS' AS entity,
    COUNT(*) AS total_count
FROM PRODUCT_CATALOG

UNION ALL

SELECT 
    'TRANSACTIONS' AS entity,
    COUNT(*) AS total_count
FROM RAW_CUSTOMER_TRANSACTIONS;

-- Date range of transactions
SELECT 
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS days_of_data
FROM RAW_CUSTOMER_TRANSACTIONS;

SELECT 'Data generation completed successfully!' AS status;