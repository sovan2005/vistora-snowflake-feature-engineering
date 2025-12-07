-- ============================================
-- ML TRAINING DATASET PREPARATION
-- File: 05-ml-pipeline/training_dataset.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- STEP 1: Create Target Variable
-- ============================================

-- Define high-value customer based on revenue
CREATE OR REPLACE VIEW VW_TARGET_VARIABLE AS
SELECT
    customer_id,
    total_revenue,
    
    -- Binary target: High-value customer (1) or not (0)
    CASE 
        WHEN total_revenue >= (
            SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_revenue)
            FROM VW_CUSTOMER_AGGREGATION_FEATURES
        ) THEN 1
        ELSE 0
    END AS is_high_value_customer,
    
    -- Multi-class target: Customer tier
    CASE 
        WHEN total_revenue >= (
            SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_revenue)
            FROM VW_CUSTOMER_AGGREGATION_FEATURES
        ) THEN 'Platinum'
        WHEN total_revenue >= (
            SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_revenue)
            FROM VW_CUSTOMER_AGGREGATION_FEATURES
        ) THEN 'Gold'
        WHEN total_revenue >= (
            SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_revenue)
            FROM VW_CUSTOMER_AGGREGATION_FEATURES
        ) THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier,
    
    CURRENT_TIMESTAMP() AS created_at
FROM VW_CUSTOMER_AGGREGATION_FEATURES;

-- Check target distribution
SELECT 
    is_high_value_customer,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM VW_TARGET_VARIABLE
GROUP BY is_high_value_customer;

-- ============================================
-- STEP 2: Create Complete Training Dataset
-- ============================================

CREATE OR REPLACE TABLE TBL_ML_TRAINING_DATASET AS
SELECT
    -- Customer ID
    fm.customer_id,
    
    -- Numerical Features (from Feature Matrix)
    fm.total_revenue,
    fm.total_transactions,
    fm.avg_order_value,
    fm.days_since_last_purchase,
    fm.recency_score,
    fm.frequency_score,
    fm.monetary_score,
    
    -- Categorical Features
    fm.preferred_category,
    fm.preferred_device,
    fm.customer_lifecycle_segment,
    fm.churn_risk,
    
    -- Additional features from aggregation view
    af.active_months,
    af.unique_categories_purchased,
    af.avg_session_duration,
    af.discount_usage_rate,
    af.customer_age,
    af.customer_location,
    af.customer_segment,
    
    -- Target Variables
    tv.is_high_value_customer,
    tv.customer_tier,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at
    
FROM VW_ML_FEATURE_MATRIX fm
JOIN VW_CUSTOMER_AGGREGATION_FEATURES af ON fm.customer_id = af.customer_id
JOIN VW_TARGET_VARIABLE tv ON fm.customer_id = tv.customer_id;

-- Verify dataset
SELECT COUNT(*) AS total_records FROM TBL_ML_TRAINING_DATASET;
SELECT * FROM TBL_ML_TRAINING_DATASET LIMIT 10;

-- ============================================
-- STEP 3: Encode Categorical Variables
-- ============================================

CREATE OR REPLACE VIEW VW_ML_ENCODED_DATASET AS
SELECT
    customer_id,
    
    -- Numerical features (no encoding needed)
    total_revenue,
    total_transactions,
    avg_order_value,
    days_since_last_purchase,
    recency_score,
    frequency_score,
    monetary_score,
    active_months,
    unique_categories_purchased,
    avg_session_duration,
    discount_usage_rate,
    customer_age,
    
    -- One-hot encoding for preferred_category
    CASE WHEN preferred_category = 'Electronics' THEN 1 ELSE 0 END AS cat_electronics,
    CASE WHEN preferred_category = 'Fashion' THEN 1 ELSE 0 END AS cat_fashion,
    CASE WHEN preferred_category = 'Home & Kitchen' THEN 1 ELSE 0 END AS cat_home,
    CASE WHEN preferred_category = 'Books' THEN 1 ELSE 0 END AS cat_books,
    CASE WHEN preferred_category = 'Sports' THEN 1 ELSE 0 END AS cat_sports,
    
    -- One-hot encoding for preferred_device
    CASE WHEN preferred_device = 'Mobile' THEN 1 ELSE 0 END AS device_mobile,
    CASE WHEN preferred_device = 'Desktop' THEN 1 ELSE 0 END AS device_desktop,
    CASE WHEN preferred_device = 'Tablet' THEN 1 ELSE 0 END AS device_tablet,
    
    -- One-hot encoding for churn_risk
    CASE WHEN churn_risk = 'High Risk' THEN 1 ELSE 0 END AS risk_high,
    CASE WHEN churn_risk = 'Medium Risk' THEN 1 ELSE 0 END AS risk_medium,
    CASE WHEN churn_risk = 'Low Risk' THEN 1 ELSE 0 END AS risk_low,
    
    -- One-hot encoding for customer_segment
    CASE WHEN customer_segment = 'Premium' THEN 1 ELSE 0 END AS segment_premium,
    CASE WHEN customer_segment = 'Regular' THEN 1 ELSE 0 END AS segment_regular,
    
    -- Target variable
    is_high_value_customer,
    customer_tier
    
FROM TBL_ML_TRAINING_DATASET;

-- Verify encoded dataset
SELECT * FROM VW_ML_ENCODED_DATASET LIMIT 10;

-- ============================================
-- STEP 4: Split into Training and Testing
-- ============================================

-- Training set (80%)
CREATE OR REPLACE TABLE TBL_TRAINING_SET AS
SELECT *
FROM VW_ML_ENCODED_DATASET
WHERE MOD(ABS(HASH(customer_id)), 10) < 8;

-- Testing set (20%)
CREATE OR REPLACE TABLE TBL_TESTING_SET AS
SELECT *
FROM VW_ML_ENCODED_DATASET
WHERE MOD(ABS(HASH(customer_id)), 10) >= 8;

-- Verify split
SELECT 'Training' AS dataset, COUNT(*) AS record_count FROM TBL_TRAINING_SET
UNION ALL
SELECT 'Testing' AS dataset, COUNT(*) AS record_count FROM TBL_TESTING_SET;

-- ============================================
-- STEP 5: Feature Statistics for ML
-- ============================================

CREATE OR REPLACE VIEW VW_FEATURE_STATISTICS AS
SELECT
    'total_revenue' AS feature_name,
    AVG(total_revenue) AS mean_value,
    STDDEV(total_revenue) AS std_dev,
    MIN(total_revenue) AS min_value,
    MAX(total_revenue) AS max_value,
    MEDIAN(total_revenue) AS median_value
FROM TBL_TRAINING_SET

UNION ALL

SELECT
    'total_transactions',
    AVG(total_transactions),
    STDDEV(total_transactions),
    MIN(total_transactions),
    MAX(total_transactions),
    MEDIAN(total_transactions)
FROM TBL_TRAINING_SET

UNION ALL

SELECT
    'recency_score',
    AVG(recency_score),
    STDDEV(recency_score),
    MIN(recency_score),
    MAX(recency_score),
    MEDIAN(recency_score)
FROM TBL_TRAINING_SET;

SELECT * FROM VW_FEATURE_STATISTICS;

-- ============================================
-- STEP 6: Create Normalized Features (Optional)
-- ============================================

CREATE OR REPLACE VIEW VW_ML_NORMALIZED_DATASET AS
SELECT
    customer_id,
    
    -- Z-score normalization for numerical features
    (total_revenue - (SELECT AVG(total_revenue) FROM TBL_TRAINING_SET)) / 
        NULLIF((SELECT STDDEV(total_revenue) FROM TBL_TRAINING_SET), 0) AS total_revenue_norm,
    
    (total_transactions - (SELECT AVG(total_transactions) FROM TBL_TRAINING_SET)) / 
        NULLIF((SELECT STDDEV(total_transactions) FROM TBL_TRAINING_SET), 0) AS total_transactions_norm,
    
    (avg_order_value - (SELECT AVG(avg_order_value) FROM TBL_TRAINING_SET)) / 
        NULLIF((SELECT STDDEV(avg_order_value) FROM TBL_TRAINING_SET), 0) AS avg_order_value_norm,
    
    -- Keep categorical encodings as is
    cat_electronics, cat_fashion, cat_home, cat_books, cat_sports,
    device_mobile, device_desktop, device_tablet,
    risk_high, risk_medium, risk_low,
    
    -- Target
    is_high_value_customer
    
FROM VW_ML_ENCODED_DATASET;

SELECT 'ML training dataset prepared successfully!' AS status;