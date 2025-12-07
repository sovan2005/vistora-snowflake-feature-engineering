-- ============================================
-- BASIC FEATURE ENGINEERING
-- File: 03-feature-engineering/basic_features.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- VIEW 1: Customer Aggregation Features
-- ============================================

CREATE OR REPLACE VIEW VW_CUSTOMER_AGGREGATION_FEATURES AS
SELECT
    t.customer_id,
    
    -- Transaction Count Features
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    COUNT(DISTINCT DATE_TRUNC('month', t.transaction_date)) AS active_months,
    COUNT(DISTINCT DATE_TRUNC('week', t.transaction_date)) AS active_weeks,
    
    -- Revenue Features
    SUM(t.product_price * t.quantity * (1 - t.discount_percentage/100)) AS total_revenue,
    AVG(t.product_price * t.quantity * (1 - t.discount_percentage/100)) AS avg_order_value,
    MAX(t.product_price * t.quantity * (1 - t.discount_percentage/100)) AS max_order_value,
    MIN(t.product_price * t.quantity * (1 - t.discount_percentage/100)) AS min_order_value,
    STDDEV(t.product_price * t.quantity * (1 - t.discount_percentage/100)) AS order_value_std,
    
    -- Product Features
    SUM(t.quantity) AS total_items_purchased,
    AVG(t.quantity) AS avg_items_per_order,
    COUNT(DISTINCT t.product_category) AS unique_categories_purchased,
    COUNT(DISTINCT t.product_name) AS unique_products_purchased,
    
    -- Discount Features
    AVG(t.discount_percentage) AS avg_discount_percentage,
    SUM(CASE WHEN t.discount_percentage > 0 THEN 1 ELSE 0 END) AS discounted_orders_count,
    SUM(CASE WHEN t.discount_percentage > 0 THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0) * 100 AS discount_usage_rate,
    
    -- Behavioral Features
    AVG(t.session_duration_minutes) AS avg_session_duration,
    AVG(t.page_views) AS avg_page_views,
    SUM(CASE WHEN t.is_first_purchase THEN 1 ELSE 0 END) AS first_purchase_count,
    
    -- Device & Payment Preferences
    MODE(t.device_type) AS preferred_device,
    MODE(t.payment_method) AS preferred_payment_method,
    MODE(t.product_category) AS preferred_category,
    MODE(t.referral_source) AS primary_referral_source,
    
    -- Temporal Features
    MIN(t.transaction_date) AS first_purchase_date,
    MAX(t.transaction_date) AS last_purchase_date,
    DATEDIFF('day', MIN(t.transaction_date), MAX(t.transaction_date)) AS customer_lifetime_days,
    DATEDIFF('day', MAX(t.transaction_date), CURRENT_TIMESTAMP()) AS days_since_last_purchase,
    
    -- Frequency Metrics
    COUNT(*) / NULLIF(DATEDIFF('day', MIN(t.transaction_date), MAX(t.transaction_date)), 0) 
        AS avg_daily_transaction_rate,
    
    -- Join with demographics
    d.customer_age,
    d.gender,
    d.customer_location,
    d.customer_segment,
    d.email_verified,
    d.phone_verified,
    DATEDIFF('day', d.account_created_date, CURRENT_DATE()) AS account_age_days,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS feature_created_at
    
FROM RAW_CUSTOMER_TRANSACTIONS t
LEFT JOIN RAW_CUSTOMER_DEMOGRAPHICS d ON t.customer_id = d.customer_id
GROUP BY 
    t.customer_id,
    d.customer_age,
    d.gender,
    d.customer_location,
    d.customer_segment,
    d.email_verified,
    d.phone_verified,
    d.account_created_date;

-- Verify the view
SELECT * FROM VW_CUSTOMER_AGGREGATION_FEATURES LIMIT 10;

-- Check feature statistics
SELECT 
    COUNT(DISTINCT customer_id) AS total_customers,
    AVG(total_revenue) AS avg_customer_revenue,
    AVG(total_transactions) AS avg_transactions_per_customer,
    AVG(customer_lifetime_days) AS avg_customer_lifetime
FROM VW_CUSTOMER_AGGREGATION_FEATURES;

SELECT 'Basic aggregation features created successfully!' AS status;