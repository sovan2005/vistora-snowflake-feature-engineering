-- ============================================
-- RFM (Recency, Frequency, Monetary) FEATURES
-- File: 03-feature-engineering/rfm_features.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- VIEW 2: RFM Segmentation Features
-- ============================================

CREATE OR REPLACE VIEW VW_RFM_FEATURES AS
WITH rfm_base AS (
    SELECT
        customer_id,
        -- Recency: Days since last purchase
        DATEDIFF('day', MAX(transaction_date), CURRENT_TIMESTAMP()) AS recency_days,
        
        -- Frequency: Total number of purchases
        COUNT(DISTINCT transaction_id) AS frequency_count,
        
        -- Monetary: Total revenue
        SUM(product_price * quantity * (1 - discount_percentage/100)) AS monetary_value
    FROM RAW_CUSTOMER_TRANSACTIONS
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency_count,
        monetary_value,
        
        -- Recency Score (1-5, where 5 = most recent)
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,
        
        -- Frequency Score (1-5, where 5 = most frequent)
        NTILE(5) OVER (ORDER BY frequency_count DESC) AS frequency_score,
        
        -- Monetary Score (1-5, where 5 = highest value)
        NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_score
    FROM rfm_base
)
SELECT
    customer_id,
    recency_days,
    frequency_count,
    monetary_value,
    recency_score,
    frequency_score,
    monetary_score,
    
    -- Combined RFM Score (555 is best, 111 is worst)
    CONCAT(recency_score, frequency_score, monetary_score) AS rfm_segment,
    
    -- Overall RFM Score (sum of individual scores)
    recency_score + frequency_score + monetary_score AS rfm_total_score,
    
    -- Customer Segment Classification
    CASE
        -- Champions: High recency, frequency, and monetary
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
            THEN 'Champions'
        
        -- Loyal Customers: High frequency and monetary, moderate recency
        WHEN frequency_score >= 4 AND monetary_score >= 4 
            THEN 'Loyal Customers'
        
        -- Potential Loyalists: Recent customers with good frequency
        WHEN recency_score >= 4 AND frequency_score >= 3 
            THEN 'Potential Loyalists'
        
        -- New Customers: Very recent, low frequency
        WHEN recency_score >= 4 AND frequency_score <= 2 
            THEN 'New Customers'
        
        -- At Risk: Good past customers who haven't purchased recently
        WHEN frequency_score >= 3 AND monetary_score >= 3 AND recency_score <= 2 
            THEN 'At Risk'
        
        -- Cannot Lose Them: High monetary but low recency
        WHEN monetary_score >= 4 AND recency_score <= 2 
            THEN 'Cannot Lose Them'
        
        -- Hibernating: Low recency, low frequency
        WHEN recency_score <= 2 AND frequency_score <= 2 
            THEN 'Hibernating'
        
        -- Lost Customers: Lowest recency
        WHEN recency_score = 1 
            THEN 'Lost'
        
        ELSE 'Others'
    END AS customer_lifecycle_segment,
    
    -- Risk Flag
    CASE 
        WHEN recency_days > 180 THEN 'High Risk'
        WHEN recency_days > 90 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS churn_risk,
    
    CURRENT_TIMESTAMP() AS feature_created_at
FROM rfm_scores;

-- Verify RFM features
SELECT * FROM VW_RFM_FEATURES LIMIT 10;

-- Customer segment distribution
SELECT 
    customer_lifecycle_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(monetary_value), 2) AS avg_revenue,
    ROUND(AVG(frequency_count), 2) AS avg_frequency,
    ROUND(AVG(recency_days), 2) AS avg_recency_days
FROM VW_RFM_FEATURES
GROUP BY customer_lifecycle_segment
ORDER BY customer_count DESC;

-- Churn risk distribution
SELECT 
    churn_risk,
    COUNT(*) AS customer_count,
    ROUND(AVG(monetary_value), 2) AS avg_revenue
FROM VW_RFM_FEATURES
GROUP BY churn_risk
ORDER BY 
    CASE churn_risk
        WHEN 'High Risk' THEN 1
        WHEN 'Medium Risk' THEN 2
        WHEN 'Low Risk' THEN 3
    END;

SELECT 'RFM features created successfully!' AS status;