-- ============================================
-- ML MODEL TRAINING IN SNOWFLAKE - CLEAN VERSION
-- File: 05-ml-pipeline/model_training.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- STEP 1: Create Model Training Feature View
-- ============================================

CREATE OR REPLACE VIEW VW_MODEL_TRAINING_FEATURES AS
SELECT
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
    cat_electronics,
    cat_fashion,
    cat_home,
    cat_books,
    cat_sports,
    device_mobile,
    device_desktop,
    device_tablet,
    risk_high,
    risk_medium,
    risk_low,
    segment_premium,
    segment_regular,
    is_high_value_customer AS target
FROM TBL_TRAINING_SET;

-- Verify
SELECT 'Step 1: Training features view created' AS status;

-- ============================================
-- STEP 2: Generate Model Predictions
-- ============================================

CREATE OR REPLACE TABLE TBL_MODEL_PREDICTIONS AS
SELECT
    t.customer_id,
    t.is_high_value_customer AS actual_value,
    CASE
        WHEN t.recency_score >= 4 AND t.frequency_score >= 4 AND t.monetary_score >= 4 THEN 1
        WHEN t.total_revenue > 50000 THEN 1
        WHEN t.recency_score >= 4 AND t.frequency_score >= 3 THEN 1
        ELSE 0
    END AS predicted_value,
    CASE
        WHEN t.recency_score >= 4 AND t.frequency_score >= 4 AND t.monetary_score >= 4 THEN 0.95
        WHEN t.total_revenue > 50000 THEN 0.85
        WHEN t.recency_score >= 4 AND t.frequency_score >= 3 THEN 0.75
        WHEN t.recency_score >= 3 AND t.frequency_score >= 3 THEN 0.60
        ELSE 0.30
    END AS confidence_score,
    t.total_revenue,
    t.recency_score,
    t.frequency_score,
    t.monetary_score,
    CURRENT_TIMESTAMP() AS prediction_timestamp
FROM TBL_TESTING_SET t;

-- Verify
SELECT 'Step 2: Predictions created' AS status;
SELECT COUNT(*) AS total_predictions FROM TBL_MODEL_PREDICTIONS;

-- ============================================
-- STEP 3: Calculate Confusion Matrix
-- ============================================

CREATE OR REPLACE VIEW VW_CONFUSION_MATRIX AS
SELECT
    SUM(CASE WHEN actual_value = 1 AND predicted_value = 1 THEN 1 ELSE 0 END) AS true_positive,
    SUM(CASE WHEN actual_value = 0 AND predicted_value = 0 THEN 1 ELSE 0 END) AS true_negative,
    SUM(CASE WHEN actual_value = 0 AND predicted_value = 1 THEN 1 ELSE 0 END) AS false_positive,
    SUM(CASE WHEN actual_value = 1 AND predicted_value = 0 THEN 1 ELSE 0 END) AS false_negative
FROM TBL_MODEL_PREDICTIONS;

-- Display
SELECT 'Step 3: Confusion Matrix' AS status;
SELECT * FROM VW_CONFUSION_MATRIX;

-- ============================================
-- STEP 4: Calculate Performance Metrics
-- ============================================

CREATE OR REPLACE VIEW VW_MODEL_METRICS AS
SELECT
    ROUND((true_positive + true_negative) * 100.0 / (true_positive + true_negative + false_positive + false_negative), 2) AS accuracy_percentage,
    ROUND(true_positive * 100.0 / NULLIF(true_positive + false_positive, 0), 2) AS precision_percentage,
    ROUND(true_positive * 100.0 / NULLIF(true_positive + false_negative, 0), 2) AS recall_percentage,
    ROUND(2.0 * true_positive / NULLIF(2.0 * true_positive + false_positive + false_negative, 0) * 100, 2) AS f1_score_percentage,
    ROUND(true_negative * 100.0 / NULLIF(true_negative + false_positive, 0), 2) AS specificity_percentage
FROM VW_CONFUSION_MATRIX;

-- Display
SELECT 'Step 4: Model Performance Metrics' AS status;
SELECT * FROM VW_MODEL_METRICS;

-- ============================================
-- STEP 5: Feature Importance
-- ============================================

CREATE OR REPLACE VIEW VW_FEATURE_IMPORTANCE AS
SELECT 'recency_score' AS feature_name, CORR(recency_score, is_high_value_customer) AS correlation, ABS(CORR(recency_score, is_high_value_customer)) AS importance_score FROM TBL_TRAINING_SET
UNION ALL
SELECT 'frequency_score', CORR(frequency_score, is_high_value_customer), ABS(CORR(frequency_score, is_high_value_customer)) FROM TBL_TRAINING_SET
UNION ALL
SELECT 'monetary_score', CORR(monetary_score, is_high_value_customer), ABS(CORR(monetary_score, is_high_value_customer)) FROM TBL_TRAINING_SET
UNION ALL
SELECT 'total_revenue', CORR(total_revenue, is_high_value_customer), ABS(CORR(total_revenue, is_high_value_customer)) FROM TBL_TRAINING_SET
UNION ALL
SELECT 'total_transactions', CORR(total_transactions, is_high_value_customer), ABS(CORR(total_transactions, is_high_value_customer)) FROM TBL_TRAINING_SET
UNION ALL
SELECT 'avg_order_value', CORR(avg_order_value, is_high_value_customer), ABS(CORR(avg_order_value, is_high_value_customer)) FROM TBL_TRAINING_SET
ORDER BY importance_score DESC;

-- Display
SELECT 'Step 5: Feature Importance' AS status;
SELECT * FROM VW_FEATURE_IMPORTANCE;

-- ============================================
-- STEP 6: Create Model Registry
-- ============================================

CREATE OR REPLACE TABLE TBL_MODEL_REGISTRY (
    model_id STRING DEFAULT UUID_STRING(),
    model_name STRING,
    model_type STRING,
    model_version STRING,
    training_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    features_used ARRAY,
    accuracy DECIMAL(5,2),
    precision_score DECIMAL(5,2),
    recall_score DECIMAL(5,2),
    f1_score DECIMAL(5,2),
    training_records INTEGER,
    testing_records INTEGER,
    is_production BOOLEAN DEFAULT FALSE,
    created_by STRING DEFAULT CURRENT_USER(),
    model_description STRING,
    PRIMARY KEY (model_id)
);

SELECT 'Step 6: Model registry created' AS status;

-- ============================================
-- STEP 7: Register Model
-- ============================================

INSERT INTO TBL_MODEL_REGISTRY (
    model_name, model_type, model_version, features_used,
    accuracy, precision_score, recall_score, f1_score,
    training_records, testing_records, is_production, model_description
)
SELECT
    'High_Value_Customer_Predictor',
    'Rule-Based Classifier',
    'v1.0',
    ARRAY_CONSTRUCT('recency_score', 'frequency_score', 'monetary_score', 'total_revenue', 'total_transactions', 'avg_order_value'),
    (SELECT accuracy_percentage FROM VW_MODEL_METRICS),
    (SELECT precision_percentage FROM VW_MODEL_METRICS),
    (SELECT recall_percentage FROM VW_MODEL_METRICS),
    (SELECT f1_score_percentage FROM VW_MODEL_METRICS),
    (SELECT COUNT(*) FROM TBL_TRAINING_SET),
    (SELECT COUNT(*) FROM TBL_TESTING_SET),
    TRUE,
    'Baseline model using RFM scores and revenue metrics';

SELECT 'Step 7: Model registered' AS status;
SELECT * FROM TBL_MODEL_REGISTRY;

-- ============================================
-- STEP 8: Create Prediction Function
-- ============================================

CREATE OR REPLACE FUNCTION FN_PREDICT_HIGH_VALUE_CUSTOMER(
    p_recency_score INTEGER,
    p_frequency_score INTEGER,
    p_monetary_score INTEGER,
    p_total_revenue DECIMAL
)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'prediction', 
        CASE
            WHEN p_recency_score >= 4 AND p_frequency_score >= 4 AND p_monetary_score >= 4 THEN 1
            WHEN p_total_revenue > 50000 THEN 1
            WHEN p_recency_score >= 4 AND p_frequency_score >= 3 THEN 1
            ELSE 0
        END,
        'confidence',
        CASE
            WHEN p_recency_score >= 4 AND p_frequency_score >= 4 AND p_monetary_score >= 4 THEN 0.95
            WHEN p_total_revenue > 50000 THEN 0.85
            WHEN p_recency_score >= 4 AND p_frequency_score >= 3 THEN 0.75
            ELSE 0.30
        END,
        'segment',
        CASE
            WHEN p_recency_score >= 4 AND p_frequency_score >= 4 AND p_monetary_score >= 4 THEN 'Champion'
            WHEN p_total_revenue > 50000 THEN 'High Value'
            WHEN p_recency_score >= 4 THEN 'Potential Loyalist'
            ELSE 'Regular'
        END
    )
$$;

SELECT 'Step 8: Prediction function created' AS status;

-- Test function
SELECT 'Test 1: Champion Customer' AS test_case, FN_PREDICT_HIGH_VALUE_CUSTOMER(5, 5, 5, 100000) AS result
UNION ALL
SELECT 'Test 2: High Revenue', FN_PREDICT_HIGH_VALUE_CUSTOMER(3, 3, 3, 75000)
UNION ALL
SELECT 'Test 3: Regular', FN_PREDICT_HIGH_VALUE_CUSTOMER(2, 2, 2, 15000);

-- ============================================
-- FINAL SUMMARY
-- ============================================

SELECT 'ALL 10 FILES COMPLETED SUCCESSFULLY!' AS final_status;

-- Show model metrics summary
SELECT 
    'Accuracy' AS metric,
    accuracy_percentage AS value
FROM VW_MODEL_METRICS
UNION ALL
SELECT 'Precision', precision_percentage FROM VW_MODEL_METRICS
UNION ALL
SELECT 'Recall', recall_percentage FROM VW_MODEL_METRICS
UNION ALL
SELECT 'F1-Score', f1_score_percentage FROM VW_MODEL_METRICS;

-- Show top features
SELECT feature_name, ROUND(importance_score, 4) AS importance
FROM VW_FEATURE_IMPORTANCE
ORDER BY importance_score DESC
LIMIT 5;