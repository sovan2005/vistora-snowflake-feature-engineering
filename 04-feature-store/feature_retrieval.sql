-- ============================================
-- FEATURE RETRIEVAL FUNCTIONS
-- File: 04-feature-store/feature_retrieval.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- FUNCTION 1: Get All Features for a Customer
-- ============================================

CREATE OR REPLACE FUNCTION FN_GET_CUSTOMER_FEATURES(input_customer_id STRING)
RETURNS TABLE (
    feature_group STRING,
    feature_name STRING,
    feature_value VARIANT,
    feature_data_type STRING,
    created_at TIMESTAMP_NTZ
)
AS
$$
    SELECT 
        feature_group,
        feature_name,
        feature_value,
        feature_data_type,
        created_at
    FROM FEATURE_STORE
    WHERE customer_id = input_customer_id
    AND is_active = TRUE
    AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP())
    ORDER BY feature_group, feature_name
$$;

-- Test the function
SELECT * FROM TABLE(FN_GET_CUSTOMER_FEATURES('CUST_000001'));

-- ============================================
-- FUNCTION 2: Get Features by Group
-- ============================================

CREATE OR REPLACE FUNCTION FN_GET_FEATURES_BY_GROUP(
    input_customer_id STRING,
    input_feature_group STRING
)
RETURNS TABLE (
    feature_name STRING,
    feature_value VARIANT,
    created_at TIMESTAMP_NTZ
)
AS
$$
    SELECT 
        feature_name,
        feature_value,
        created_at
    FROM FEATURE_STORE
    WHERE customer_id = input_customer_id
    AND feature_group = input_feature_group
    AND is_active = TRUE
    AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP())
    ORDER BY feature_name
$$;

-- Test
SELECT * FROM TABLE(FN_GET_FEATURES_BY_GROUP('CUST_000001', 'rfm'));

-- ============================================
-- FUNCTION 3: Get Specific Features (for ML models)
-- ============================================

CREATE OR REPLACE FUNCTION FN_GET_SPECIFIC_FEATURES(
    input_customer_id STRING,
    feature_list ARRAY
)
RETURNS TABLE (
    feature_name STRING,
    feature_value VARIANT
)
AS
$$
    SELECT 
        feature_name,
        feature_value
    FROM FEATURE_STORE
    WHERE customer_id = input_customer_id
    AND feature_name IN (SELECT VALUE FROM TABLE(FLATTEN(input => feature_list)))
    AND is_active = TRUE
    AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP())
$$;

-- Test with array of feature names
SELECT * FROM TABLE(FN_GET_SPECIFIC_FEATURES(
    'CUST_000001', 
    ARRAY_CONSTRUCT('total_revenue', 'rfm_segment', 'churn_risk')
));

-- ============================================
-- VIEW: ML Ready Feature Matrix
-- ============================================

CREATE OR REPLACE VIEW VW_ML_FEATURE_MATRIX AS
SELECT
    fs.customer_id,
    
    -- Pivot features into columns for ML
    MAX(CASE WHEN fs.feature_name = 'total_revenue' THEN fs.feature_value END)::DECIMAL(18,2) AS total_revenue,
    MAX(CASE WHEN fs.feature_name = 'total_transactions' THEN fs.feature_value END)::INTEGER AS total_transactions,
    MAX(CASE WHEN fs.feature_name = 'avg_order_value' THEN fs.feature_value END)::DECIMAL(18,2) AS avg_order_value,
    MAX(CASE WHEN fs.feature_name = 'days_since_last_purchase' THEN fs.feature_value END)::INTEGER AS days_since_last_purchase,
    MAX(CASE WHEN fs.feature_name = 'recency_score' THEN fs.feature_value END)::INTEGER AS recency_score,
    MAX(CASE WHEN fs.feature_name = 'frequency_score' THEN fs.feature_value END)::INTEGER AS frequency_score,
    MAX(CASE WHEN fs.feature_name = 'monetary_score' THEN fs.feature_value END)::INTEGER AS monetary_score,
    MAX(CASE WHEN fs.feature_name = 'preferred_category' THEN fs.feature_value END)::STRING AS preferred_category,
    MAX(CASE WHEN fs.feature_name = 'preferred_device' THEN fs.feature_value END)::STRING AS preferred_device,
    MAX(CASE WHEN fs.feature_name = 'customer_lifecycle_segment' THEN fs.feature_value END)::STRING AS customer_lifecycle_segment,
    MAX(CASE WHEN fs.feature_name = 'churn_risk' THEN fs.feature_value END)::STRING AS churn_risk
    
FROM FEATURE_STORE fs
WHERE fs.is_active = TRUE
AND (fs.valid_to IS NULL OR fs.valid_to > CURRENT_TIMESTAMP())
GROUP BY fs.customer_id;

-- Verify ML feature matrix
SELECT * FROM VW_ML_FEATURE_MATRIX LIMIT 20;

-- ============================================
-- STORED PROCEDURE: Batch Feature Retrieval
-- ============================================

CREATE OR REPLACE PROCEDURE SP_GET_BATCH_FEATURES(customer_ids ARRAY)
RETURNS TABLE (
    customer_id STRING,
    feature_name STRING,
    feature_value VARIANT
)
LANGUAGE SQL
AS
$$
BEGIN
    LET result RESULTSET := (
        SELECT 
            customer_id,
            feature_name,
            feature_value
        FROM FEATURE_STORE
        WHERE customer_id IN (SELECT VALUE FROM TABLE(FLATTEN(input => :customer_ids)))
        AND is_active = TRUE
        AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP())
        ORDER BY customer_id, feature_group, feature_name
    );
    RETURN TABLE(result);
END;
$$;

-- Test batch retrieval
CALL SP_GET_BATCH_FEATURES(ARRAY_CONSTRUCT('CUST_000001', 'CUST_000002', 'CUST_000003'));

-- ============================================
-- PROCEDURE: Log Feature Access (for tracking)
-- ============================================

CREATE OR REPLACE PROCEDURE SP_LOG_FEATURE_ACCESS(
    p_customer_id STRING,
    p_feature_group STRING,
    p_access_purpose STRING,
    p_model_id STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO FEATURE_USAGE_LOG (
        customer_id,
        feature_group,
        feature_names,
        access_purpose,
        model_id
    )
    SELECT
        :p_customer_id,
        :p_feature_group,
        ARRAY_AGG(feature_name),
        :p_access_purpose,
        :p_model_id
    FROM FEATURE_STORE
    WHERE customer_id = :p_customer_id
    AND feature_group = :p_feature_group
    AND is_active = TRUE;
    
    RETURN 'Feature access logged successfully';
END;
$$;

SELECT 'Feature retrieval functions created successfully!' AS status;