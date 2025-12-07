-- ============================================
-- FEATURE STORE IMPLEMENTATION (FIXED - NO INDEXES)
-- File: 04-feature-store/create_feature_store.sql
-- ============================================

USE DATABASE VISTORA_ML_PROJECT;
USE SCHEMA FEATURE_ENGINEERING;
USE WAREHOUSE ML_WAREHOUSE;

-- ============================================
-- TABLE 1: Feature Store - Main Storage
-- ============================================

CREATE OR REPLACE TABLE FEATURE_STORE (
    -- Primary identifiers
    feature_store_id STRING DEFAULT UUID_STRING(),
    customer_id STRING NOT NULL,
    
    -- Feature metadata
    feature_group STRING NOT NULL,  -- e.g., 'aggregation', 'rfm', 'behavioral'
    feature_name STRING NOT NULL,
    feature_value VARIANT,  -- Flexible storage for any data type
    feature_data_type STRING,  -- 'numeric', 'categorical', 'boolean', 'timestamp'
    
    -- Versioning and lineage
    feature_version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Timestamps
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ,
    
    -- Metadata
    created_by STRING DEFAULT CURRENT_USER(),
    computation_method STRING,  -- How was this feature computed?
    source_tables STRING,  -- Which tables were used?
    
    -- Constraints
    PRIMARY KEY (feature_store_id)
);

-- NOTE: Indexes removed - not supported in standard Snowflake tables
-- For better query performance, we use clustering keys instead

-- Add clustering key for faster queries on customer_id
ALTER TABLE FEATURE_STORE CLUSTER BY (customer_id, feature_group);

-- ============================================
-- TABLE 2: Feature Metadata Registry
-- ============================================

CREATE OR REPLACE TABLE FEATURE_METADATA (
    feature_id STRING DEFAULT UUID_STRING(),
    feature_group STRING NOT NULL,
    feature_name STRING NOT NULL,
    feature_description STRING,
    feature_data_type STRING,
    feature_category STRING,  -- 'demographic', 'behavioral', 'transactional', etc.
    
    -- Computation details
    computation_logic STRING,
    source_query STRING,
    dependencies STRING,  -- JSON array of dependent features
    
    -- Usage metadata
    update_frequency STRING,  -- 'real-time', 'daily', 'weekly', 'monthly'
    last_updated TIMESTAMP_NTZ,
    version INTEGER DEFAULT 1,
    
    -- Quality metrics
    null_percentage DECIMAL(5,2),
    uniqueness_percentage DECIMAL(5,2),
    
    -- Ownership
    owner STRING,
    team STRING,
    created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (feature_id)
);

-- ============================================
-- TABLE 3: Feature Usage Tracking
-- ============================================

CREATE OR REPLACE TABLE FEATURE_USAGE_LOG (
    log_id STRING DEFAULT UUID_STRING(),
    customer_id STRING,
    feature_group STRING,
    feature_names ARRAY,  -- List of features accessed
    access_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    access_purpose STRING,  -- 'training', 'prediction', 'analysis'
    model_id STRING,
    user_id STRING DEFAULT CURRENT_USER(),
    
    PRIMARY KEY (log_id)
);

-- ============================================
-- TABLE 4: Feature Quality Metrics
-- ============================================

CREATE OR REPLACE TABLE FEATURE_QUALITY_METRICS (
    metric_id STRING DEFAULT UUID_STRING(),
    feature_group STRING,
    feature_name STRING,
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Data quality metrics
    total_records INTEGER,
    null_count INTEGER,
    null_percentage DECIMAL(5,2),
    unique_count INTEGER,
    uniqueness_percentage DECIMAL(5,2),
    
    -- Statistical metrics
    min_value VARIANT,
    max_value VARIANT,
    mean_value DECIMAL(18,6),
    median_value DECIMAL(18,6),
    std_dev DECIMAL(18,6),
    
    -- Anomaly flags
    has_anomalies BOOLEAN DEFAULT FALSE,
    anomaly_description STRING,
    
    PRIMARY KEY (metric_id)
);

-- ============================================
-- VERIFICATION
-- ============================================

-- Verify tables created
SHOW TABLES LIKE 'FEATURE%';

-- Check table structure
DESC TABLE FEATURE_STORE;

SELECT 'Feature Store tables created successfully!' AS status;