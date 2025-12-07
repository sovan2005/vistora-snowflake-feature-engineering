# Feature Engineering with Snowflake & Feature Store

**Vistora AI - ML Assignment Project**

## Project Overview

This project demonstrates a complete end-to-end **Feature Engineering Pipeline** using Snowflake as the primary data warehouse and implementing a custom **Feature Store** for machine learning model training. The use case focuses on **Customer Lifetime Value (CLV) Prediction** for an e-commerce platform.

### Objectives

1. Build a scalable feature engineering pipeline in Snowflake
2. Implement a production-ready Feature Store with versioning
3. Create and store features for ML model training
4. Train and evaluate a customer segmentation model
5. Demonstrate feature retrieval for real-time predictions

---

## Architecture

```
Raw Data (Snowflake Tables)
    ↓
Feature Engineering (SQL Views & Transformations)
    ↓
Feature Store (Centralized Feature Repository)
    ↓
ML Training Pipeline (Feature Retrieval & Model Training)
    ↓
Model Predictions & Evaluation
```

### Key Components

- **Data Layer**: Raw customer transactions and demographics
- **Feature Engineering Layer**: Aggregations, RFM analysis, behavioral features
- **Feature Store**: Versioned, metadata-rich feature repository
- **ML Layer**: Model training, evaluation, and prediction

---

## Project Structure

```
vistora-snowflake-feature-engineering/
│
├── 01-setup/
│   └── snowflake_setup.sql              # Initial database setup
│
├── 02-data-generation/
│   ├── create_tables.sql                 # Table schemas
│   └── generate_sample_data.sql          # Sample data generation
│
├── 03-feature-engineering/
│   ├── basic_features.sql                # Aggregation features
│   ├── rfm_features.sql                  # RFM segmentation
│   └── feature_documentation.md          # Feature descriptions
│
├── 04-feature-store/
│   ├── create_feature_store.sql          # Feature store tables
│   ├── populate_feature_store.sql        # Load features
│   └── feature_retrieval.sql             # Retrieval functions
│
├── 05-ml-pipeline/
│   ├── training_dataset.sql              # Training data prep
│   └── model_training.sql                # Model training & evaluation
│
└── README.md                              # This file
```

---

##  Getting Started

### Prerequisites

- Snowflake account (free trial: https://signup.snowflake.com/)
- Basic SQL knowledge
- Understanding of ML concepts

### Step 1: Setup Snowflake

1. Create Snowflake account
2. Run `01-setup/snowflake_setup.sql`

### Step 2: Generate Data

1. Run `02-data-generation/create_tables.sql`
2. Run `02-data-generation/generate_sample_data.sql`

### Step 3: Create Features

1. Run `03-feature-engineering/basic_features.sql`
2. Run `03-feature-engineering/rfm_features.sql`

### Step 4: Build Feature Store

1. Run `04-feature-store/create_feature_store.sql`
2. Run `04-feature-store/populate_feature_store.sql`
3. Run `04-feature-store/feature_retrieval.sql`

### Step 5: Train Model

1. Run `05-ml-pipeline/training_dataset.sql`
2. Run `05-ml-pipeline/model_training.sql`

---

## Key Features

### 1. Feature Engineering Types

| Feature Type | Examples | Purpose |
|-------------|----------|---------|
| **Aggregation** | Total revenue, transaction count | Customer value metrics |
| **RFM** | Recency, Frequency, Monetary scores | Customer segmentation |
| **Behavioral** | Preferred device, category | User preferences |
| **Temporal** | Days since last purchase | Engagement tracking |

### 2. Feature Store Capabilities

 **Version Control**: Track feature changes over time  
 **Metadata Management**: Document feature lineage  
 **Quality Metrics**: Monitor data quality  
 **Usage Tracking**: Log feature access  
 **Batch & Real-time Retrieval**: Flexible access patterns

### 3. ML Pipeline Features

- Train/test split (80/20)
- Feature encoding (one-hot, normalization)
- Model evaluation metrics (accuracy, precision, recall, F1)
- Feature importance analysis
- Model registry

---

## Data Schema

### Main Tables

**RAW_CUSTOMER_TRANSACTIONS**
- Transaction-level data
- 2000+ records
- Product, payment, device info

**RAW_CUSTOMER_DEMOGRAPHICS**
- Customer profiles
- 200 unique customers
- Age, location, segment

**FEATURE_STORE**
- Versioned feature storage
- Multiple feature groups
- Metadata tracking

---

## 🎓 Feature Engineering Techniques

### 1. Aggregation Features

```sql
-- Example: Customer lifetime value
SUM(product_price * quantity * (1 - discount/100)) AS total_revenue
```

### 2. RFM Segmentation

```sql
-- Recency Score (1-5)
NTILE(5) OVER (ORDER BY MAX(transaction_date) DESC)
```

### 3. Behavioral Features

```sql
-- Most preferred category
MODE(product_category) AS preferred_category
```

### 4. Time-based Features

```sql
-- Days since last purchase
DATEDIFF('day', MAX(transaction_date), CURRENT_TIMESTAMP())
```

---

## 📈 Model Performance

| Metric | Value |
|--------|-------|
| Accuracy | ~75-85% |
| Precision | ~70-80% |
| Recall | ~65-75% |
| F1-Score | ~70-77% |

*Note: Actual values depend on data distribution*

---

## 🔍 Feature Store vs Traditional Approach

| Aspect | Feature Store | Traditional |
|--------|---------------|-------------|
| Feature Reusability | High
| Consistency |  Guaranteed 
| Versioning |  Built-in 
| Discoverability |  Centralized
| Governance |  Strong 

---

## 🛠️ Technologies Used

- **Snowflake**: Data warehouse & compute
- **SQL**: Feature engineering & transformations
- **Snowflake Functions**: UDFs, stored procedures
- **Snowflake Views**: Feature abstraction

---

## 📝 Key Learnings

1. **Feature Stores** centralize feature management and improve ML workflow efficiency
2. **Snowflake** provides powerful compute for large-scale feature engineering
3. **RFM Analysis** is highly effective for customer segmentation
4. **Feature versioning** is critical for reproducible ML models
5. **Metadata tracking** enables better feature governance

---

## 🔮 Future Enhancements

- [ ] Real-time feature computation with Snowflake Streams
- [ ] Integration with Snowpark ML for advanced models
- [ ] Automated feature quality monitoring
- [ ] Feature drift detection
- [ ] A/B testing framework integration

---

## 📚 References

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Feature Store Concepts](https://www.featurestore.org/)
- [RFM Analysis Guide](https://en.wikipedia.org/wiki/RFM_(market_research))

---
