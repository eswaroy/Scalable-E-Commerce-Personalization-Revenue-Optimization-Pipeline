# Scalable E-Commerce Personalization & Revenue Optimization Pipeline

## 🚀 Project Overview

This project implements a scalable distributed recommendation system inspired by Amazon’s personalization architecture.  
The system processes 10M+ user-product interactions and builds a production-style data pipeline using Hadoop, Hive, and Spark.

The objective is to:
- Analyze customer behavior at scale
- Train distributed recommendation models
- Compare personalization vs popularity baseline
- Simulate revenue uplift from personalized ranking

---

## 🏗 System Architecture

### Layer 1 — Data Lake (HDFS)
- Raw Amazon review JSON ingested into HDFS
- Converted to Parquet format
- Partitioned by category, year, month
- Optimized for distributed query performance

### Layer 2 — Data Warehouse (Hive + Spark SQL)
- Star schema design:
  - `fact_reviews` (10M+ records)
  - `dim_products` (1M+ products)
- External tables built over Parquet
- Distributed analytical queries executed at scale

### Layer 3 — Distributed Processing (Spark)
- Feature engineering at scale
- High-value customer analysis
- Product popularity modeling
- Seasonal trend analysis

### Layer 4 — ML Recommendation Engine
- Popularity baseline model
- Implicit ALS collaborative filtering (Spark MLlib)
- Precision@10 evaluation
- Revenue uplift simulation

---

## 📊 Dataset

Amazon Reviews 2023 Dataset  
Category: Beauty & Personal Care  
Subset Used: 10 Million interactions  

Fields used:
- user_id
- parent_asin
- rating
- timestamp
- product metadata (title, price, brand)

---

## ⚙️ Technology Stack

- Hadoop (HDFS)
- Hive Metastore
- Spark SQL
- PySpark
- Spark MLlib (ALS)
- Parquet Storage Format

---

## 🔍 Analytical Insights

- Identified high-value customers (2,000+ reviews per user)
- Analyzed product popularity distribution
- Evaluated verified purchase impact
- Computed revenue proxy via review count × price
- Performed seasonal demand analysis

---

## 🤖 Recommendation Models

### 1️⃣ Popularity Baseline
- Recommends globally most reviewed products

### 2️⃣ Implicit ALS Model
- Collaborative filtering using confidence-weighted feedback
- Sparse user-item matrix handling
- Hash-based indexing to avoid driver memory bottlenecks
- Optimized for distributed training

---

## 📈 Model Evaluation

Metric: Precision@10

| Model        | Precision@10 |
|--------------|--------------|
| Popularity   | 0.00185      |
| ALS Model    | 0.00333      |

✔ ALS achieved ~1.8× improvement over baseline

---

## 💰 Revenue Uplift Simulation

Assumptions:
- 1,000,000 impressions
- 5% conversion rate
- $40 average order value

Results:

- Baseline Revenue: $3,706
- Personalized Revenue: $6,664
- Revenue Uplift: **~80%**

This demonstrates how ranking improvements can significantly impact business revenue at scale.

---

## 🧠 Engineering Challenges Solved

- Handled high-cardinality user indexing without StringIndexer
- Prevented Spark driver OOM using hash-based indexing
- Reduced shuffle pressure using repartitioning
- Switched from explicit RMSE to implicit ranking optimization
- Designed evaluation logic aligned with recommender best practices

---


