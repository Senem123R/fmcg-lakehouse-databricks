# 🏋️ Sports & Nutrition FMCG Data Integration Pipeline
### End-to-End ETL Pipeline for Sports Equipment & Energy Bar Company Merger | Databricks · AWS S3 · Medallion Architecture

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)

---

## 📌 Project Overview

**Atliqon** (large FMCG company) acquired **Atlion** (sports equipment & energy bar manufacturer). While Atliqon had a mature data infrastructure, Atlion's data existed only as raw files in Amazon S3 with no engineering system in place.

This project delivers a **fully automated, production-grade ETL pipeline** that ingests Atlion's raw operational data, applies multi-layer transformations using **Medallion Architecture (Bronze → Silver → Gold)**, and surfaces analytics-ready tables in Databricks Unity Catalog for consolidated BI reporting across both companies.

---

## 🏗️ Architecture

![Architecture Diagram](project_architecture.svg)

---

## 📁 Project Structure

```
📦 fmcg-data-pipeline/
├── 📂 setup/
│   ├── setup_catalog.ipynb          # Creates fmcg catalog + bronze/silver/gold schemas
│   └── utilities.ipynb              # Shared schema variables
│
├── 📂 dimensions/
│   ├── 1_customers_data_processing.ipynb    # dim_customer transformation
│   ├── 2_products_data_processing.ipynb     # dim_product transformation
│   ├── 3_pricing_data_processing.ipynb      # dim_price / gross price transformation
│   └── dim_date_table_creation.ipynb        # Custom date dimension (2024–2025)
│
├── 📂 facts/
│   ├── 1_full_load_fact.ipynb               # Full load for fact_orders
│   └── 2_incremental_load_fact.ipynb        # Delta MERGE-based incremental load
│
└── 📂 architecture/
    └── project_architecture.svg             # Architecture diagram
```

---

## 🔄 Pipeline Flow

```
1. Raw files land in AWS S3
          ↓
2. Bronze Layer  — Ingest raw data as-is into Delta tables
          ↓
3. Silver Layer  — Clean, deduplicate, apply SCD logic, enforce schema
          ↓
4. Gold Layer    — Build Star Schema (dim + fact tables)
          ↓
5. Unity Catalog — Govern & expose tables for BI consumption
          ↓
6. Databricks Workflow — Orchestrate all tasks, run daily at 5 PM
```

---

## 📊 Data Model (Gold Layer — Star Schema)

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │─────────────────│
                    │ date_key (PK)   │
                    │ month_start_date│
                    │ year            │
                    │ month_name      │
                    │ quarter         │
                    │ year_quarter    │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼────────┐    ┌──────────────────┐
│ dim_customer │    │   fact_orders   │    │   dim_product    │
│──────────────│    │─────────────────│    │──────────────────│
│ customer_id  │◄───│ customer_id(FK) │───►│ product_id (PK)  │
│ customer_name│    │ product_id (FK) │    │ product_name     │
│ market       │    │ date_key (FK)   │    │ category         │
│ platform     │    │ order_qty       │    │ segment          │
│ channel      │    │ gross_price     │    └──────────────────┘
└──────────────┘    │ net_sales       │
                    │ load_type       │    ┌──────────────────┐
                    └────────┬────────┘    │   dim_price      │
                             │             │──────────────────│
                             └────────────►│ product_id (FK)  │
                                           │ gross_price      │
                                           │ fiscal_year      │
                                           └──────────────────┘
```

---

## ⚙️ Tech Stack

| Category | Technology |
|---|---|
| Cloud Storage | AWS S3 |
| Compute & Processing | Databricks, Apache Spark, PySpark |
| Languages | Python, Spark SQL |
| Storage Format | Delta Lake (ACID, MERGE, Time Travel) |
| Architecture | Medallion Architecture, Star Schema |
| Load Strategy | Full Load + Incremental Load (MERGE/UPSERT) |
| Orchestration | Databricks Workflows (Jobs & Pipelines) |
| Scheduling | Cron — Daily 5:00 PM UTC+5:30 |
| Data Governance | Databricks Unity Catalog |
| BI Layer | Power BI / Tableau compatible Gold tables |

---

## 🗂️ Databricks Workflow

```
Workflow: fmcg  (Daily — 5:00 PM UTC+5:30)
│
├── Task 1: dim_processing_customer    ✅
├── Task 2: dim_processing_products    ✅
├── Task 3: dim_processing_prices      ✅
└── Task 4: fact_order_processing      ✅

Average run time: ~2m 56s
Lineage: 10 upstream tables → 16 downstream tables
```

---

## 🚀 How to Run

### Prerequisites
- Databricks workspace (Free/Standard edition works)
- AWS S3 bucket with source data folders: `customers/`, `products/`, `orders/`, `gross_price/`
- Databricks cluster with Spark runtime

### Step 1 — Setup Catalog
```sql
-- Run setup_catalog.ipynb
CREATE CATALOG IF NOT EXISTS fmcg;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
CREATE SCHEMA IF NOT EXISTS fmcg.gold;
```

### Step 2 — Configure S3 Connection
```python
# In Databricks, configure your S3 mount or use instance profile
spark.conf.set("fs.s3a.access.key", "<your-access-key>")
spark.conf.set("fs.s3a.secret.key", "<your-secret-key>")
```

### Step 3 — Run Notebooks in Order
```
1. setup/setup_catalog.ipynb
2. dimensions/dim_date_table_creation.ipynb
3. dimensions/1_customers_data_processing.ipynb
4. dimensions/2_products_data_processing.ipynb
5. dimensions/3_pricing_data_processing.ipynb
6. facts/1_full_load_fact.ipynb          ← First time only
7. facts/2_incremental_load_fact.ipynb   ← Daily runs
```

### Step 4 — Schedule via Databricks Workflow
1. Go to **Jobs & Pipelines** in Databricks
2. Create new job named `fmcg`
3. Add 4 tasks in sequence (customer → products → prices → fact)
4. Set trigger: **Scheduled → Every day at 5:00 PM**

---

## 📈 Key Outcomes

- ✅ **Zero manual effort** — fully automated daily pipeline
- ✅ **Unified analytics** — consolidated view across both companies post-merger
- ✅ **Data quality enforced** — deduplication + SCD + schema validation at Silver layer
- ✅ **~3 min end-to-end** pipeline runtime
- ✅ **16 downstream tables** available for BI dashboards
- ✅ **Incremental loads** — only new/changed records processed daily

---

## 💡 Key Concepts Demonstrated

| Concept | Where Used |
|---|---|
| Medallion Architecture | Full Bronze → Silver → Gold pipeline |
| Delta Lake | All table writes use `.format("delta")` |
| MERGE / UPSERT | Incremental fact load (`2_incremental_load_fact.ipynb`) |
| SCD (Slowly Changing Dimensions) | Customer & Product Silver layer |
| Star Schema | Gold layer — dim + fact structure |
| Unity Catalog | `fmcg` catalog with governed schemas |
| Workflow Orchestration | Databricks Jobs with task dependencies |
| Incremental vs Full Load | Separate notebooks for each strategy |

---

## 👤 Author

**Risni Maleesha**
Data Engineer | Databricks | AWS | PySpark

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/your-profile)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/your-username)


---

*Built with ❤️ using Databricks + AWS S3 | FMCG Domain | 2026*
---
