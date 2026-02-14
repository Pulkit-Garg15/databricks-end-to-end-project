# 🚀 Azure Databricks End-to-End Data Engineering Project

## 📌 Overview

This repository contains a **production-style end-to-end data engineering platform** built on **Azure Databricks**, implementing a scalable **Medallion Architecture (Bronze → Silver → Gold)**.

The project demonstrates how enterprise data platforms are designed using:
- Incremental ingestion with **Databricks Autoloader**
- Domain-oriented transformations
- **Delta Live Tables (DLT)** for stateful dimensions
- YAML-based orchestration using **Databricks Jobs**

The focus is on **scalability, data quality, and real-world design patterns**, not just notebook execution.

---

## 🏗️ Architecture

![Architecture Diagram](https://raw.githubusercontent.com/Pulkit-Garg15/databricks-end-to-end-project/main/docs/architecture.png)

### 🔹 Bronze Layer
- Incremental file ingestion from **Azure Data Lake Storage (ADLS)**
- Implemented using **Databricks Autoloader (cloudFiles)**
- Metadata-driven ingestion using a parameter notebook
- Raw data persisted as Delta tables

### 🔹 Silver Layer
Domain-based curated datasets:
- Customers
- Orders
- Products
- Regions

Responsibilities:
- Schema standardization
- Data cleansing
- Business-level transformations
- Independent, parallel execution per domain

### 🔹 Gold Layer

| Dataset | Implementation | Strategy |
|------|---------------|----------|
| Customers | Delta Lake | SCD Type 1 |
| Products | Delta Live Tables | SCD Type 2 |
| Orders | Delta Lake | Fact Table |

#### Why a Mixed SCD Strategy?
- **SCD Type 1** is used where historical tracking is not required (e.g. customers)
- **SCD Type 2** is implemented for product data using **DLT `apply_changes`** to track historical changes
- Fact tables depend on fully processed Gold dimensions

---

## 🔄 Orchestration & Pipelines

End-to-end execution is orchestrated using **Databricks Jobs (YAML)**:

- Metadata-driven ingestion using `for_each_task`
- Parallel Silver processing
- Gold layer executed with strict dependencies
- **DLT pipeline executed as a first-class job task**
- Serverless compute with **Photon enabled**

The Gold Products pipeline is defined and deployed using **DLT YAML configuration**.

---

## 🛠️ Technologies Used

- Azure Databricks
- Delta Lake
- Delta Live Tables (DLT)
- Databricks Autoloader
- PySpark
- Azure Data Lake Storage (ADLS)
- Databricks Jobs (YAML)
- Photon Engine
- Serverless DLT Pipelines

---

## 📂 Repository Structure
databricks-end-to-end-project/
│
├── notebooks/
│ ├── bronze/
│ ├── silver/
│ ├── gold/
│
├── pipelines/
│ ├── jobs_e2e_ingestion.yml
│ └── dlt_gold_products.yml
│
├── docs/
│ └── architecture.png
│
├── sample_data/
├── README.md
└── .gitignore


---

## ▶️ How to Run the Project

1. Upload source parquet files to **Azure Data Lake Storage**
2. Update dataset metadata in `bronze_parameters`
3. Deploy the Databricks Job using `jobs_e2e_ingestion.yml`
4. Deploy the DLT pipeline using `dlt_gold_products.yml`
5. Trigger the **e2e-ingestion-pipeline** job

---

## 📈 Key Design Patterns Demonstrated

- Medallion Architecture (Bronze–Silver–Gold)
- Incremental ingestion using Autoloader
- Metadata-driven pipelines
- Domain-oriented Silver layer
- Hybrid batch + streaming design
- SCD Type 1 and SCD Type 2 modeling
- Stateful transformations using Delta Live Tables
- Enterprise-style orchestration with YAML

---

## 🔮 Future Enhancements

- Add DLT data quality expectations
- CI/CD integration using Databricks CLI
- Monitoring and alerting
- Table optimization and retention automation

---

## 👤 Author

Built as a hands-on data engineering project to demonstrate **enterprise-grade Azure Databricks architecture and design patterns**.

---

## 📜 License

MIT