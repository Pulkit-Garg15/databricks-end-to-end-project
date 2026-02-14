# Azure Databricks End-to-End Data Engineering Project

## Overview
This project demonstrates an end-to-end data engineering pipeline built on **Azure Databricks**, following the **Medallion Architecture (Bronze, Silver, Gold)**.

The pipeline ingests data from Azure Data Lake Storage (ADLS) using **Databricks Autoloader**, performs domain-based transformations in the Silver layer, and builds analytics-ready Gold tables using **Delta Live Tables (DLT)** and PySpark.

---

## Architecture
- **Bronze**: Incremental ingestion using Databricks Autoloader
- **Silver**: Cleaned and curated domain tables (customers, orders, products, regions)
- **Gold**:
  - SCD Type 1 dimensions using Delta Lake
  - SCD Type 2 dimensions using Delta Live Tables (DLT)
  - Fact tables for analytics

---

## Technologies Used
- Azure Databricks
- Delta Lake
- Delta Live Tables (DLT)
- Databricks Autoloader
- PySpark
- Azure Data Lake Storage (ADLS)

---

## Pipeline Orchestration
- End-to-end orchestration using **Databricks Jobs (YAML)**
- Dynamic ingestion using metadata-driven `for_each_task`
- DLT pipeline executed as a first-class task

---

## Repository Structure
notebooks/
bronze/
silver/
gold/
pipelines/
docs/
sample_data/


---

## How to Run
1. Configure source data in ADLS
2. Update metadata in `bronze_parameters`
3. Deploy Jobs and DLT pipelines using YAML
4. Trigger the end-to-end Databricks Job

---

## Future Enhancements
- Add data quality checks using DLT expectations
- Add monitoring and alerting
- CI/CD integration using Databricks CLI

---

## License
MIT