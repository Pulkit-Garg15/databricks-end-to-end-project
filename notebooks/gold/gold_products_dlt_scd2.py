# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create streaming tables

# COMMAND ----------

product_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : 'product_name IS NOT NULL'
}

# COMMAND ----------

@dlt.table(name="products_staging")
@dlt.expect_all_or_drop(product_rules)
def gold_products():
    return spark.readStream.table("databricks_ete_catalog.silver.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create streaming views

# COMMAND ----------

@dlt.view()
def products_view():
  return spark.readStream.table("Live.products_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating final dimension Products table

# COMMAND ----------

dlt.create_streaming_table("products")

# COMMAND ----------

dlt.apply_changes(
    target = "products",
    source = "products_view",
    keys = ["product_id"],
    sequence_by = "product_id",
    stored_as_scd_type = 2
)