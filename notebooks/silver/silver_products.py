# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_products = spark.read.parquet("abfss://bronze@databricksproject.dfs.core.windows.net/products")

# COMMAND ----------

df_products.show()

# COMMAND ----------

df_products = df_products.drop("_rescued_data", "create_date")

# COMMAND ----------

df_products.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Discount Function

# COMMAND ----------

# %sql
# CREATE OR REPLACE FUNCTION databricks_ete_catalog.bronze.discount_func(price DOUBLE)
# RETURNS DOUBLE
# LANGUAGE SQL
# RETURN price * 0.90

# COMMAND ----------

df_products = df_products.withColumn("discounted_price", expr("databricks_ete_catalog.bronze.discount_func(price)"))

# COMMAND ----------

df_products.show()

# COMMAND ----------

df_products.write.format("delta").mode("overwrite").save("abfss://silver@databricksproject.dfs.core.windows.net/products")

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.silver.products"):
    spark.sql("DROP TABLE IF EXISTS databricks_ete_catalog.silver.products")

spark.sql("""
          CREATE TABLE IF NOT EXISTS databricks_ete_catalog.silver.products
          USING DELTA
          LOCATION "abfss://silver@databricksproject.dfs.core.windows.net/products"
          """)
print("Table created successsfully")

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.silver.products"):
    spark.sql("OPTIMIZE databricks_ete_catalog.silver.products ZORDER BY (product_id)")