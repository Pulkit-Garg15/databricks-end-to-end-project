# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading from Orders Bronze

# COMMAND ----------

df_orders = spark.read.parquet("abfss://bronze@databricksproject.dfs.core.windows.net/orders")

# COMMAND ----------

df_orders.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping Unwanted Columns from Bronze Orders

# COMMAND ----------

df_orders = df_orders.drop("_rescued_data","create_date")

# COMMAND ----------

df_orders.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying Some Transformations

# COMMAND ----------

# Making the order_date column as timestamp
df_orders = df_orders.withColumn("order_date", to_timestamp(col("order_date")))
df_orders.show()

# COMMAND ----------

# Adding year column of order date
df_orders = df_orders.withColumn("year", year(col("order_date")))
df_orders.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a delta table

# COMMAND ----------

# DBTITLE 1,Untitled
df_orders.write.format("delta").mode("overwrite").save("abfss://silver@databricksproject.dfs.core.windows.net/orders")

# COMMAND ----------

# DBTITLE 1,Untitled
if spark.catalog.tableExists("databricks_ete_catalog.silver.orders"):
    spark.sql("DROP TABLE IF EXISTS databricks_ete_catalog.silver.orders")

# create a new silver table
spark.sql("""
    CREATE TABLE IF NOT EXISTS databricks_ete_catalog.silver.orders
    USING DELTA
    LOCATION "abfss://silver@databricksproject.dfs.core.windows.net/orders"
    """)
print("Table created succcessfully")

# COMMAND ----------

# Delete the unwanted files
if spark.catalog.tableExists("databricks_ete_catalog.silver.orders"):
    spark.sql("VACUUM databricks_ete_catalog.silver.orders")
    spark.sql("OPTIMIZE databricks_ete_catalog.silver.orders")