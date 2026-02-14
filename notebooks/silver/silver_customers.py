# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_customer = spark.read.parquet("abfss://bronze@databricksproject.dfs.core.windows.net/customers")

# COMMAND ----------

display(df_customer)

# COMMAND ----------

df_customer = df_customer.drop("_rescued_data", "create_date")

# COMMAND ----------

df_customer = df_customer.withColumn("domain", split(col("email"), "@")[1])

# COMMAND ----------

df_customer.show()

# COMMAND ----------

df_customer = df_customer.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))).drop("first_name", "last_name")

# COMMAND ----------

df_customer.show()

# COMMAND ----------

df_customer.write.format("delta").mode("overwrite").save("abfss://silver@databricksproject.dfs.core.windows.net/customers")

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.silver.customers"):
    spark.sql("DROP TABLE IF EXISTS databricks_ete_catalog.silver.customers")

spark.sql("""
          CREATE TABLE IF NOT EXISTS databricks_ete_catalog.silver.customers
          USING DELTA
          LOCATION "abfss://silver@databricksproject.dfs.core.windows.net/customers"
          """)
print("Table created successfully")

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.silver.customers"):
    spark.sql("VACUUM databricks_ete_catalog.silver.customers")
    spark.sql("OPTIMIZE databricks_ete_catalog.silver.customers")