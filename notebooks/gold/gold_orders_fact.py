# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data reading from silver

# COMMAND ----------

df_orders = spark.read.table("databricks_ete_catalog.silver.orders")

# COMMAND ----------

df_orders.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulling joining columns from Dimension tables- Customers & Products

# COMMAND ----------

# Data from Customers Dimension table
df_customers = spark.read.table("databricks_ete_catalog.gold.customers").select("customer_id", "dim_customer_id")

# COMMAND ----------

# Data from Products Dimension table
df_products = spark.read.table("databricks_ete_catalog.gold.products").select("product_id", col("product_id").alias("dim_product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining fact Orders table with the two dimension tables

# COMMAND ----------

df_final = df_orders.join(df_customers, df_orders["customer_id"] == df_customers["customer_id"], how="left").join(df_products, df_orders["product_id"] == df_products["product_id"], how="left")

# COMMAND ----------

df_final = df_final.drop("customer_id", "product_id")

# COMMAND ----------

df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upserting the final fact Orders table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.gold.orders"):
  window_spec = Window.partitionBy("order_id", "dim_customer_id", "dim_product_id").orderBy(col("order_date").desc())
  df_final = df_final.withColumn("rn", row_number().over(window_spec)).filter("rn == 1").drop("rn")
  dlt = DeltaTable.forName(spark, "databricks_ete_catalog.gold.orders")
  dlt.alias("trg").merge(df_final.alias("src"), "trg.order_id == src.order_id AND trg.dim_customer_id == src.dim_customer_id AND trg.dim_product_id == src.dim_product_id")\
    .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
        .execute()
  spark.sql("VACUUM databricks_ete_catalog.gold.orders")
  spark.sql("OPTIMIZE databricks_ete_catalog.gold.orders")

else:
  df_final.write.format("delta").mode("overwrite")\
                .option("path", "abfss://gold@databricksproject.dfs.core.windows.net/orders")\
                .saveAsTable("databricks_ete_catalog.gold.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_ete_catalog.gold.orders