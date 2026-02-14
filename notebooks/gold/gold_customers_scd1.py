# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_customers = spark.read.table("databricks_ete_catalog.silver.customers")

# COMMAND ----------

df_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing duplicates

# COMMAND ----------

df_customers = df_customers.dropDuplicates(["customer_id"])
print("Duplicates dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segregating old vs new records

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.gold.customers"):
  df_old = spark.sql("select dim_customer_id, customer_id, create_date, update_date from databricks_ete_catalog.gold.customers")
else:
  df_old = spark.sql("select 0 dim_customer_id, customer_id, 0 create_date, 0 update_date from databricks_ete_catalog.silver.customers where 1=0")

# COMMAND ----------

df_old = df_old.withColumnRenamed("customer_id", "old_customer_id")\
                .withColumnRenamed("create_date", "old_create_date")\
                .withColumnRenamed("update_date", "old_update_date")\
                .withColumnRenamed("dim_customer_id", "old_dim_customer_id")

# COMMAND ----------

df_join = df_customers.join(df_old, df_customers["customer_id"] == df_old["old_customer_id"], "left")

# COMMAND ----------

df_join.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering old & new records

# COMMAND ----------

df_old_records = df_join.filter(df_join["old_dim_customer_id"].isNotNull())

# COMMAND ----------

df_new_records = df_join.filter(df_join["old_dim_customer_id"].isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing old records dataframe

# COMMAND ----------

# Removing unwanted columns
df_old_records = df_old_records.drop("old_customer_id", "old_update_date")

# Renaming the 'old_dim_customer_id' to 'dim_customer_id'
df_old_records = df_old_records.withColumnRenamed("old_dim_customer_id", "dim_customer_id")

# Renaming the 'old_create_date' to 'create_date'
df_old_records = df_old_records.withColumnRenamed("old_create_date", "create_date")
df_old_records = df_old_records.withColumn("create_date", to_timestamp("create_date"))

# Recreating the update_date column
df_old_records = df_old_records.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing new records dataframe

# COMMAND ----------

# Deleting the unwanted columns
df_new_records = df_new_records.drop("old_create_date", "old_customer_id", "old_dim_customer_id", "old_update_date")

# Recreating the 'create_date' and 'update_date' columns
df_new_records = df_new_records.withColumn("create_date", current_timestamp())\
                                .withColumn("update_date", current_timestamp())

# COMMAND ----------

df_new_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Surrogate keys

# COMMAND ----------

df_new_records = df_new_records.withColumn("dim_customer_id", monotonically_increasing_id() + lit(1))

# COMMAND ----------

df_new_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logic for fetching the max surrogate key

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.gold.customers"):
    max_surrogate_key = spark.sql("SELECT MAX(dim_customer_id) as max_surrogate_key from databricks_ete_catalog.gold.customers").collect()[0]['max_surrogate_key']
else:
    max_surrogate_key = 0

# COMMAND ----------

df_new_records = df_new_records.withColumn("dim_customer_id", col("dim_customer_id") + lit(max_surrogate_key))

# COMMAND ----------

df_new_records.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding the old and new records

# COMMAND ----------

df_final = df_old_records.unionByName(df_new_records)
display(df_final)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.gold.customers"):
    delta_obj = DeltaTable.forPath(spark, "abfss://gold@databricksproject.dfs.core.windows.net/customers")
    delta_obj.alias('trg_tbl').merge(df_final.alias("src_tbl"), "trg_tbl.dim_customer_id = src_tbl.dim_customer_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    spark.sql("OPTIMIZE databricks_ete_catalog.gold.customers")
    spark.sql("VACUUM databricks_ete_catalog.gold.customers")

else:
    df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path", "abfss://gold@databricksproject.dfs.core.windows.net/customers")\
    .saveAsTable("databricks_ete_catalog.gold.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_ete_catalog.gold.customers