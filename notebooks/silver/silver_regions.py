# Databricks notebook source
df_regions = spark.read.table("databricks_ete_catalog.bronze.regions")

# COMMAND ----------

df_regions.show()

# COMMAND ----------

df_regions = df_regions.drop("_rescued_data")

# COMMAND ----------

df_regions.show()

# COMMAND ----------

df_regions.write.format("delta").mode("overwrite").save("abfss://silver@databricksproject.dfs.core.windows.net/regions")

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.silver.regions"):
    spark.sql("DROP TABLE IF EXISTS databricks_ete_catalog.silver.regions")

spark.sql("""
        CREATE TABLE IF NOT EXISTS databricks_ete_catalog.silver.regions
        USING DELTA
        LOCATION "abfss://silver@databricksproject.dfs.core.windows.net/regions"
        """)
print("Table created successfully")

# COMMAND ----------

if spark.catalog.tableExists("databricks_ete_catalog.silver.regions"):
    spark.sql("VACUUM databricks_ete_catalog.silver.regions")