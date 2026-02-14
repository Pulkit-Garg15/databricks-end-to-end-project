# Databricks notebook source
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_read = spark.read.parquet(f"abfss://source@databricksproject.dfs.core.windows.net/{file_name}/")
df_read.show()

# COMMAND ----------

df_autolod = spark.readStream.format("cloudFiles")\
                            .option("cloudFiles.format", "parquet")\
                            .option("cloudFiles.SchemaLocation", f"abfss://bronze@databricksproject.dfs.core.windows.net/checkpoint_{file_name}")\
                            .load(f"abfss://source@databricksproject.dfs.core.windows.net/{file_name}")

df_autolod = df_autolod.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_autolod.writeStream.format("parquet")\
                        .outputMode("append")\
                        .option("checkpointLocation",f"abfss://bronze@databricksproject.dfs.core.windows.net/checkpoint_{file_name}")\
                        .option("path", f"abfss://bronze@databricksproject.dfs.core.windows.net/{file_name}")\
                        .trigger(once=True)\
                        .start()\
                        .awaitTermination()

# COMMAND ----------

df_read_bronze = spark.read.parquet(f"abfss://bronze@databricksproject.dfs.core.windows.net/{file_name}")
df_read_bronze.show()