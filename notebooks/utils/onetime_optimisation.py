# Databricks notebook source
catalog_name = dbutils.widgets.get("catalog")
table_name = dbutils.widgets.get("table_name")
clustering_key = dbutils.widgets.get("clustering_key")

# COMMAND ----------

class Optimisation:
    def optimise_table(self, catalog_name, table_name, clustering_key):
        spark.sql(f"ALTER TABLE databricks_ete_catalog.{catalog_name}.{table_name} CLUSTER BY ({clustering_key})") 

# COMMAND ----------

if spark.catalog.tableExists(f"databricks_ete_catalog.{catalog_name}.{table_name}"):
    optimise = Optimisation()
    optimise.optimise_table(catalog_name, table_name, clustering_key)
    print(f"{table_name} table clustered by {clustering_key} column")