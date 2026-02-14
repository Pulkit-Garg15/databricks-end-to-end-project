# Databricks notebook source
datasets = [
    {
        "file_name" : "products"
    },
    {
        "file_name" : "customers"
    },
    {
        "file_name" : "orders"
    },
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_dataset", datasets)