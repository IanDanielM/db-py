# Databricks notebook source
from pyspark.sql.functions import *
shipping_df = spark.read.table("order_data")

# COMMAND ----------

# Average shipping costs
avg_shipping_costs_df = (shipping_df
                         .groupBy("PostalService")
                         .agg(round(avg("PostageCosts"),2).alias("avg_shipping_costs"))
                         .orderBy(desc("avg_shipping_costs"))
).display()

# COMMAND ----------

# Most used postal services
shipping_services_df = shipping_df.groupBy("PostalService").count().orderBy(desc("count")).display()

# COMMAND ----------


