# Databricks notebook source
from pyspark.sql.functions import *
customers_df = spark.read.table("order_data")


# COMMAND ----------

# Number of unique customers per month.
unique_customers_df = (
    customers_df.withColumn(
        "Year", year(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss"))
    )
    .withColumn("Month", month(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss")))
    .groupBy("Year", "Month")
    .agg(countDistinct("cEmailAddress").alias("unique_customers"))
)
display(unique_customers_df)

# COMMAND ----------

# Average items per order

# COMMAND ----------

# Most common product combinations in orders

# COMMAND ----------

# Distribution of customers by country

# COMMAND ----------


