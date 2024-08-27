# Databricks notebook source
# read dataframe from table
from pyspark.sql.functions import *
orders_df = spark.read.table("order_data")

# COMMAND ----------

# monthly Order data
monthly_order_df = (
    orders_df.withColumn(
        "Month", month(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss"))
    )
    .groupBy("Month")
    .count()
    .withColumnRenamed("count", "Total Orders")
    .orderBy("Month")
)
display(monthly_order_df)

# COMMAND ----------

# Daily average number of orders.
avg_orders_df = (orders_df
                 .withColumn("Date", to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss"))
                 .groupBy("Date")
                 .count()
                 .withColumnRenamed("count", "Total Orders")
)
avg_orders = avg_orders_df.agg(avg("Total Orders").alias("Average Orders Per Day"))
display(avg_orders)

# COMMAND ----------

# -- Order volume by country.
country_orders_df = (orders_df
                     .groupBy("Country")
                     .count()
                     .withColumnRenamed("count", "Total Orders")
                     .orderBy(desc("Total Orders")))
display(country_orders_df)

# COMMAND ----------

# -- Order volume by Source
source_orders_df = (orders_df
                     .groupBy("Source")
                     .count()
                     .withColumnRenamed("count", "Total Orders")
                     .orderBy(desc("Total Orders")))
display(source_orders_df)

# COMMAND ----------

# order volume by source and subsource
source_subsource_orders_df = (orders_df
                              .groupBy("Source", "SubSource")
                              .count()
                              .withColumnRenamed("count", "Total Orders")
                              .orderBy(desc("Total Orders"))
)
display(source_subsource_orders_df)

# COMMAND ----------

TA_monthly_df = (orders_df
                 .withColumn("Year", year(to_date('dReceievedDate', 'dd/MM/yyyy HH:mm:ss')))
                 .alias("Year")
                 .withColumn("Month", month(to_date('dReceievedDate', 'dd/MM/yyyy HH:mm:ss')))
                 .alias("Month")
                 .groupBy("Year", "Month")
                 .count()
                 .withColumnRenamed("count", "Order Count")
                 .orderBy("Year", "Month")
)
display(TA_monthly_df)

# COMMAND ----------

TA_yearly_df = (
    orders_df.withColumn("Year", year(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss")))
    .alias("Year")
    .groupBy("Year")
    .count()
    .withColumnRenamed("count", "Order Count")
    .orderBy("Year")
)
display(TA_yearly_df)
