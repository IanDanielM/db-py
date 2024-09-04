# Databricks notebook source
from pyspark.sql.functions import *
channel_df = spark.read.table("order_data")

# COMMAND ----------

# Average revenue per Source
revenue_per_source_df = (
    channel_df.groupBy("Source")
    .agg(round(avg("Total"), 2).alias("Average Revenue"))
    .orderBy(desc("Average Revenue"))
)
display(revenue_per_source_df)

# COMMAND ----------

# order volume by source and subsource
source_subsource_orders_df = (channel_df
                              .groupBy("Source", "SubSource")
                              .count()
                              .withColumnRenamed("count", "Total Orders")
                              .orderBy(desc("Total Orders"))
)
display(source_subsource_orders_df)

# COMMAND ----------

# Average order value by channel
aov_df = (channel_df
          .groupBy("Source")
          .agg(
              round(sum("Total"), 2).alias("Total Revenue"),
              count("*").alias("Total Orders")
              )
          .orderBy(desc("Total Orders"))
)
aov_df = aov_df.withColumn("AOV", round(col("Total Revenue")/col("Total Orders"),2)).display()

# COMMAND ----------

# Sales by source
from pyspark.sql import Window
window_spec = Window.partitionBy()
sales_df = (channel_df
            .groupBy("Source")
            .agg(round(sum("Total"),2).alias("Total Revenue"),
                  count("*").alias("Total Orders"),
                  round(avg("Total"), 2).alias("AOV"))
            .withColumn("PercentageOfTotalRevenue",
                        round(col("Total Revenue")/sum(col("Total Revenue")).over(window_spec) *100, 2 ))
            .orderBy(desc("Total Revenue"))
).display()
