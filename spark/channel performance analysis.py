# Databricks notebook source


# COMMAND ----------

# Average revenue per Source
revenue_per_customer_df = (
    revenue_df.groupBy("Source")
    .agg(round(avg("Total"), 2).alias("Average Revenue"))
    .orderBy(desc("Average Revenue"))
)
display(revenue_per_customer_df)

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

# Sales by source and subsource

# COMMAND ----------

# Conversion rates by channel

# COMMAND ----------

# Average order value by channel
