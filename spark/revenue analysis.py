# Databricks notebook source
from pyspark.sql.functions import *
revenue_df = spark.read.table("order_data")

# COMMAND ----------

# monthly Revenue Trend
monthly_revenue_df = (revenue_df
                     .withColumn("Year", year(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss")))
                     .withColumn("Month", month(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss")))
                     .groupBy("Year", "Month")
                     .agg(round(sum("Total"), 2).alias("Revenue"))
                     .orderBy("Year", desc("Month"))
)
display(monthly_revenue_df)

# COMMAND ----------

# -- Average order value (AOV) per month
aov_df = (revenue_df
          .withColumn("Year", year(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss")))
          .withColumn("Month", month(to_date("dReceievedDate", "dd/MM/yyyy HH:mm:ss")))
          .groupBy("Year", "Month")
          .agg(
              round(sum("Total"), 2).alias("Total Revenue"),
              count("*").alias("Total Orders")
              )
          .orderBy("Year", "Month")
)
aov_df = aov_df.withColumn("AOV", round(col("Total Revenue")/col("Total Orders"),2))
display(aov_df)

# COMMAND ----------

# Revenue by country
country_revenue_df = (revenue_df
                      .groupBy("Country")
                      .agg(round(sum("Total"),2).alias("Revenue"))
                      .orderBy(desc("Revenue"))
)
display(country_revenue_df)

# COMMAND ----------

# Revenue by product Brand
brand_revenue_df = (revenue_df
                    .groupBy("ItemCategory")
                    .agg(round(sum("Total"),2).alias("Revenue"))
                    .orderBy(desc("Revenue"))
)
display(brand_revenue_df)

# COMMAND ----------

# Revenue breakdown by currency
revenue_per_currency_df = (
    revenue_df.groupBy("Currency")
    .agg(round(sum("Total"), 2).alias("Total Revenue"))
    .orderBy(desc("Total Revenue"))
)
display(revenue_per_currency_df)

# COMMAND ----------


