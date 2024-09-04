# Databricks notebook source
from pyspark.sql.functions import *
payments_df = spark.read.table("order_data");

# COMMAND ----------

# Revenue distribution by payment method.
payment_revenue_df = (payments_df
                    .groupBy("Payment_Method")
                    .agg(round(sum("Total"),2).alias("Revenue"))
                    .orderBy(desc("Revenue"))
).display()

# COMMAND ----------

# Distribution of payment methods
from pyspark.sql import Window
window_spec = Window.partitionBy()
payment_method_distribution = (payments_df
    .groupBy("Payment_Method")
    .agg(
        count("*").alias("OrderCount"),
        round(sum("Total"),2).alias("TotalRevenue")
    )
    .withColumn("OrderPercentage", round(col("OrderCount") / sum("OrderCount").over(window_spec) * 100, 2)) 
    .withColumn("RevenuePercentage", round(col("TotalRevenue") / sum("TotalRevenue").over(window_spec) * 100, 2))
    .orderBy(desc("OrderCount"))
).display()

# COMMAND ----------

# Average transaction value by payment method
avg_transaction_value_df = (payments_df
                            .groupBy("Payment_Method")
                            .agg(round(avg("Total"),2).alias("Average_Transaction_Value"))
                            .orderBy(desc("Average_Transaction_Value"))
).display()

# COMMAND ----------

# Payment method preferences by country
payment_method_preferences_df = (payments_df
                                 .groupBy("Country","Payment_Method")
                                 .count()
                                 .withColumnRenamed("count","payment count")
                                 .orderBy(desc("payment count"))    
).display()
