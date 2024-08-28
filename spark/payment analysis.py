# Databricks notebook source


# COMMAND ----------

# Revenue distribution by payment method.
payment_revenue_df = (revenue_df
                    .groupBy("Payment_Method")
                    .agg(round(sum("Total"),2).alias("Revenue"))
                    .orderBy(desc("Revenue"))
)
display(payment_revenue_df)

# COMMAND ----------

# Distribution of payment methods

# COMMAND ----------

# Average transaction value by payment method

# COMMAND ----------

# Payment method preferences by country
