# Databricks notebook source
from pyspark.sql.functions import *
products_df = spark.read.table("order_data")

# COMMAND ----------

# Top 10 products by sales quantity.(SKUS)
top_products_df = (
    products_df.groupBy("OrderItemSKU")
    .agg(sum("OrderItemQuantity").alias("Total Quantity Sold"))
    .orderBy(desc("Total Quantity Sold"))
    .limit(10)
)
display(top_products_df)

# COMMAND ----------

# Product sales by category.
category_sales_df = (
    products_df
    .groupBy("ItemCategory")
    .agg(
        sum("OrderItemQuantity").alias("Total Quantity Sold"),
        round(sum(col("OrderItemCostIncTax") * col("OrderItemQuantity")), 2).alias("Total Sales"),
    )
    .orderBy(desc("Total Quantity Sold"))
)

display(category_sales_df)

# COMMAND ----------

# Average selling price per product category.
avg_price_df = (products_df
                .groupBy("ItemCategory")
                .agg(round(avg("OrderItemCostIncTax"), 2).alias("Average Selling Price"))
                .orderBy(desc("Average Selling Price"))
)
display(avg_price_df)

# COMMAND ----------

# Most profitable products
profitables_df = (products_df
                  .withColumn("Profit Per unit", col("OrderItemCostExTax")-col("PurchasePrice"))
                  .groupBy("OrderItemSKU")
                  .agg(
                      sum(col("Profit Per unit") * col("OrderItemQuantity")).alias("Total Profit"),
                      sum(col("OrderItemQuantity")).alias("Total Quantity Sold"),
                      round(avg(col("OrderItemCostIncTax")),2).alias("Average Selling Price"),
                      round(avg(col("PurchasePrice")), 2).alias("Average Purchase Price")
                      )
                  .withColumn("Profit Margin", round((col("Total Profit") / col("Average Selling Price") * col("Total Quantity Sold")),2))
                  .orderBy(desc("Total Profit"))
)
display(profitables_df)

# COMMAND ----------

# Top-selling products (by quantity and revenue)
top_revenue_products_df = (
    products_df.groupBy("OrderItemSKU")
    .agg(
        sum("OrderItemQuantity").alias("Total Quantity Sold"),
        round(sum("OrderItemCostIncTax"), 2).alias("Total Revenue")
        )
    .orderBy(desc("Total Quantity Sold"), desc("Total Revenue"))
    .limit(10)
)
display(top_revenue_products_df)
