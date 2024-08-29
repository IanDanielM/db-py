# Databricks notebook source
orders_24 = "/FileStore/tables/orders2024.csv"
orders_23 = "/FileStore/tables/orders2023.csv"
order_resend = "/FileStore/tables/order_item_resend.csv"
order_returns = "/FileStore/tables/order_item_returns.csv"
order_exchanges = "/FileStore/tables/order_item_exchange.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
orders_24_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load(orders_24)
)

orders_23_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load(orders_23)
)

order_item_return_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load(order_returns)
)

order_item_resend_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load(order_resend)
)

order_item_exchanges_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load(order_exchanges)
)

# COMMAND ----------

merged_orders_df = orders_24_df.union(orders_23_df)
display(merged_orders_df)

# COMMAND ----------

from pyspark.sql.functions import col

cleaned_columns = [col(column_name).alias(column_name.strip().replace(' ', '_').replace('\n', '').replace('\t', '')) 
                   for column_name in orders_23_df.columns]

merged_orders_df = merged_orders_df.select(*cleaned_columns)
display(merged_orders_df)

# COMMAND ----------

display(order_item_resend_df)

# COMMAND ----------

# Separate order item resends from order returns
order_item_return_df = (order_item_return_df.join(
    order_item_resend_df.select("OriginalOrderId"), order_item_return_df.nOrderId == order_item_resend_df.OriginalOrderId,
    how="leftanti"))

display(order_item_return_df.count())

# COMMAND ----------

# remove returned good from orders
merged_orders_df = (merged_orders_df.join(
    order_item_return_df.select("nOrderId").distinct(),
    on="nOrderId", how="leftanti"))

display(merged_orders_df)


# COMMAND ----------

# factor in additional costs from order item resend
from pyspark.sql.functions import col, sum, when
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Calculate additional costs from order item resend
additional_costs = order_item_resend_df.groupBy("OriginalOrderId").agg(
    sum("AdditionalCost").alias("TotalAdditionalCost")
)
# Convert additional_costs to a dictionary for efficient lookup
additional_costs_dict = dict(additional_costs.collect())

@udf(returnType=DoubleType())
def get_additional_cost(order_id):
    return additional_costs_dict.get(order_id, 0.0)

# Update the merged_orders_df
merged_orders_df = merged_orders_df.withColumn(
    "TotalAdditionalCost", 
    get_additional_cost(col("nOrderId"))
)

merged_orders_df = merged_orders_df.withColumn(
    "Total", 
    col("Total") + col("TotalAdditionalCost")
).withColumn(
    "OrderItemCostIncTax", 
    col("OrderItemCostIncTax") + col("TotalAdditionalCost")
)

display(merged_orders_df)


# COMMAND ----------

# remove unpaid orders and orders which have Redacted data
merged_orders_df = merged_orders_df.filter((col("status") == "PAID") & (col("cEmailAddress") != "Redacted"))
merged_orders_df.count()

# COMMAND ----------

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import DoubleType

# Convert currency to GBP
CURRENCY_RATES = {
    "USD": 1.27,
    "EUR": 1.15,
    "SEK": 13.58,
    "MXN": 21.05,
    "AUD": 1.90,
    "PLN": 4.98,
    "CAD": 1.78,
    "AED": 4.85,
    "NZD": 2.10
}

# currency_converter 
@udf(returnType=DoubleType())
def currency_converter(currency, amount):
    if amount is None or currency is None:
        return None
    rate = CURRENCY_RATES.get(currency.upper(), 1)  # Defaults to 1 if currency not found
    return float(amount) / rate

columns_to_convert = ["PostageCosts", "Subtotal", "PostageCostExTax", "Total", "Tax", "OrderItemCostExTax", "OrderItemCostIncTax", "OrderItemSalesTax"]

# Apply the conversion
for column in columns_to_convert:
    merged_orders_df = merged_orders_df.withColumn(column, currency_converter(col("Currency"), col(column)))

# Update the Currency column to GBP
merged_orders_df = merged_orders_df.withColumn("Currency", lit("GBP"))
display(merged_orders_df)

# COMMAND ----------

permanent_table_name = "order_data"
merged_orders_df.write.format("delta").mode("overwrite").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_data;
