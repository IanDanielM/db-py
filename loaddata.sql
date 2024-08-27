-- Databricks notebook source
-- MAGIC %python
-- MAGIC orders_24 = "/FileStore/tables/orders2024.csv"
-- MAGIC orders_23 = "/FileStore/tables/orders2023.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC # CSV options
-- MAGIC infer_schema = "True"
-- MAGIC first_row_is_header = "True"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC orders_24_df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(orders_24)
-- MAGIC f
-- MAGIC orders_23_df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(orders_23)
-- MAGIC
-- MAGIC orders_23_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC cleaned_columns = [col(column_name).alias(column_name.strip().replace(' ', '_').replace('\n', '').replace('\t', '')) 
-- MAGIC                    for column_name in orders_23_df.columns]
-- MAGIC
-- MAGIC orders_23_df = orders_23_df.select(*cleaned_columns)
-- MAGIC orders_24_df = orders_24_df.select(*cleaned_columns)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC permanent_table_name = "order_data"
-- MAGIC orders_23_df.write.format("delta").mode("append").saveAsTable(permanent_table_name)
-- MAGIC orders_24_df.write.format("delta").mode("append").saveAsTable(permanent_table_name)

-- COMMAND ----------

select * from order_data;

-- COMMAND ----------

DROP TABLE IF EXISTS order_data;
