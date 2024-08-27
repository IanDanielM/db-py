-- Databricks notebook source
-- Top 10 products by sales quantity.(SKUS)
SELECT
  OrderItemSKU,
  SUM(OrderItemQuantity) AS total_quantity_sold
FROM
  order_data
GROUP BY
  OrderItemSKU
ORDER BY
  total_quantity_sold DESC
LIMIT
  10;

-- COMMAND ----------

-- Product sales by category.
SELECT
  ItemCategory,
  SUM(OrderItemQuantity) AS total_quantity_sold,
  ROUND(SUM(OrderItemCostIncTax * OrderItemQuantity), 2) AS total_sales
FROM
  order_data
GROUP BY
  ItemCategory
ORDER BY
  total_sales DESC;

-- COMMAND ----------

-- Average selling price per product category.
SELECT
  ItemCategory,
  ROUND(AVG(OrderItemCostIncTax), 2) AS avg_selling_price
FROM
  order_data
GROUP BY
  ItemCategory
ORDER BY
  avg_selling_price DESC;
