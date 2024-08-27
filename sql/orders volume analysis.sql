-- Databricks notebook source
-- monthly Order data
SELECT
  MONTH(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS month,
  COUNT(*) AS total_orders
FROM
  order_data
GROUP BY
  month
ORDER BY
  month

-- COMMAND ----------

-- Daily average number of orders.
WITH daily_orders AS (
  SELECT
    DATE(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS date,
    COUNT(*) AS total_orders
  FROM
    order_data
  GROUP BY
    DATE(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss'))
)
SELECT
  AVG(total_orders) AS avg_orders
FROM
  daily_orders;

-- COMMAND ----------

-- Order volume by country.
SELECT
  Country,
  COUNT (*) AS total_orders
FROM
  order_data
GROUP BY
  Country
ORDER BY
  total_orders DESC

-- COMMAND ----------

-- Order volume by Source
SELECT
  Source,
  COUNT (*) AS total_orders
FROM
  order_data
GROUP BY
  Source
ORDER BY
  total_orders DESC

-- COMMAND ----------

-- order volume by source and subsource
SELECT
  Source,
  SubSource,
  count(*) AS total_orders
FROM
  order_data
GROUP BY
  Source,
  SubSource
ORDER BY
  total_orders DESC

-- COMMAND ----------

-- Trend Analysis over different months
SELECT
  YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS year,
  MONTH(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS month,
  COUNT(*) AS order_count
FROM
  order_data
GROUP BY
  YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')),
  MONTH(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss'))
ORDER BY
  year,
  month;

-- COMMAND ----------

-- Trend Analysis over different years
SELECT
  YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS year,
  COUNT(*) AS order_count
FROM
  order_data
GROUP BY
  YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss'))
ORDER BY
  year
