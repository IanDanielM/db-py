-- Databricks notebook source
-- monthly Revenue Trend
SELECT
  YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS year,
  MONTH(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS month,
  ROUND(SUM(Total), 2) AS revenue
FROM
  order_data
GROUP BY
  year,
  month
ORDER BY
  year,
  revenue DESC;

-- COMMAND ----------

-- Average order value (AOV) per month
WITH monthly_totals AS (
  SELECT
    YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS year,
    MONTH(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS month,
    SUM(Total) AS total_revenue,
    COUNT(*) AS total_orders
  FROM
    order_data
  GROUP BY
    year,
    month
)
SELECT
  year,
  month,
  ROUND(total_revenue / total_orders, 2) AS average_order_value
FROM
  monthly_totals
order by
  year,
  month;

-- COMMAND ----------

-- Revenue by country
SELECT
  Country,
  ROUND(SUM(Total), 2) as revenue
FROM
  order_data
GROUP BY
  Country
ORDER BY
  revenue DESC

-- COMMAND ----------

-- Revenue by product Brand
SELECT
  ItemCategory,
  ROUND(SUM(Total), 2) as revenue
FROM
  order_data
GROUP BY
  ItemCategory
ORDER BY
  revenue DESC

-- COMMAND ----------

-- Revenue distribution by payment method.
SELECT
  Payment_Method,
  ROUND(SUM(Total), 2) as revenue
FROM
  order_data
GROUP BY
  Payment_Method
ORDER BY
  revenue DESC

-- COMMAND ----------

SELECT
  *
FROM
  order_data
WHERE
  Total > (
    SELECT
      AVG(Total) + 3 * STDDEV(Total)
    FROM
      order_data
  )
  OR Total < (
    SELECT
      AVG(Total) - 3 * STDDEV(Total)
    FROM
      order_data
  )

-- COMMAND ----------

-- Profit margin analysis by product category.(spark)

-- COMMAND ----------

-- Lifetime value (LTV) analysis of customers.(spark)
