-- Databricks notebook source
-- Number of unique customers per month.
WITH monthly_totals AS (
  SELECT
    YEAR(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS year,
    MONTH(TO_DATE(dReceievedDate, 'dd/MM/yyyy HH:mm:ss')) AS month,
    count(DISTINCT cEmailAddress) as unique_customers
  FROM
    order_data
  GROUP BY
    year,
    month
)
SELECT
  *
FROM
  monthly_totals

-- COMMAND ----------

-- Top 10 customers by revenue.
WITH customer_totals AS (
  SELECT
    cEmailAddress,
    SUM(Total) AS total_revenue
  FROM
    order_data
  GROUP BY
    cEmailAddress
  ORDER BY
    total_revenue DESC
)
SELECT
  cEmailAddress,
  round(total_revenue, 2)
FROM
  customer_totals
LIMIT
  10;

-- COMMAND ----------

--  Average revenue per customer.
WITH customer_totals AS (
  SELECT
    cEmailAddress,
    AVG(Total) AS average_revenue
  FROM
    order_data
  GROUP BY
    cEmailAddress
)
SELECT
  cEmailAddress,
  ROUND(average_revenue, 2)
FROM
  customer_totals
ORDER BY
  average_revenue DESC;

-- COMMAND ----------

-- Repeat customer rate.
