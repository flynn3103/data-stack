WITH fact_customer_snapshot__source AS (
  SELECT 
    customer_key
    , DATE_TRUNC(order_date, MONTH) AS year_month
    , SUM(gross_amount) AS sales_amount
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1, 2
)
 
, dim_year_month AS (
  SELECT DISTINCT year_month
  FROM {{ref('dim_date')}}
)

, fact_customer_snapshot__densed AS (
  SELECT DISTINCT customer_key
  , dim_year_month.year_month
  FROM fact_customer_snapshot__source
  CROSS JOIN dim_year_month
)

, fact_customer_snapshot__calculation  AS (
  SELECT 
    customer_key
    , year_month
    , COALESCE(fact_customer_snapshot__source.sales_amount, 0) AS sales_amount
    , SUM(COALESCE(fact_customer_snapshot__source.sales_amount, 0)) OVER (
      PARTITION BY customer_key 
      ORDER BY year_month) 
      AS life_time_sales_amount
    , LAG(fact_customer_snapshot__source.sales_amount, 1) OVER (
      PARTITION BY customer_key 
      ORDER BY year_month) 
      AS last_month_sales_amount
    , SUM(fact_customer_snapshot__source.sales_amount) OVER (
      PARTITION BY customer_key 
      ORDER BY year_month 
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) 
      AS l12m_sales_amount
  FROM fact_customer_snapshot__densed 
  LEFT JOIN fact_customer_snapshot__source
    USING (customer_key, year_month)
)

, fact_customer_snapshot__cleanse  AS (
  SELECT 
    fact_customer_snapshot__calculation.*

  FROM fact_customer_snapshot__calculation 
  JOIN {{ref('dim_customer_attribute')}}
    ON dim_customer_attribute.customer_key = fact_customer_snapshot__calculation.customer_key
    AND year_month BETWEEN start_month AND end_month
)

, fact_customer_snapshot__percentile AS (
  SELECT *
  , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY sales_amount) AS sales_amount_percentile_rank
  , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY life_time_sales_amount) AS life_time_sales_amount_percentile_rank  

  FROM fact_customer_snapshot__cleanse
)

, fact_customer_snapshot__segmentation AS (
  SELECT *
    , CASE 
        WHEN sales_amount_percentile_rank <0.5 THEN 'Low'
        WHEN sales_amount_percentile_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN sales_amount_percentile_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS sales_amount_segment
    , CASE 
        WHEN life_time_sales_amount_percentile_rank <0.5 THEN 'Low'
        WHEN life_time_sales_amount_percentile_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN life_time_sales_amount_percentile_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS life_time_sales_amount_segment

  FROM fact_customer_snapshot__percentile
)

SELECT
  customer_key
  , year_month
  , sales_amount 
  , last_month_sales_amount
  , l12m_sales_amount
  , life_time_sales_amount
  , sales_amount_percentile_rank
  , life_time_sales_amount_percentile_rank
  , sales_amount_segment
  , life_time_sales_amount_segment
FROM fact_customer_snapshot__segmentation 
