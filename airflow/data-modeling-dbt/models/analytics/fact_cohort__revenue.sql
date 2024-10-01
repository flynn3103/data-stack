WITH fact_sales AS (
  SELECT *
  FROM {{ref('fact_sales_order_line')}}
)

, fact_cohort AS ( 
  SELECT 
    customer_key
    , MIN(DATE_TRUNC(order_date, MONTH)) AS cohort_month
  FROM fact_sales
  GROUP BY 1
)

, fact_sales__order_month AS (
  SELECT 
    customer_key
    , DATE_TRUNC(order_date, MONTH) AS order_month
    , SUM(gross_amount) AS revenue
  FROM fact_sales
  GROUP BY 1,2
)

, fact_cohort_size AS (
  SELECT 
    fact_cohort.cohort_month
    , SUM(fact_sales.revenue) AS cohort_size
  FROM fact_sales__order_month AS fact_sales
  LEFT JOIN fact_cohort 
    ON fact_cohort.customer_key = fact_sales.customer_key
    AND fact_cohort.cohort_month = fact_sales.order_month
  GROUP BY 1
)

, fact_cohort_retention AS  (
  SELECT 
    fact_cohort.cohort_month
    -- , fact_sales.order_month
    , DATE_DIFF(fact_sales.order_month, fact_cohort.cohort_month, MONTH)  AS period
    , SUM(fact_sales.revenue) AS revenue
  FROM fact_sales__order_month AS fact_sales
  LEFT JOIN fact_cohort USING (customer_key)
  GROUP BY 1,2
)

SELECT 
  cohort_month
  , fact_retention.period
  , fact_retention.revenue
  , fact_cohort.cohort_size
  , fact_retention.revenue *100/fact_cohort.cohort_size AS percentage

FROM fact_cohort_retention AS fact_retention
LEFT JOIN fact_cohort_size AS fact_cohort USING (cohort_month)
  ORDER BY 1,2