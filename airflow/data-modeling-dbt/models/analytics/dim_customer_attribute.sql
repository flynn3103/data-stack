WITH dim_customer_attribute__gross_amount AS (
  SELECT 
    customer_key
    , SUM(gross_amount) AS lifetime_sales_amount
    , COUNT (DISTINCT sales_order_key) AS life_time_frequency
    , SUM (CASE WHEN order_date 
          BETWEEN (DATE_TRUNC('2016-05-01', MONTH) - INTERVAL 12 MONTH) 
          AND '2016-05-31'
          THEN gross_amount
          END
          ) AS l12m_sales_amount
    , COUNT (DISTINCT CASE WHEN order_date 
          BETWEEN (DATE_TRUNC('2016-05-01', MONTH) - INTERVAL 12 MONTH) 
          AND '2016-05-31'
          THEN sales_order_key
          END
          ) AS l12m_frequency
    , DATE_TRUNC(MIN(order_date), MONTH) AS start_month
    , DATE_TRUNC(MAX(order_date), MONTH) AS end_month

  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1
)
, dim_customer_attribute__percentile AS (
  SELECT 
    *
    , PERCENT_RANK() OVER (ORDER BY lifetime_sales_amount) AS lifetime_sales_amount_percent_rank
    , PERCENT_RANK() OVER (ORDER BY life_time_frequency) AS lifetime_frequency_percent_rank
    , PERCENT_RANK() OVER (ORDER BY l12m_sales_amount) AS l12m_sales_amount_percent_rank
    , PERCENT_RANK() OVER (ORDER BY l12m_frequency) AS l12m_frequency_percent_rank

  FROM dim_customer_attribute__gross_amount
)
  SELECT 
    *
    , CASE 
        WHEN lifetime_sales_amount_percent_rank <0.5 THEN 'Low'
        WHEN lifetime_sales_amount_percent_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN lifetime_sales_amount_percent_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS lifetime_monetary_segment
    , CASE 
        WHEN lifetime_frequency_percent_rank <0.5 THEN 'Low'
        WHEN lifetime_frequency_percent_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN lifetime_frequency_percent_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS lifetime_frequency_segment
    , CASE 
        WHEN l12m_sales_amount_percent_rank <0.5 THEN 'Low'
        WHEN l12m_sales_amount_percent_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN l12m_sales_amount_percent_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS l12m_monetary_segment
    , CASE 
        WHEN l12m_frequency_percent_rank <0.5 THEN 'Low'
        WHEN l12m_frequency_percent_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN l12m_frequency_percent_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS l12m_frequency_segment

  FROM dim_customer_attribute__percentile