WITH fact_rfm__summary AS (
  SELECT 
    COALESCE(dim_customer.customer_key, FARM_FINGERPRINT(CONCAT(-1, CAST('2013-01-01' AS DATE)))) AS customer_key
    --, fact_sales_order_line.product_key as product_key
    , MAX(fact_sales_order_line.order_date) AS last_active_date
    , DATE_DIFF('2016-06-1', MAX(fact_sales_order_line.order_date), DAY) AS recency -- Since recency is calculated for a point in time and the dataset last order date is May 31 2015, we will set the 1 day after to calculate recency.
    , COUNT (DISTINCT fact_sales_order_line.sales_order_key) AS frequency
    , SUM(fact_sales_order_line.gross_amount) AS monetary
  FROM {{ref('fact_sales_order_line')}}
  LEFT JOIN {{ref('dim_customer')}} USING (customer_key)
  GROUP BY 1
)

, fact_rfm__percentile AS (
  SELECT
    *
    , PERCENT_RANK() OVER (ORDER BY recency DESC) AS recency_rank --the closer active date, the higher rank for recency
    , PERCENT_RANK() OVER (ORDER BY frequency) AS frequency_rank
    , PERCENT_RANK() OVER (ORDER BY monetary) AS monetary_rank
  FROM fact_rfm__summary
)

, fact_frm__score AS (
  SELECT  *
    , CASE 
      WHEN recency_rank BETWEEN 0 AND 0.2 THEN 1
      WHEN recency_rank BETWEEN 0.2 AND 0.4 THEN 2
      WHEN recency_rank BETWEEN 0.4 AND 0.6 THEN 3
      WHEN recency_rank BETWEEN 0.6 AND 0.8 THEN 4
      WHEN recency_rank > 0.8 THEN 5
      ELSE 0
     END AS recency_score
    , CASE 
      WHEN frequency_rank BETWEEN 0 AND 0.2 THEN 1
      WHEN frequency_rank BETWEEN 0.2 AND 0.4 THEN 2
      WHEN frequency_rank BETWEEN 0.4 AND 0.6 THEN 3
      WHEN frequency_rank BETWEEN 0.6 AND 0.8 THEN 4
      WHEN frequency_rank > 0.8 THEN 5
      ELSE 0
     END AS frequency_score
    , CASE 
      WHEN monetary_rank BETWEEN 0 AND 0.2 THEN 1
      WHEN monetary_rank BETWEEN 0.2 AND 0.4 THEN 2
      WHEN monetary_rank BETWEEN 0.4 AND 0.6 THEN 3
      WHEN monetary_rank BETWEEN 0.6 AND 0.8 THEN 4
      WHEN monetary_rank > 0.8 THEN 5
      ELSE 0
      END AS monetary_score
  FROM fact_rfm__percentile
)

, fact_rfm__segment AS (
  SELECT *
    , CAST(CONCAT(recency_score, frequency_score, monetary_score) AS INT64) AS rfm_segment
  FROM fact_frm__score
)

  SELECT *
    , CASE
      WHEN rfm_segment IN (555, 554, 544, 545, 454, 455, 445) THEN "Champions"
      WHEN rfm_segment IN (543, 444, 435, 355, 354, 345, 344, 335) THEN "Loyal Customers"
      WHEN rfm_segment IN (553, 551, 552, 541, 542, 533, 532, 531, 452, 451, 442, 441, 431, 453, 433, 432, 423, 353, 352, 351, 342, 341, 333, 323) THEN "Potential Loyalist"
      WHEN rfm_segment IN (512, 511, 422, 421, 412, 411, 311) THEN "Recent Customers"
      WHEN rfm_segment IN (525, 524, 523, 522, 521, 515, 514, 513, 425, 424, 413,414, 415, 315, 314, 313) THEN "Promising"
      WHEN rfm_segment IN (535, 534, 443, 434, 343, 334, 325, 324) THEN "Customers Needing Attention"
      WHEN rfm_segment IN (331, 321, 312, 221, 213) THEN "About To Sleep"
      WHEN rfm_segment IN (255, 254, 245, 244, 253, 252, 243, 242, 235, 234, 225, 224, 153, 152, 145, 143, 142, 135, 134, 133, 125, 124) THEN "At Risk"
      WHEN rfm_segment IN (155, 154, 144, 214, 215,115, 114, 113) THEN "Can’t Lose Them"
      WHEN rfm_segment IN (332, 322, 231, 241, 251, 233, 232, 223, 222, 132, 123, 122, 212, 211) THEN "Hibernating"
      WHEN rfm_segment IN (111, 112, 121, 131, 141,151) THEN "Lost"
    ELSE 'Undefined'
    END AS rfm_category
  FROM fact_rfm__segment