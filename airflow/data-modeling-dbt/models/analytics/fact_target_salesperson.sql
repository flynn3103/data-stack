WITH fact_target_salesperson__gross_amount AS (
    SELECT 
      CAST(DATE_TRUNC(order_date, month) AS DATE) AS year_month
      , salesperson_person_key
      , SUM(gross_amount) AS gross_amount

    FROM {{ref('fact_sales_order_line')}}
    GROUP BY 1, 2
)

, fact_target_salesperson__consolidate AS (
    SELECT 
      year_month
      , salesperson_person_key
      , COALESCE(fact_target.target_gross_amount, 0) AS target_gross_amount
      , COALESCE(fact_actual.gross_amount, 0) AS gross_amount

    FROM  fact_target_salesperson__gross_amount AS fact_actual
    FULL OUTER JOIN {{ref('stg_fact_target_salesperson')}} AS fact_target
      USING (year_month, salesperson_person_key)
)

, fact_target_salesperson__calculate AS (
    SELECT *
      , gross_amount/ NULLIF(target_gross_amount, 0) AS ratio_achieve
    FROM fact_target_salesperson__consolidate
)

, fact_target_salesperson__category AS (
    SELECT *
      , CASE 
          WHEN ratio_achieve BETWEEN 0 AND 0.8 THEN 'Not Achieved'
          WHEN ratio_achieve > 0.8 THEN 'Achieved'
          ELSE 'Undefined'
        END AS is_achieved
    FROM fact_target_salesperson__calculate
)

SELECT
  year_month
  , salesperson_person_key
  , target_gross_amount
  , gross_amount
  , ratio_achieve
  , is_achieved
  
FROM fact_target_salesperson__category

