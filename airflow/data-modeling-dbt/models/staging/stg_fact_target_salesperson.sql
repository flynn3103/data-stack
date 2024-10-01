WITH fact_external_target_salesperson__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_external_target_salesperson__rename_column AS (
  SELECT
    year_month
    , salesperson_person_id AS salesperson_person_key
    , target_revenue AS target_gross_amount

  FROM fact_external_target_salesperson__source
)

, fact_external_target_salesperson__cast_type AS (
  SELECT 
    CAST(year_month AS DATE) AS year_month
    , CAST(salesperson_person_key AS INTEGER) AS salesperson_person_key
    , CAST(target_gross_amount AS FLOAT64) AS target_gross_amount

  FROM fact_external_target_salesperson__rename_column
)

SELECT 
  year_month
  , salesperson_person_key
  , SUM(target_gross_amount) AS target_gross_amount

FROM fact_external_target_salesperson__cast_type
GROUP BY 1, 2