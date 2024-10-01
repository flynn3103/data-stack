WITH fact_external_category_target__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__category_target`
)

, fact_external_category_target__rename_column AS (
  SELECT
    year AS year_month
    , category_id AS category_key
    , target_revenue

  FROM fact_external_category_target__source
)

, fact_external_category_target__cast_type AS (
  SELECT 
    CAST(year_month AS DATE) AS year_month
    , CAST(category_key AS INTEGER) AS category_key
    , CAST(target_revenue AS NUMERIC) AS target_revenue

  FROM fact_external_category_target__rename_column
)

SELECT 
  year_month
  , category_key
  , SUM(target_revenue) AS target_revenue

FROM fact_external_category_target__cast_type
GROUP BY 1, 2