WITH dim_customer_membership__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)

, dim_customer_membership__rename_column AS (
  SELECT
    customer_id
    , membership
    , begin_effective_date
    , end_effective_date

  FROM dim_customer_membership__source
)

, dim_customer_membership__cast_type AS (
  SELECT 
    CAST(customer_id AS INTEGER) AS customer_id
    , CAST(membership AS STRING) AS membership
    , CAST(begin_effective_date AS DATE) AS begin_effective_date
    , CAST(end_effective_date AS DATE) AS end_effective_date

  FROM dim_customer_membership__rename_column
)

SELECT 
  customer_id
  , membership
  , begin_effective_date
  , end_effective_date

FROM dim_customer_membership__cast_type