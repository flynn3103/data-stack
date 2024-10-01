WITH dim_delivery_method__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, dim_delivery_method__rename AS (
  SELECT 
    delivery_method_id AS delivery_method_key
    , delivery_method_name
  FROM dim_delivery_method__source
 )

, dim_delivery_method__cast_type AS (
  SELECT 
    CAST(delivery_method_key AS INTEGER) AS delivery_method_key
    , CAST(delivery_method_name AS STRING) AS delivery_method_name
  FROM dim_delivery_method__rename
 )

, dim_delivery_method__add_undefined_record AS (
   SELECT
    *
   FROM dim_delivery_method__cast_type

   UNION ALL

   SELECT
   0 AS delivery_method_key,
   'Undefined' AS delivery_method_name

   UNION ALL

   SELECT
   -1 AS delivery_method_key,
   'Error' AS delivery_method_name
 )

SELECT 
  delivery_method_key
  , delivery_method_name
FROM dim_delivery_method__add_undefined_record