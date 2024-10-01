WITH fact_purchase_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
  )

, fact_purchase_order__rename_column AS (
    SELECT 
      purchase_order_id AS purchase_order_key
      , supplier_id AS supplier_key
      , delivery_method_id AS delivery_method_key
      , contact_person_id AS contact_person_key
      , order_date
      , expected_delivery_date
      , is_order_finalized
    FROM fact_purchase_order__source
)

, fact_purchase_order__cast_type AS (
    SELECT
      CAST(purchase_order_key AS INTEGER) AS purchase_order_key
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
      , CAST(contact_person_key AS INTEGER) AS contact_person_key
      , CAST(order_date AS DATE) AS order_date
      , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
      , CAST(is_order_finalized AS BOOLEAN) AS is_order_finalized_boolean

    FROM fact_purchase_order__rename_column
)

, fact_purchase_order__convert_boolean AS (
    SELECT *
    , CASE 
        WHEN is_order_finalized_boolean IS TRUE THEN 'Order Finalized'
        WHEN is_order_finalized_boolean IS FALSE THEN 'Not Order Finalized'
        ELSE 'Undefined'
      END AS is_order_finalized
    FROM fact_purchase_order__cast_type
)

SELECT 
  purchase_order_key
  , COALESCE(supplier_key, 0) AS supplier_key
  , COALESCE(delivery_method_key, 0) AS delivery_method_key
  , COALESCE(contact_person_key, 0) AS contact_person_key
  , order_date
  , expected_delivery_date
  , is_order_finalized
FROM fact_purchase_order__convert_boolean