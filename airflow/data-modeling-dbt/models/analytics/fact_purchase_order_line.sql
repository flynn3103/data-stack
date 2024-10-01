WITH fact_purchase_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename_column AS (
    SELECT 
      purchase_order_line_id AS purchase_order_line_key
      , purchase_order_id AS purchase_order_key
      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , ordered_outers
      , received_outers
      , is_order_line_finalized
      , expected_unit_price_per_outer
    FROM fact_purchase_order_line__source
)

, fact_purchase_order_line__cast_type AS (
    SELECT
      CAST(purchase_order_line_key AS INTEGER) AS purchase_order_line_key 
      , CAST(purchase_order_key AS INTEGER) AS purchase_order_key 
      , CAST(product_key AS INTEGER) AS product_key 
      , CAST(package_type_key AS INTEGER) AS package_type_key 
      , CAST(ordered_outers AS NUMERIC) AS ordered_outers 
      , CAST(received_outers AS NUMERIC) AS received_outers 
      , CAST(expected_unit_price_per_outer AS NUMERIC) AS expected_unit_price_per_outer 
      , CAST(is_order_line_finalized AS BOOLEAN) AS is_order_line_finalized_boolean

    FROM fact_purchase_order_line__rename_column
)

, fact_purchase_order_line__convert_boolean AS (
    SELECT *
      , CASE
          WHEN is_order_line_finalized_boolean IS TRUE THEN 'Order Line Finalized'
          WHEN is_order_line_finalized_boolean IS FALSE THEN 'Not Order Line Finalized'
          ELSE 'Undefined'
          END AS is_order_line_finalized
    FROM fact_purchase_order_line__cast_type
)

SELECT
  fact_purchase_order_line.purchase_order_line_key
  , purchase_order_key
  , fact_purchase_order_line.product_key
  , COALESCE(fact_purchase_order.supplier_key, -1) AS supplier_key 
  , COALESCE(fact_purchase_order.contact_person_key, -1) AS contact_person_key 
  , FARM_FINGERPRINT(
      CONCAT(fact_purchase_order_line.package_type_key
        , ','
        , fact_purchase_order.delivery_method_key
        , ','
        , fact_purchase_order_line.is_order_line_finalized
        , ','
        , fact_purchase_order.is_order_finalized)
      ) AS purchase_order_line_indicator_key
  , fact_purchase_order.order_date
  , fact_purchase_order.expected_delivery_date
  , fact_purchase_order_line.ordered_outers
  , fact_purchase_order_line.received_outers
  , fact_purchase_order_line.expected_unit_price_per_outer

FROM fact_purchase_order_line__convert_boolean AS fact_purchase_order_line
LEFT JOIN {{ref ('stg_fact_purchase_order')}} AS fact_purchase_order
  USING(purchase_order_key)
