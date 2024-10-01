WITH dim_is_order_finalized AS (
  SELECT 
    'Order Finalized' AS is_order_finalized
  UNION ALL
  SELECT
    'Not Order Finalized' AS is_order_finalized 
  UNION ALL
  SELECT
    'Undefined' AS is_order_finalized 
 )

 , dim_is_order_line_finalized AS (
  SELECT 
    'Order Line Finalized' AS is_order_line_finalized
  UNION ALL
  SELECT
    'Not Order Line Finalized' AS is_order_line_finalized 
  UNION ALL
  SELECT
    'Undefined' AS is_order_line_finalized 
 )

SELECT 
  FARM_FINGERPRINT(
    CONCAT(package_type_key
      , ','
      , delivery_method_key
      , ','
      , is_order_line_finalized
      , ','
      , is_order_finalized)
    ) AS purchase_order_line_indicator_key
    , dim_package_type.package_type_key
    , dim_package_type.package_type_name
    , dim_delivery_method.delivery_method_key
    , dim_delivery_method.delivery_method_name
    , is_order_line_finalized
    , is_order_finalized

FROM {{ref('stg_dim_package_type')}} AS dim_package_type
CROSS JOIN {{ref('stg_dim_delivery_method')}} AS dim_delivery_method
CROSS JOIN dim_is_order_finalized
CROSS JOIN dim_is_order_line_finalized

