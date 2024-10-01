WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`

),

  fact_sales_order_line__rename_column AS (
  SELECT
    order_line_id AS sales_order_line_key
    , order_id AS sales_order_key
    , stock_item_id AS product_key
    , package_type_id AS package_type_key
    , quantity
    , unit_price
    , tax_rate
    , picked_quantity
    , picking_completed_when
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
  SELECT
    CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
    , CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(product_key AS INTEGER) AS product_key
    , CAST(package_type_key AS INTEGER) AS package_type_key
    , CAST (quantity AS INTEGER) AS quantity
    , CAST (unit_price AS FLOAT64) AS unit_price
    , CAST (tax_rate AS NUMERIC) AS tax_rate
    , CAST (picked_quantity AS INTEGER) AS picked_quantity
    , CAST (picking_completed_when AS date) picking_completed_when

  FROM fact_sales_order_line__rename_column
)  

SELECT 
  fact_sales_order_line.sales_order_key
  , fact_sales_order_line.product_key
  , fact_sales_order_line.sales_order_line_key
  , COALESCE(fact_sales_order.picked_by_person_key,-1) AS picked_by_person_key
  , COALESCE(fact_sales_order.salesperson_person_key,-1) AS salesperson_person_key
  , fact_sales_order.customer_key
  , fact_sales_order_line.quantity
  , fact_sales_order_line.unit_price
  , fact_sales_order_line.quantity * fact_sales_order_line.unit_price AS gross_amount
  , fact_sales_order_line.tax_rate
  , fact_sales_order_line.picked_quantity
  , fact_sales_order_line.picking_completed_when
  , fact_sales_order.order_date
  , fact_sales_order.expected_delivery_date
  , fact_sales_order.backorder_order_key
  , FARM_FINGERPRINT(
    CONCAT(fact_sales_order.is_undersupply_backordered,
          ',',
          fact_sales_order_line.package_type_key
          )
   ) AS sales_order_line_indicator_key
  , COALESCE(fact_sales_order.membership, 'None') AS membership
FROM fact_sales_order_line__cast_type AS fact_sales_order_line
LEFT JOIN {{ref('stg_fact_sales_order')}} AS fact_sales_order
ON fact_sales_order_line.sales_order_key = fact_sales_order.sales_order_key
LEFT JOIN {{ref('stg_dim_package_type')}} AS dim_package_type
ON fact_sales_order_line.package_type_key = dim_package_type.package_type_key