WITH fact_sales_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename_column AS (
    SELECT 
      order_id AS sales_order_key
      , customer_id
      , picked_by_person_id AS picked_by_person_key
      , salesperson_person_id AS salesperson_person_key
      , backorder_order_id AS backorder_order_key
      , order_date
      , expected_delivery_date
      , is_undersupply_backordered
    FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
    SELECT 
      CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST (customer_id AS INTEGER) AS customer_id
      , CAST (picked_by_person_key AS INTEGER) AS picked_by_person_key
      , CAST (salesperson_person_key AS INTEGER) AS salesperson_person_key
      , CAST (backorder_order_key AS INTEGER) AS backorder_order_key
      , CAST (order_date AS date) as order_date
      , CAST (expected_delivery_date AS date) as expected_delivery_date
      , CAST (is_undersupply_backordered AS BOOLEAN) AS is_undersupply_backordered_boolean

    FROM fact_sales_order__rename_column
)

, fact_sales_order__convert_boolean AS (
    SELECT *
      , CASE 
        WHEN is_undersupply_backordered_boolean IS TRUE THEN 'Undersupply Backordered'
        WHEN is_undersupply_backordered_boolean IS FALSE THEN 'Not Undersupply Backordered'
      ELSE 'Undefined'
      END AS is_undersupply_backordered
    
    FROM fact_sales_order__cast_type
)

, fact_sales_order__enrich AS (
SELECT 
  fact_sales_order.sales_order_key
  , COALESCE(dim_customer.customer_key, FARM_FINGERPRINT(CONCAT(-1, CAST('2013-01-01' AS DATE)))) AS customer_key
  , COALESCE(fact_sales_order.picked_by_person_key,0) AS picked_by_person_key
  , COALESCE(fact_sales_order.salesperson_person_key,0) AS salesperson_person_key
  , COALESCE(fact_sales_order.backorder_order_key,0) AS backorder_order_key
  , fact_sales_order.order_date
  , fact_sales_order.expected_delivery_date
  , fact_sales_order.is_undersupply_backordered
  , COALESCE(dim_customer.membership, 'None') AS membership
  , COALESCE(dim_customer.begin_effective_date, '2013-01-01') AS begin_effective_date
  , COALESCE(dim_customer.end_effective_date, '2050-12-31') AS end_effective_date
FROM fact_sales_order__convert_boolean AS fact_sales_order
LEFT JOIN {{ref('dim_customer')}}
  ON dim_customer.customer_id = fact_sales_order.customer_id
  AND fact_sales_order.order_date BETWEEN dim_customer.begin_effective_date AND dim_customer.end_effective_date
)

SELECT 
  sales_order_key
  , customer_key
  , picked_by_person_key
  , salesperson_person_key
  , backorder_order_key
  , order_date
  , expected_delivery_date
  , is_undersupply_backordered
  , membership
  , begin_effective_date
  , end_effective_date
FROM fact_sales_order__enrich
