WITH dim_customer__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customers`
)
, dim_customer__rename AS (
  SELECT  
    customer_id
    , customer_name 
    , bill_to_customer_id
    , is_statement_sent AS is_statement_sent_boolean
    , is_on_credit_hold AS is_on_credit_hold_boolean
    , payment_days
    , standard_discount_percentage
    , credit_limit
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
    , delivery_method_id AS delivery_method_key
    , delivery_city_id AS delivery_city_key
    , account_opened_date
    , primary_contact_person_id AS primary_contact_person_key

  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT 
    CAST (customer_id AS INTEGER) AS customer_id
    , CAST (customer_name AS STRING) AS customer_name
    , CAST (bill_to_customer_id AS INTEGER) AS bill_to_customer_id
    , CAST (is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean
    , CAST (is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST (payment_days AS INTEGER) AS payment_days
    , CAST (standard_discount_percentage AS INTEGER) AS standard_discount_percentage
    , CAST (credit_limit AS INTEGER) AS credit_limit
    , CAST (customer_category_key AS INTEGER) AS customer_category_key
    , CAST (buying_group_key AS INTEGER) AS buying_group_key
    , CAST (delivery_method_key AS INTEGER) AS delivery_method_key
    , CAST(delivery_city_key AS INTEGER) AS delivery_city_key
    , CAST(account_opened_date AS date) AS account_opened_date
    , CAST(primary_contact_person_key AS INTEGER) AS primary_contact_person_key
  FROM dim_customer__rename
  )

, dim_customer__convert_boolean AS (
  SELECT *
    , CASE 
      WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
      WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
      ELSE 'Undefined'
      END AS is_on_credit_hold
    , CASE 
      WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
      WHEN is_statement_sent_boolean IS FALSE THEN 'Not Statement Sent'
      ELSE 'Undefined'
      END AS is_statement_sent

  FROM dim_customer__cast_type
  )

, dim_customer__add_undefined_record AS (
  SELECT 
    customer_id
    , customer_name
    , bill_to_customer_id
    , is_statement_sent
    , is_on_credit_hold
    , payment_days
    , standard_discount_percentage
    , credit_limit
    , customer_category_key
    , buying_group_key
    , delivery_method_key
    , delivery_city_key
    , account_opened_date
    , primary_contact_person_key
  FROM dim_customer__convert_boolean

  UNION ALL

  SELECT
    0 AS customer_id
    , 'Undefined' AS customer_name
    , 0 AS bill_to_customer_id
    , 'Undefined' AS is_statement_sent
    , 'Undefined' AS is_on_credit_hold
    , CAST(NULL AS INTEGER) AS payment_days
    , CAST(NULL AS INTEGER) AS standard_discount_percentage
    , CAST(NULL AS INTEGER) AS credit_limit
    , 0 AS customer_category_key
    , 0 AS buying_group_key
    , 0 AS delivery_method_key
    , 0 AS delivery_city_key
    , CAST(NULL AS DATE) AS account_opened_date
    , 0 AS primary_contact_person_key

  UNION ALL

  SELECT
    -1 AS customer_id
    , 'Invalid' AS customer_name
    , -1 AS bill_to_customer_id
    , 'Invalid' AS is_statement_sent
    , 'Invalid' AS is_on_credit_hold
    , CAST(NULL AS INTEGER) AS payment_days
    , CAST(NULL AS INTEGER) AS standard_discount_percentage
    , CAST(NULL AS INTEGER) AS credit_limit
    , -1 AS customer_category_key
    , -1 AS buying_group_key
    , -1 AS delivery_method_key
    , -1 AS delivery_city_key
    , CAST(NULL AS DATE) AS account_opened_date
    , -1 AS primary_contact_person_key
)

, dim_customer__integrate_scd_2 AS ( -- Slowly changing dimension type 2 allow to display membership overtime, including start date and end date
  SELECT 
    dim_customer.*
    , COALESCE(dim_customer_membership.membership, 'None') AS membership
    , COALESCE(dim_customer_membership.begin_effective_date, '2013-01-01') AS begin_effective_date
    , COALESCE(dim_customer_membership.end_effective_date, '2050-12-31') AS end_effective_date
  FROM dim_customer__add_undefined_record AS dim_customer
  LEFT JOIN {{ref('stg_dim_customer_membership')}} AS dim_customer_membership
  USING (customer_id)
)

, dim_customer__generate_key AS (
  SELECT *
    , FARM_FINGERPRINT(CONCAT(customer_id, begin_effective_date)) AS customer_key
  FROM dim_customer__integrate_scd_2 AS dim_customer
)

, dim_customer__current_membership AS (
  SELECT *
  FROM {{ref('stg_dim_customer_membership')}}
  WHERE CURRENT_DATE() BETWEEN begin_effective_date AND end_effective_date
)
SELECT 
  dim_customer.customer_key
  , dim_customer.customer_id
  , dim_customer.customer_name
  , dim_customer.bill_to_customer_id
  , dim_bill_to_customer.customer_name AS bill_to_customer_name
  , dim_customer.is_statement_sent
  , dim_customer.is_on_credit_hold
  , dim_customer.payment_days
  , dim_customer.standard_discount_percentage
  , dim_customer.credit_limit
  , dim_customer.customer_category_key
  , COALESCE(dim_customer_category.customer_category_name,'Undefined') AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE(dim_buying_group.buying_group_name,'Undefined') AS buying_group_name
  , dim_customer.delivery_method_key
  , COALESCE(dim_delivery_method.delivery_method_name,'Undefined') AS delivery_method_name
  , dim_customer.delivery_city_key
  , COALESCE(dim_city.city_name,'Undefined') AS delivery_city_name
  , dim_city.state_province_key
  , COALESCE(dim_city.state_province_name,'Undefined') AS delivery_state_name
  , dim_customer.account_opened_date
  , dim_customer.primary_contact_person_key
  , dim_person.full_name AS primary_contact_full_name
  , COALESCE(dim_customer__current_membership.membership, 'None') AS current_membership
  , dim_customer.membership
  , dim_customer.begin_effective_date
  , dim_customer.end_effective_date
FROM dim_customer__generate_key AS dim_customer 
LEFT JOIN {{ref ('stg_dim_customer_category')}} AS dim_customer_category 
  ON dim_customer_category.customer_category_key = dim_customer.customer_category_key

LEFT JOIN {{ref ('stg_dim_sales_buying_group')}} AS dim_buying_group
  ON dim_buying_group.buying_group_key = dim_customer.buying_group_key

LEFT JOIN {{ref('stg_dim_delivery_method')}} AS dim_delivery_method
  ON dim_delivery_method.delivery_method_key = dim_customer.delivery_method_key

LEFT JOIN {{ref ('stg_dim_city')}} AS dim_city
  ON dim_city.city_key = dim_customer.delivery_city_key

LEFT JOIN {{ref ('dim_person')}}
  ON dim_person.person_key = dim_customer.primary_contact_person_key

LEFT JOIN dim_customer__add_undefined_record AS dim_bill_to_customer
  ON dim_customer.bill_to_customer_id = dim_bill_to_customer.customer_id
LEFT JOIN dim_customer__current_membership
  ON dim_customer__current_membership.customer_id = dim_customer.customer_id