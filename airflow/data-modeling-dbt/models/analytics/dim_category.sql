WITH dim_category__source AS (
  SELECT *
  FROM {{ref('stg_dim_external_category')}}
)

, dim_category__level_1 AS (
  SELECT
    category_key
    , category_name
    , category_key AS category_key_level_1
    , category_name AS category_name_level_1
    , 0 AS category_key_level_2
    , 'Undefined' AS category_name_level_2
    , 0 AS category_key_level_3
    , 'Undefined' AS category_name_level_3
    , 0 AS category_key_level_4
    , 'Undefined' AS category_name_level_4
    , category_level
  FROM dim_category__source  
  WHERE category_level = 1
)

, dim_category__level_2 AS (
  SELECT
    category_key
    , category_name
    , parent_category_key AS category_key_level_1
    , parent_category_name AS category_name_level_1
    , category_key AS category_key_level_2
    , category_name AS category_name_level_2
    , 0 AS category_key_level_3
    , 'Undefined' AS category_name_level_3
    , 0 AS category_key_level_4
    , 'Undefined' AS category_name_level_4
    , category_level
  FROM dim_category__source  
  WHERE category_level = 2
)

, dim_category__level_3 AS (
  SELECT
    category_level_3.category_key
    , category_level_3.category_name
    ,  category_level_1.parent_category_key AS category_key_level_1
    ,  category_level_1.parent_category_name AS category_name_level_1
    , category_level_3.parent_category_key AS category_key_level_2
    , category_level_3.parent_category_name AS category_name_level_2
    , category_level_3.category_key AS category_key_level_3
    , category_level_3.category_name AS category_name_level_3
    , 0 AS category_key_level_4
    , 'Undefined' AS category_name_level_4
    , category_level_3.category_level
  FROM dim_category__source AS category_level_3
  LEFT JOIN dim_category__source AS category_level_1
    ON category_level_1.category_key = category_level_3.parent_category_key
  WHERE category_level_3.category_level = 3
)

, dim_category__level_4 AS (
  SELECT
    category_level_4.category_key
    , category_level_4.category_name
    ,  category_level_1.parent_category_key AS category_key_level_1
    ,  category_level_1.parent_category_name AS category_name_level_1
    , category_level_2.parent_category_key AS category_key_level_2
    , category_level_2.parent_category_name AS category_name_level_2
    , category_level_4.parent_category_key AS category_key_level_3
    , category_level_4.parent_category_name AS category_name_level_3
    , category_level_4.category_key AS category_key_level_4
    , category_level_4.category_name AS category_name_level_4
    , category_level_4.category_level
  FROM dim_category__source AS category_level_4
  LEFT JOIN dim_category__source AS category_level_2
    ON category_level_2.category_key = category_level_4.parent_category_key
  LEFT JOIN dim_category__source AS category_level_1
    ON category_level_2.parent_category_key = category_level_1.category_key
  WHERE category_level_4.category_level = 4
)

SELECT
  category_key
  , category_name
  , category_key_level_1
  , category_name_level_1
  , category_key_level_2
  , category_name_level_2
  , category_key_level_3
  , category_name_level_3
  , category_key_level_4
  , category_name_level_4
  , category_level
FROM dim_category__level_1

UNION ALL

SELECT
  category_key
  , category_name
  , category_key_level_1
  , category_name_level_1
  , category_key_level_2
  , category_name_level_2
  , category_key_level_3
  , category_name_level_3
  , category_key_level_4
  , category_name_level_4
  , category_level
FROM dim_category__level_2

UNION ALL

SELECT
  category_key
  , category_name
  , category_key_level_1
  , category_name_level_1
  , category_key_level_2
  , category_name_level_2
  , category_key_level_3
  , category_name_level_3
  , category_key_level_4
  , category_name_level_4
  , category_level
FROM dim_category__level_3

UNION ALL

SELECT
  category_key
  , category_name
  , category_key_level_1
  , category_name_level_1
  , category_key_level_2
  , category_name_level_2
  , category_key_level_3
  , category_name_level_3
  , category_key_level_4
  , category_name_level_4
  , category_level
FROM dim_category__level_4