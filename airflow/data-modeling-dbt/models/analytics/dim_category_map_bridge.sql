WITH dim_category AS (
  SELECT *
  FROM {{ref('dim_category')}}
)

, dim_category_map_bridge__parent_level_1 AS (
  SELECT  
    category_key_level_1 AS parent_category_key
    , category_name_level_1 AS parent_category_name
    , category_key AS child_category_key
    , category_name AS child_category_name
    , 1 AS parent_category_level
    , category_level AS child_category_level
    , category_level - 1 AS depth_from_parent
  FROM dim_category
  ORDER BY 1,2 
)

, dim_category_map_bridge__parent_level_2 AS (
  SELECT  
    category_key_level_2 AS parent_category_key
    , category_name_level_2 AS parent_category_name
    , category_key AS child_category_key
    , category_name AS child_category_name
    , 2 AS parent_category_level
    , category_level AS child_category_level
    , category_level - 2 AS depth_from_parent
  FROM dim_category
  WHERE category_key_level_2 <> 0
  ORDER BY 1,2 
)

, dim_category_map_bridge__parent_level_3 AS (
  SELECT  
    category_key_level_3 AS parent_category_key
    , category_name_level_3 AS parent_category_name
    , category_key AS child_category_key
    , category_name AS child_category_name
    , 3 AS parent_category_level
    , category_level AS child_category_level
    , category_level - 3 AS depth_from_parent
  FROM dim_category
  WHERE category_key_level_3 <> 0
  ORDER BY 1,2 
)

, dim_category_map_bridge__parent_level_4 AS (
  SELECT  
    category_key_level_4 AS parent_category_key
    , category_name_level_4 AS parent_category_name
    , category_key AS child_category_key
    , category_name AS child_category_name
    , 4 AS parent_category_level
    , category_level AS child_category_level
    , category_level - 4 AS depth_from_parent
  FROM dim_category
  WHERE category_key_level_4 <> 0
  ORDER BY 1,2 
)

, dim_category_map_bridge__union AS (
    SELECT
      parent_category_key
      , parent_category_name
      , child_category_key
      , child_category_name
      , parent_category_level
      , child_category_level
      , depth_from_parent
    FROM dim_category_map_bridge__parent_level_1
    
    UNION ALL
    SELECT
      parent_category_key
      , parent_category_name
      , child_category_key
      , child_category_name
      , parent_category_level
      , child_category_level
      , depth_from_parent
    FROM dim_category_map_bridge__parent_level_2
    
    UNION ALL
    SELECT
      parent_category_key
      , parent_category_name
      , child_category_key
      , child_category_name
      , parent_category_level
      , child_category_level
      , depth_from_parent
    FROM dim_category_map_bridge__parent_level_3
    
    UNION ALL
    SELECT
      parent_category_key
      , parent_category_name
      , child_category_key
      , child_category_name
      , parent_category_level
      , child_category_level
      , depth_from_parent
    FROM dim_category_map_bridge__parent_level_4
)

SELECT
  parent_category_key
  , parent_category_name
  , child_category_key
  , child_category_name
  , parent_category_level
  , child_category_level
  , depth_from_parent
FROM dim_category_map_bridge__union