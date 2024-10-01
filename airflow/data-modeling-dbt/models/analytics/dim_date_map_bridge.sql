WITH dim_date_map_bridge__source AS (
  SELECT date 
  FROM {{ref('dim_date')}}
)

SELECT 
  date
  , date AS date_for_joining
  , 'Current Year' AS comparing_type
FROM dim_date_map_bridge__source

UNION ALL
SELECT 
  date
  , date AS date_for_joining
  , 'Current Month' AS comparing_type
FROM dim_date_map_bridge__source

UNION ALL
SELECT 
  CAST(date + INTERVAL 1 MONTH AS DATE)
  , date AS date_for_joining
  , 'Last Month' AS comparing_type
FROM dim_date_map_bridge__source

UNION ALL
SELECT
  CAST(date + INTERVAL 1 YEAR AS DATE) -- use date column as date_for_joining to apply for Leap year (Feb has 29 days)
  , date AS date_for_joining
  , 'Last Year' AS comparing_type
FROM dim_date_map_bridge__source