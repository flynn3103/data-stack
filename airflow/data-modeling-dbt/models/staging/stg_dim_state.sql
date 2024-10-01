WITH dim_state__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
),
  dim_state__rename AS (
  SELECT 
    state_province_id AS state_province_key,
    state_province_name
  FROM dim_state__source
),

  dim_state__cast_type AS (
  SELECT 
    CAST(state_province_key AS INTEGER) AS state_province_key,
    CAST (state_province_name AS STRING) AS state_province_name

  FROM dim_state__rename
)
SELECT 
  state_province_key,
  state_province_name
  
FROM dim_state__cast_type


 

