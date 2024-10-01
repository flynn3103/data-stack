
WITH dim_person__source AS (
  SELECT  *
  FROM `vit-lam-data.wide_world_importers.application__people`
),

  dim_person__rename AS (
  SELECT 
    person_id AS person_key
    , full_name
    , search_name
    , is_system_user
    , is_employee
    , is_salesperson    
    , is_permitted_to_logon

  FROM dim_person__source
  ),

  dim_person__concast_type AS (
  SELECT 
    CAST (person_key AS INTEGER) AS person_key
    , CAST (full_name AS STRING) AS full_name
    , CAST(search_name AS STRING) AS search_name
    , CAST(is_system_user AS BOOLEAN) AS is_system_user_boolean
    , CAST(is_employee AS BOOLEAN) AS is_employee_boolean
    , CAST(is_salesperson AS BOOLEAN) AS is_salesperson_boolean
    , CAST(is_permitted_to_logon AS BOOLEAN) AS is_permitted_to_logon_boolean
  FROM dim_person__rename
  ),

  dim_person__convert_boolean AS (
  SELECT 
    *
    , CASE 
      WHEN is_system_user_boolean IS TRUE THEN 'System User'
      WHEN is_system_user_boolean IS FALSE THEN 'Not System User'
    ELSE 'Undefined'
    END AS is_system_user
    , CASE 
      WHEN is_employee_boolean IS TRUE THEN 'Employee'
      WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
    ELSE 'Undefined'
    END AS is_employee
    , CASE 
      WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
      WHEN is_salesperson_boolean IS FALSE THEN 'Not Salesperson'
    ELSE 'Undefined'
    END AS is_salesperson
    , CASE 
      WHEN is_permitted_to_logon_boolean IS TRUE THEN 'Is Permitted To Logon'
      WHEN is_permitted_to_logon_boolean IS FALSE THEN 'Permitted To Logon'
    ELSE 'Undefined'
    END AS is_permitted_to_logon
    
  FROM dim_person__concast_type

  ),

  dim_person__add_undefined_record AS (
  SELECT 
    person_key
    , full_name
    , search_name
    , is_system_user
    , is_employee
    , is_salesperson
    , is_permitted_to_logon
  FROM dim_person__convert_boolean
  
  UNION ALL

  SELECT 
    0 AS person_key
    , 'Undefined' AS full_name
    , 'Undefined' AS search_name
    , 'Undefined' AS is_system_user
    , 'Undefined' AS is_employee
    , 'Undefined' AS is_salesperson
    , 'Undefined' AS is_permitted_to_logon
  UNION ALL

  SELECT 
    -1 AS person_key
    , 'Error' AS full_name
    , 'Error' AS search_name
    , 'Error' AS is_system_user
    , 'Error' AS is_employee
    , 'Error' AS is_salesperson
    , 'Error' AS is_permitted_to_logon
  )

SELECT 
  person_key
  , full_name
  , search_name
  , is_system_user
  , is_employee
  , is_salesperson
  , is_permitted_to_logon
FROM dim_person__add_undefined_record
