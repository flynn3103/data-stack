SELECT 
  person_key AS picked_by_person_key
  , full_name AS picked_by_person_full_name
  , search_name AS picked_by_person_search_name
  , is_system_user
  , is_employee
  , is_salesperson

FROM {{ref ('dim_person')}}
