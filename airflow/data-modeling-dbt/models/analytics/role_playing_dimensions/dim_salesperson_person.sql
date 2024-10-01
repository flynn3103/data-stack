SELECT 
  person_key AS salesperson_key
  , full_name AS salesperson_full_name
  , search_name AS salesperson_search_name
  , is_system_user
  , is_employee
  , is_salesperson

FROM {{ref ('dim_person')}}
WHERE 
  is_salesperson = 'Salesperson'
  OR person_key IN (0,1)