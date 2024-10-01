select  supplier_key
		,category_key	
		,product_key	
		,customer_key	
		,sales_order_key	
		,sales_order_line_key	
		,picked_by_person_key
		,picking_completed_when	
        ,order_date
,expected_delivery_date
,backorder_order_key
,sales_order_line_indicator_key
,f.membership
,last_active_date
,recency
,frequency
,monetary
,recency_rank
,frequency_rank
,monetary_rank
,recency_score
,frequency_score
,monetary_score
,rfm_segment
,rfm_category
,customer_id
,customer_name
,bill_to_customer_id
,bill_to_customer_name
,is_statement_sent
,is_on_credit_hold
,payment_days
,standard_discount_percentage
,credit_limit
,customer_category_key
,customer_category_name
,buying_group_key
,buying_group_name
,delivery_method_key
,delivery_method_name
,delivery_city_key
,delivery_city_name
,state_province_key
,delivery_state_name
,account_opened_date
,primary_contact_person_key
,primary_contact_full_name
,current_membership
,begin_effective_date
,end_effective_date
,product_name
,is_chiller_stock
,lead_time_days
,quantity_per_outer
,brand_name
,pro.supplier_name
,color_key
,color_name
,unit_package_type_key
,unit_package_type_name
,outer_package_type_key
,outer_package_type_name
,pro.category_name
,pro.category_key_level_1
,pro.category_name_level_1
,pro.category_key_level_2
,pro.category_name_level_2
,pro.category_key_level_3
,pro.category_name_level_3
,pro.category_key_level_4
,pro.category_name_level_4
,cat.category_level
,s.supplier_category_key
,s.supplier_category_name
,case when frequency >= 12 then 'Từ 12 lần trở lên'
			when frequency >=9 and frequency >=12  then 'Từ 9 đến 12 lần'
			when frequency >=6 and frequency >=9  then 'Từ 6 đến 9 lần'
			when frequency >=3 and frequency >=6  then 'Từ 3 đến 6 lần'
			when frequency < 3 then 'Bé hơn 3 lần'
		else 'Other'
		end as freq_group
, case when credit_limit is NULL then 'Unlimited Credit' else 'Limit Credit' end as credit_grp
from {{ ref('fact_sales_order_line')}} as f
left join {{ ref('fact_rfm')}} rfm using(customer_key)
left join {{ ref('dim_customer')}} c using(customer_key)
left join {{ ref('dim_product')}} pro using(product_key)
left join {{ ref('dim_category')}} cat using(category_key)
left join {{ ref('dim_supplier')}} s using(supplier_key)
