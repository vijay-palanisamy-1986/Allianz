-- depends_on: {{ref('datatest_business_validation_outputs')}}

{{ config(
    materialized='view'
    ,post_hook="{{insert_into_datatest_business_validation_outputs('datatest_business_validation_outputs')}}"
    )
}}

with base_data as(
	-- HIGH Risk shoppers
	select distinct
		cust.customer_hk
		, cust.customer_id
		, spending_bucket.SPENDING_SCORE
		, prof.ANNUAL_INCOME_INR
		, prof.ANNUAL_INCOME_BUCKET
	from 
		{{ ref("sat_customer_v0") }} cust
		inner join (
			select customer_hk, SPENDING_SCORE from {{ ref("sat_customer_spending_v0") }} 
			QUALIFY row_number() over (partition by customer_hk ORDER BY dv_load_datetime DESC) = 1
			) spending_bucket on spending_bucket.customer_hk = cust.customer_hk
		inner join (
			select customer_hk, ANNUAL_INCOME_INR, ANNUAL_INCOME_BUCKET from {{ ref("sat_customer_profession_v0") }} 
			QUALIFY row_number() over (partition by customer_hk ORDER BY dv_load_datetime DESC) = 1
			) prof on prof.customer_hk = cust.customer_hk
	WHERE
		spending_bucket.SPENDING_SCORE > 75
		and prof.ANNUAL_INCOME_BUCKET = 'LOW'
)

select distinct
	'{{ ref('sat_customer_v0') }}' as table_name
	, 'customer_hk' as pk_column_name
	, cast (customer_hk as varchar) as pk_column_value
	, 'customer_id' as business_column_name
	, customer_id as business_column_value
	, 'Received high risk spender transacation' AS test_message
	, 'spending_score' as test_column_name
	, spending_score as test_column_value
	, concat('No of transactions: ', count(1)) as test_result
	, '' as test_details
	, 'SAP - Order' as assignment_group
	, 'Warning' as validation_type
from 
	base_data
group by all