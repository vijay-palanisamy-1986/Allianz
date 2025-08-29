
{{ config(
    materialized='incremental'
    , unique_key =  'invocation_id' 
    ,post_hook="delete from {{this}} where invocation_id is null")
}}

select 
null::text as invocation_id
,null::date as dwh_run_date
,null::text as test_name
,null::text as table_name
,null::text as pk_column_name
,null::text as pk_column_value
,null::text as business_column_name
,null::text as business_column_value
,null::text as test_message
,null::text as test_column_name
,null::text as test_column_value
,null::text as test_result
,null::text as test_details
,null::timestamp with time zone as inserted_dt
,null::text as assignment_group
,null::text as validation_type

