
{{ config(materialized='table') }}

with source_data as (
    select 
        customer_id
        , gender
        , age
        , CAST(annual_income as integer) as annual_income_inr
        , CAST(spending_score as integer) as spending_score 
        , profession
        , CAST(work_experience as integer) as work_experience 
        , CAST(family_size as integer) as family_size
        , source
        , SYSDATE() as stg_load_ts
    from 
        {{ source("customer", "CRM_CUSTOMER") }}
)

select * from source_data