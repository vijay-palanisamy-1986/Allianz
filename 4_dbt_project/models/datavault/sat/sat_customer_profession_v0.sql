{{
    config(
        materialized="incremental",
        unique_key=["customer_profession_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(customer_id || source || profession || annual_income_inr || work_experience) as BINARY(32)) as customer_profession_hk
        , CAST(SHA2_BINARY(customer_id) as BINARY(32)) as customer_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , profession
        , CASE
            WHEN lower(profession) IS NULL THEN 'UNKNOWN'
            WHEN lower(profession) IN ('doctor','healthcare') THEN 'HIGH'
            WHEN lower(profession) IN ('artist','homemaker') THEN 'MEDIUM'
            ELSE 'LOW'
        END as risk_category
        , annual_income_inr
        , CASE
            WHEN annual_income_inr BETWEEN       0 and   100000 THEN 'LOW'
            WHEN annual_income_inr BETWEEN  100001 and  1000000 THEN 'MEDIUM'
            WHEN annual_income_inr BETWEEN 1000001 and 10000000 THEN 'HIGH'
            WHEN annual_income_inr > 10000000 THEN 'HNI'
            ELSE 'NA'
        END as annual_income_bucket
        , work_experience
    from 
        {{ ref("stg_customer_v0") }}
)

select * from source_data