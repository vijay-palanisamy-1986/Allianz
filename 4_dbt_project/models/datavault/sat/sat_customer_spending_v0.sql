{{
    config(
        materialized="incremental",
        unique_key=["customer_spending_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select
        CAST(SHA2_BINARY(customer_id || source || spending_score) as BINARY(32)) as customer_spending_hk
        , CAST(SHA2_BINARY(customer_id) as BINARY(32)) as customer_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , spending_score
        , CASE
            WHEN spending_score BETWEEN 0 and 25 THEN '00-25'
            WHEN spending_score BETWEEN 26 and 50 THEN '26-50'
            WHEN spending_score BETWEEN 51 and 75 THEN '51-75'
            WHEN spending_score BETWEEN 76 and 100 THEN '76-100'
            ELSE 'NA'
        END as spending_bucket
    from 
        {{ ref("stg_customer_v0") }}
)

select * from source_data