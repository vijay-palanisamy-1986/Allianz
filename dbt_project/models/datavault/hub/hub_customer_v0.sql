
-- depends_on: {{ref('sat_customer_v0')}}
-- depends_on: {{ref('sat_customer_profession_v0')}}
-- depends_on: {{ref('sat_customer_spending_v0')}}

{{
    config(
        materialized="incremental",
        unique_key=["customer_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(customer_id) as BINARY(32)) as customer_hk
        , customer_id
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
    from 
        {{ ref("stg_customer_v0") }} stg
)

select * from source_data