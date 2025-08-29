{{
    config(
        materialized="incremental",
        unique_key=["customer_hk", "DV_HASH_DIFF"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(customer_id) as BINARY(32)) as customer_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , CAST(SHA2_BINARY(source || gender || age || family_size) as BINARY(32)) as DV_HASH_DIFF
        , customer_id
        , gender
        , age
        , family_size
    from 
        {{ ref("stg_customer_v0") }}
)

select * from source_data