{{
    config(
        materialized="incremental",
        unique_key=["product_category_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(product_id || source || category_name) as BINARY(32)) as product_category_hk
        , CAST(SHA2_BINARY(product_id) as BINARY(32)) as product_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , category_name
    from 
        {{ ref("stg_product_v0") }}      
)

select * from source_data

