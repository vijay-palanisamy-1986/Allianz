{{
    config(
        materialized="incremental",
        unique_key=["product_hk","DV_HASH_DIFF"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(product_id) as BINARY(32)) as product_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , CAST(SHA2_BINARY(source || product_id || product_name || product_description || product_sizes || brand_name) as BINARY(32)) as DV_HASH_DIFF
        , product_id
        , product_name
        , product_description
        , product_sizes
        , brand_name
    from 
        {{ ref("stg_product_v0") }}
)

select * from source_data

