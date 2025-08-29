{{
    config(
        materialized="incremental",
        unique_key=["product_discount_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(product_id || source || discount_name) as BINARY(32)) as product_discount_hk
        , CAST(SHA2_BINARY(product_id) as BINARY(32)) as product_hk
        , SYSDATE()  as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , discount_name
        , CAST( LEFT( discount_name , POSITION( '%' IN discount_name)-1 ) as NUMBER(38,2)) as discount_percentage
    from 
        {{ ref("stg_product_v0") }}
)

select * from source_data
