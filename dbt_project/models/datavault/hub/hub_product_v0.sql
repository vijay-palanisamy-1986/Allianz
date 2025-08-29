
-- depends_on: {{ref('sat_product_v0')}}
-- depends_on: {{ref('sat_product_category_v0')}}
-- depends_on: {{ref('sat_product_discount_v0')}}

{{
    config(
        materialized="incremental",
        unique_key=["product_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(product_id) as BINARY(32)) as product_hk
        , product_id
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
    from 
        {{ ref("stg_product_v0") }} stg
)

select * from source_data