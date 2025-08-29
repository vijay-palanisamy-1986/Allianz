-- depends_on: {{ref('hub_customer_v0')}}
-- depends_on: {{ref('hub_product_v0')}}
-- depends_on: {{ref('sat_orders_v0')}}

{{
    config(
        materialized="incremental",
        unique_key=["order_hk"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(order_id || customer_id || product_id) as BINARY(32)) as lnk_orders_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , CAST(SHA2_BINARY(order_id) as BINARY(32)) as order_hk
        , CAST(SHA2_BINARY(customer_id) as BINARY(32)) as customer_hk
        , CAST(SHA2_BINARY(product_id) as BINARY(32)) as product_hk
    from 
        {{ ref("stg_sap_orders_v0") }}
)

select * from source_data

