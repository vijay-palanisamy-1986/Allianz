{{
    config(
        materialized="incremental",
        unique_key=["order_hk","DV_HASH_DIFF"],
        incremental_strategy="merge",
    )
}}

with source_data as (
    select distinct
        CAST(SHA2_BINARY(order_id) as BINARY(32)) as order_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , CAST(SHA2_BINARY(source || order_id || quantity || amount) as BINARY(32)) as DV_HASH_DIFF
        , order_id
        , quantity
        , amount
    from 
        {{ ref("stg_sap_orders_v0") }}
)

select * from source_data

