
{{ config(materialized='table') }}

with source_data as (
    select 
        ORDERID as order_id
        , CUSTOMERID as customer_id
        , PRODUCTID as product_id
        , QTY as quantity
        , ORDERVALES as amount
        , source
        , CAST(SHA2_BINARY(order_id) as BINARY(32)) as order_hk
        , SYSDATE() as DV_LOAD_DATETIME
        , source as DV_RECORD_SOURCE
        , CAST(SHA2_BINARY(source || order_id || quantity || amount) as BINARY(32)) as DV_HASH_DIFF
    from 
        {{ source("orders", "SAP_ORDERS") }}
)

select * from source_data