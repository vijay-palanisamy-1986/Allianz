
{{ config(materialized='table') }}

with source_data as (
    select 
        ORDERID as order_id
        , CUSTOMERID as customer_id
        , PRODUCTID as product_id
        , QTY as quantity
        , ORDERVALES as amount
        , source
        , SYSDATE() as stg_load_ts
    from 
        {{ source("orders", "SAP_ORDERS") }}
)

select * from source_data