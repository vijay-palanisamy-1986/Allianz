
{{ config(materialized='table') }}

with source_data as (
    select 
        c3 as product_id
        , c4 as product_name
        , c5 as product_description
        , c2 as brand_name
        , c6 as product_sizes
        , TRY_CAST(c7 as number(38,4)) as price
        , TRY_CAST(c8 as number(38,4)) as mrp
        , c9 as discount_name
        , c10 as category_name
        , source
        , SYSDATE() as stg_load_ts
    from 
        {{ source("product", "PIM_PRODUCTS") }}
    where c1 <> 'PNo' -- Data in the table has header field. Adding this logic to skip the first line
)

select * from source_data