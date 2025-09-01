{%- set source_model = "stg_sap_orders_v1" -%}
{%- set src_pk = ("order_hk") -%}
{%- set src_hashdiff = "DV_HASH_DIFF" -%}
{%- set src_payload = [
    "order_id"
    ,"quantity"
    ,"amount"
    ] -%}
{%- set src_eff = "DV_LOAD_DATETIME" -%}
{%- set src_ldts = "DV_LOAD_DATETIME" -%}
{%- set src_source = "DV_RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
