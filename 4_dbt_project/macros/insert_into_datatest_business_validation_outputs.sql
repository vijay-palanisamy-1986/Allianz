{% macro insert_into_datatest_business_validation_outputs(tablename) %}

INSERT INTO validation.{{tablename}} (
    invocation_id,
    dwh_run_date,
    test_name,
    table_name,
    pk_column_name,
    pk_column_value,
    business_column_name,
    business_column_value,
    test_message,
    test_column_name,
    test_column_value,
    test_result,
    test_details,
    inserted_dt,
    assignment_group,
    validation_type
    )

with test_table as (
    select * from {{ this }}
)

SELECT
    '{{invocation_id}}' as invocation_id,
    SYSDATE() as dwh_run_date,
    '{{ this.table }}' as test_name,
    test_table.table_name,    
    test_table.pk_column_name,
    test_table.pk_column_value,
    test_table.business_column_name,
    test_table.business_column_value,
    test_table.test_message,
    test_table.test_column_name,
    test_table.test_column_value,
    test_table.test_result,
    test_table.test_details,
    current_timestamp,
    test_table.assignment_group,
    test_table.validation_type
FROM test_table
;
{% endmacro %}