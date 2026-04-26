-- =============================================================================
-- macros/generic_tests.sql
-- Custom reusable generic tests for the dbt_hr project.
--
-- Generic tests are defined as macros and referenced in YAML files exactly
-- like built-in tests (unique, not_null). Each macro must return a query
-- whose result set represents test FAILURES — dbt fails the test if any
-- rows are returned.
--
-- Usage in YAML:
--   columns:
--     - name: termination_date
--       tests:
--         - dbt_hr.not_in_future
--         - dbt_hr.not_before_column:
--             reference_column: hire_date
-- =============================================================================


-- -----------------------------------------------------------------------------
-- not_in_future
-- Fails if any value in the column is a future date.
-- Applied to: termination_date, hire_date
-- -----------------------------------------------------------------------------
{% test not_in_future(model, column_name) %}

    select
        {{ column_name }}  as failing_value,
        count(*)           as row_count
    from {{ model }}
    where {{ column_name }} > current_date()
    group by 1

{% endtest %}


-- -----------------------------------------------------------------------------
-- not_before_column
-- Fails if column_name < reference_column for any row.
-- Applied to: termination_date must not be before hire_date
-- -----------------------------------------------------------------------------
{% test not_before_column(model, column_name, reference_column) %}

    select
        {{ column_name }}       as failing_value,
        {{ reference_column }}  as reference_value,
        count(*)                as row_count
    from {{ model }}
    where
        {{ column_name }}      is not null
        and {{ reference_column }} is not null
        and {{ column_name }} < {{ reference_column }}
    group by 1, 2

{% endtest %}


-- -----------------------------------------------------------------------------
-- is_between
-- Fails if any numeric value falls outside [min_value, max_value].
-- Applied to: rolling_12m_turnover_rate anomaly detection
-- -----------------------------------------------------------------------------
{% test is_between(model, column_name, min_value, max_value) %}

    select
        {{ column_name }} as failing_value,
        count(*)          as row_count
    from {{ model }}
    where
        {{ column_name }} is not null
        and (
            {{ column_name }} < {{ min_value }}
            or {{ column_name }} > {{ max_value }}
        )
    group by 1

{% endtest %}
