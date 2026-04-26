-- =============================================================================
-- stg_workday__employees.sql
-- Staging model for Workday employee records.
--
-- Source: RAW_HR.RAW.RAW_EMPLOYEES (via source('workday_raw', 'raw_employees'))
-- Output: one row per employee, reflecting the most recent snapshot of their
--         current state in Workday.
--
-- Key transformations:
--   1. Deduplication — RAW_EMPLOYEES is a full nightly snapshot, so multiple
--      rows exist per employee_id across run dates. We keep only the row from
--      the most recent _pipeline_run_date using ROW_NUMBER().
--   2. Column renaming — snake_case, no ALL_CAPS Snowflake defaults.
--   3. Type casting — dates already arrived as DATE from Parquet; validated here.
--   4. Standardisation — UPPER() on controlled vocabulary fields so downstream
--      models can filter on 'ACTIVE' rather than worrying about case variants.
--   5. Surrogate key — a hashed key on employee_id for reliable joining across
--      models without depending on source system key format.
--   6. Derived fields — full_name, tenure_days, is_active boolean computed here
--      so mart models don't repeat the logic.
--   7. Pipeline metadata stripped — _pipeline_* and _loaded_at columns are
--      dropped. They belong in raw only.
-- =============================================================================

with

source as (

    select * from {{ source('workday_raw', 'raw_employees') }}

),

-- -----------------------------------------------------------------------
-- Deduplicate: keep the most recent snapshot row per employee.
-- If the same employee appears in multiple nightly loads, only the
-- row from the latest _pipeline_run_date is retained.
-- -----------------------------------------------------------------------
ranked as (

    select
        *,
        row_number() over (
            partition by employee_id
            order by _pipeline_run_date desc
        ) as row_num

    from source

),

deduplicated as (

    select * from ranked where row_num = 1

),

-- -----------------------------------------------------------------------
-- Clean and standardise
-- -----------------------------------------------------------------------
staged as (

    select

        -- Surrogate key — hashed from employee_id for stable joining
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }}
                                                as employee_key,

        -- Natural key
        employee_id,

        -- Name fields
        trim(first_name)                        as first_name,
        trim(last_name)                         as last_name,
        trim(first_name) || ' ' || trim(last_name)
                                                as full_name,
        lower(trim(email))                      as email,

        -- Job information
        trim(job_title)                         as job_title,
        trim(department)                        as department,

        -- Location — standardise to UPPER for consistent filtering
        upper(trim(location_city))              as location_city,
        upper(trim(location_state))             as location_state,
        upper(trim(location_country))           as location_country,

        -- Dates
        hire_date,

        -- Status — UPPER so downstream filters are case-insensitive
        upper(trim(employment_status))          as employment_status,
        upper(trim(worker_type))                as worker_type,

        -- Manager
        manager_id,

        -- Derived: is_active boolean for cleaner mart filters
        case
            when upper(trim(employment_status)) = 'ACTIVE' then true
            else false
        end                                     as is_active,

        -- Derived: tenure in days as of today
        -- Null-safe: returns null if hire_date is missing
        case
            when hire_date is not null
            then datediff('day', hire_date, current_date())
        end                                     as tenure_days,

        -- Snapshot date this row was sourced from
        _pipeline_run_date                      as snapshot_date

    from deduplicated

)

select * from staged
