-- =============================================================================
-- stg_workday__terminations.sql
-- Staging model for Workday termination records.
--
-- Source: RAW_HR.RAW.RAW_TERMINATIONS (via source('workday_raw', 'raw_terminations'))
-- Output: one row per unique termination event, cleaned and standardised.
--
-- Key transformations:
--   1. Deduplication — although terminations is incremental, a re-run or a
--      full_load=true event could load the same termination_id twice.
--      We deduplicate on termination_id keeping the most recently loaded row.
--   2. Column renaming and standardisation — same conventions as employees.
--   3. Derived fields — days_between_termination_and_last_work, tenure_at_exit,
--      and is_voluntary boolean computed here for mart reuse.
--   4. Surrogate keys — one on termination_id, one on employee_id so this
--      model joins cleanly to stg_workday__employees on employee_key.
--   5. Pipeline metadata stripped.
--
-- Note on tenure_at_exit:
--   Joining to stg_workday__employees here would create a dependency between
--   two staging models, which is an anti-pattern in dbt. Tenure at exit is
--   instead computed in the mart layer where the join is explicit and documented.
-- =============================================================================

with

source as (

    select * from {{ source('workday_raw', 'raw_terminations') }}

),

-- -----------------------------------------------------------------------
-- Deduplicate: keep the most recently loaded row per termination_id.
-- Handles re-runs and full_load=true events that might load duplicate
-- termination records.
-- -----------------------------------------------------------------------
ranked as (

    select
        *,
        row_number() over (
            partition by termination_id
            order by _pipeline_written_at desc
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

        -- Surrogate key on the termination event itself
        {{ dbt_utils.generate_surrogate_key(['termination_id']) }}
                                                    as termination_key,

        -- Surrogate key on the employee — joins to employee_key
        -- in stg_workday__employees and downstream mart models
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }}
                                                    as employee_key,

        -- Natural keys
        termination_id,
        employee_id,

        -- Dates
        termination_date,
        last_day_of_work,

        -- Termination classification — UPPER for consistent filtering
        upper(trim(termination_type))               as termination_type,
        upper(trim(termination_reason))             as termination_reason,
        trim(termination_reason_detail)             as termination_reason_detail,

        -- Rehire eligibility
        rehire_eligible,

        -- HR data entry metadata
        trim(recorded_by)                           as recorded_by,
        recorded_at,

        -- Derived: is_voluntary boolean for cleaner mart aggregations
        case
            when upper(trim(termination_type)) = 'VOLUNTARY' then true
            else false
        end                                         as is_voluntary,

        -- Derived: days between official termination and actual last day of work.
        -- Negative = employee left before official date (unusual, flags data issues).
        -- Positive = garden leave / notice period served.
        -- Zero     = standard same-day termination.
        case
            when termination_date is not null and last_day_of_work is not null
            then datediff('day', last_day_of_work, termination_date)
        end                                         as notice_period_days,

        -- Derived: calendar year and month of termination for time-series grouping
        year(termination_date)                      as termination_year,
        month(termination_date)                     as termination_month,
        date_trunc('month', termination_date)       as termination_month_start,

        -- Snapshot date this row was sourced from
        _pipeline_run_date                          as snapshot_date

    from deduplicated

)

select * from staged
