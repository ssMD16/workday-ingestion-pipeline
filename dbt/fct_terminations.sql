-- =============================================================================
-- fct_terminations.sql
-- Termination fact table enriched with employee context at time of exit.
--
-- Sources: ref('stg_workday__terminations')
--          ref('dim_employees')
--
-- Purpose:
--   Joins every termination event to the employee's profile so downstream
--   models and dashboards have all the context they need in one table:
--   who left, when, why, from which department and location, and after
--   how long.
--
-- Key additions over staging:
--   - All dim_employees attributes joined in (department, location, job_title)
--   - tenure_at_exit_days: days between hire_date and termination_date
--   - tenure_at_exit_bucket: binned tenure range at point of exit
--   - exit_year / exit_quarter / exit_month_start: time attributes
--
-- Note on the join:
--   We use a LEFT JOIN so terminations without a matching employee record
--   are retained (employee_key is null) rather than silently dropped.
--   The relationships test in _workday__models.yml will have already flagged
--   those as a data quality issue — this keeps them visible in the mart.
-- =============================================================================

with

terminations as (

    select * from {{ ref('stg_workday__terminations') }}

),

employees as (

    select * from {{ ref('dim_employees') }}

),

joined as (

    select

        -- Termination keys
        t.termination_key,
        t.termination_id,

        -- Employee keys and identity
        t.employee_key,
        t.employee_id,
        e.full_name,
        e.first_name,
        e.last_name,
        e.email,

        -- Job context at time of exit
        e.job_title,
        e.department,
        e.worker_type,

        -- Geographic context
        e.location_city,
        e.location_state,
        e.location_country,

        -- Manager context
        e.manager_id,
        e.is_manager,

        -- Hire date (needed for tenure calculation)
        e.hire_date,

        -- Termination event details
        t.termination_date,
        t.last_day_of_work,
        t.termination_type,
        t.termination_reason,
        t.termination_reason_detail,
        t.is_voluntary,
        t.rehire_eligible,
        t.notice_period_days,
        t.recorded_by,
        t.recorded_at,

        -- Tenure at exit: days from hire to official termination date
        -- LEFT JOIN means e.hire_date may be null for unmatched employees
        case
            when e.hire_date is not null and t.termination_date is not null
            then datediff('day', e.hire_date, t.termination_date)
        end                                             as tenure_at_exit_days,

        -- Tenure bucket at exit — prefixed numbers force correct sort order
        -- in BI tools that sort alphabetically
        case
            when e.hire_date is null then 'Unknown'
            when datediff('day', e.hire_date, t.termination_date) < 90
                then '1. 0-90 days'
            when datediff('day', e.hire_date, t.termination_date) < 365
                then '2. 91-365 days'
            when datediff('day', e.hire_date, t.termination_date) < 730
                then '3. 1-2 years'
            when datediff('day', e.hire_date, t.termination_date) < 1825
                then '4. 2-5 years'
            else '5. 5+ years'
        end                                             as tenure_at_exit_bucket,

        -- Time attributes for time-series aggregation
        t.termination_year,
        t.termination_month,
        t.termination_month_start,
        quarter(t.termination_date)                     as termination_quarter,

        -- Is this a regrettable termination?
        -- Voluntary exits from high-tenure employees are typically regrettable.
        -- Threshold: voluntary AND tenure >= 2 years.
        case
            when t.is_voluntary = true
             and e.hire_date is not null
             and datediff('day', e.hire_date, t.termination_date) >= 730
            then true
            else false
        end                                             as is_regrettable,

        -- Snapshot
        t.snapshot_date

    from terminations t
    left join employees e
        on t.employee_key = e.employee_key

)

select * from joined
