-- =============================================================================
-- dim_employees.sql
-- Current-state employee dimension table.
--
-- Source:  ref('stg_workday__employees')
-- Purpose: Provides a clean, enriched employee record for every person currently
--          or previously employed. Used as the dimension table in mart joins and
--          as the headcount source for turnover calculations.
--
-- Key additions over staging:
--   - tenure_bucket:  binned tenure range for cohort analysis
--   - hire_year/quarter: time attributes for hiring trend analysis
--   - is_manager: derived from whether any other employee lists this employee_id
--     as their manager_id (computed via a self-referencing subquery)
-- =============================================================================

with

employees as (

    select * from {{ ref('stg_workday__employees') }}

),

-- -----------------------------------------------------------------------
-- Identify managers: any employee_id that appears as another
-- employee's manager_id is a manager.
-- -----------------------------------------------------------------------
managers as (

    select distinct
        manager_id as employee_id
    from employees
    where manager_id is not null

),

enriched as (

    select

        -- Keys
        e.employee_key,
        e.employee_id,

        -- Identity
        e.first_name,
        e.last_name,
        e.full_name,
        e.email,

        -- Job
        e.job_title,
        e.department,
        e.worker_type,
        e.manager_id,

        -- Location
        e.location_city,
        e.location_state,
        e.location_country,

        -- Status
        e.employment_status,
        e.is_active,

        -- Dates
        e.hire_date,

        -- Hire time attributes for trend analysis
        year(e.hire_date)                               as hire_year,
        quarter(e.hire_date)                            as hire_quarter,
        date_trunc('month', e.hire_date)                as hire_month_start,

        -- Tenure
        e.tenure_days,

        case
            when e.tenure_days < 90    then '1. 0-90 days'
            when e.tenure_days < 365   then '2. 91-365 days'
            when e.tenure_days < 730   then '3. 1-2 years'
            when e.tenure_days < 1825  then '4. 2-5 years'
            else                            '5. 5+ years'
        end                                             as tenure_bucket,

        -- Is this employee a manager of at least one other person?
        case
            when m.employee_id is not null then true
            else false
        end                                             as is_manager,

        -- Snapshot date
        e.snapshot_date

    from employees e
    left join managers m
        on e.employee_id = m.employee_id

)

select * from enriched
