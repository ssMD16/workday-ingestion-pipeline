-- =============================================================================
-- fct_turnover_rolling_12m.sql
-- 12-month rolling turnover rate by month, geography, and termination type.
--
-- Sources: ref('fct_terminations')
--          ref('dim_employees')
--
-- Output: one row per month per location_state, containing:
--   - Monthly termination counts (total, voluntary, involuntary)
--   - End-of-month headcount
--   - 12-month rolling termination counts
--   - 12-month rolling average headcount
--   - 12-month rolling turnover rate (%)
--
-- How rolling turnover is calculated:
--   Standard HR formula:
--     Rolling turnover % = (terminations in trailing 12 months
--                          / average monthly headcount over those 12 months)
--                          × 100
--
--   Average monthly headcount is computed as the mean of end-of-month
--   headcounts across the trailing 12 months. This is the most common
--   industry approach and avoids distortion from single-point-in-time counts.
--
-- How monthly headcount is calculated:
--   For a given month-end date D:
--     Active = employees hired on or before D
--              AND (currently active OR terminated after D)
--   Terminated employees contribute to headcount in months before their exit.
--
-- Date spine:
--   dbt_utils.date_spine generates one row per month from the earliest
--   hire date in the employee table to today. This ensures every month
--   in the reporting window has a row even if there were zero terminations.
-- =============================================================================

with

-- -----------------------------------------------------------------------
-- Date spine: one row per month from earliest hire to current month
-- -----------------------------------------------------------------------
date_spine as (

    {{
        dbt_utils.date_spine(
            datepart   = "month",
            start_date = "cast('2015-01-01' as date)",
            end_date   = "date_trunc('month', current_date())"
        )
    }}

),

months as (

    select
        date_trunc('month', date_day)           as month_start,
        last_day(date_trunc('month', date_day)) as month_end

    from date_spine

),

-- -----------------------------------------------------------------------
-- All employees with their termination date (null if still active).
-- This gives us the full population needed for headcount at any point.
-- -----------------------------------------------------------------------
employee_lifecycle as (

    select
        e.employee_key,
        e.employee_id,
        e.hire_date,
        e.location_state,
        e.location_country,
        e.department,
        t.termination_date      -- null for currently active employees

    from {{ ref('dim_employees') }} e
    left join {{ ref('fct_terminations') }} t
        on e.employee_key = t.employee_key

),

-- -----------------------------------------------------------------------
-- Monthly headcount per state:
-- For each month-end, count employees who were active on that date.
-- -----------------------------------------------------------------------
monthly_headcount as (

    select
        m.month_start,
        m.month_end,
        el.location_state,

        count(el.employee_key) as headcount

    from months m
    cross join employee_lifecycle el
    where
        -- Hired on or before month end
        el.hire_date <= m.month_end
        -- Either still active, or terminated after this month ended
        and (
            el.termination_date is null
            or el.termination_date > m.month_end
        )

    group by 1, 2, 3

),

-- -----------------------------------------------------------------------
-- Monthly terminations per state
-- -----------------------------------------------------------------------
monthly_terminations as (

    select
        date_trunc('month', termination_date)   as month_start,
        location_state,

        count(*)                                as total_terminations,
        count(case when is_voluntary     then 1 end) as voluntary_terminations,
        count(case when not is_voluntary then 1 end) as involuntary_terminations,
        count(case when is_regrettable   then 1 end) as regrettable_terminations

    from {{ ref('fct_terminations') }}
    where termination_date is not null

    group by 1, 2

),

-- -----------------------------------------------------------------------
-- Join headcount and terminations for every month × state combination.
-- Use LEFT JOIN so months with zero terminations still appear.
-- -----------------------------------------------------------------------
monthly_combined as (

    select
        mh.month_start,
        mh.month_end,
        mh.location_state,
        mh.headcount                                as eom_headcount,

        coalesce(mt.total_terminations,        0)   as total_terminations,
        coalesce(mt.voluntary_terminations,    0)   as voluntary_terminations,
        coalesce(mt.involuntary_terminations,  0)   as involuntary_terminations,
        coalesce(mt.regrettable_terminations,  0)   as regrettable_terminations

    from monthly_headcount mh
    left join monthly_terminations mt
        on  mh.month_start     = mt.month_start
        and mh.location_state  = mt.location_state

),

-- -----------------------------------------------------------------------
-- Rolling 12-month window calculations using SQL window functions.
--
-- ROWS BETWEEN 11 PRECEDING AND CURRENT ROW gives us the trailing 12
-- months (current month + 11 prior months) for each partition of state.
--
-- avg() over the same window gives the mean monthly headcount —
-- the denominator of the turnover rate formula.
-- -----------------------------------------------------------------------
rolling as (

    select

        month_start,
        month_end,
        location_state,
        eom_headcount,

        -- Monthly raw counts
        total_terminations,
        voluntary_terminations,
        involuntary_terminations,
        regrettable_terminations,

        -- 12-month rolling termination totals
        sum(total_terminations) over (
            partition by location_state
            order by month_start
            rows between 11 preceding and current row
        )                                               as rolling_12m_terminations,

        sum(voluntary_terminations) over (
            partition by location_state
            order by month_start
            rows between 11 preceding and current row
        )                                               as rolling_12m_voluntary,

        sum(involuntary_terminations) over (
            partition by location_state
            order by month_start
            rows between 11 preceding and current row
        )                                               as rolling_12m_involuntary,

        sum(regrettable_terminations) over (
            partition by location_state
            order by month_start
            rows between 11 preceding and current row
        )                                               as rolling_12m_regrettable,

        -- 12-month rolling average headcount (denominator)
        avg(eom_headcount) over (
            partition by location_state
            order by month_start
            rows between 11 preceding and current row
        )                                               as rolling_12m_avg_headcount,

        -- Number of months in the rolling window (< 12 early in history)
        count(*) over (
            partition by location_state
            order by month_start
            rows between 11 preceding and current row
        )                                               as months_in_window

    from monthly_combined

),

-- -----------------------------------------------------------------------
-- Final output: compute turnover rates from rolling totals.
-- Guard against division by zero with NULLIF.
-- Suppress rows where the window is < 12 months (incomplete history)
-- using the months_in_window column — flag them rather than drop them
-- so the dashboard can visually indicate partial data.
-- -----------------------------------------------------------------------
final as (

    select

        -- Time
        month_start,
        month_end,

        -- Geography
        location_state,

        -- Headcount
        eom_headcount,
        rolling_12m_avg_headcount,

        -- Monthly termination counts
        total_terminations,
        voluntary_terminations,
        involuntary_terminations,
        regrettable_terminations,

        -- Rolling 12-month termination counts
        rolling_12m_terminations,
        rolling_12m_voluntary,
        rolling_12m_involuntary,
        rolling_12m_regrettable,

        -- Rolling 12-month turnover rates (%)
        round(
            rolling_12m_terminations
            / nullif(rolling_12m_avg_headcount, 0)
            * 100,
        2)                                              as rolling_12m_turnover_rate,

        round(
            rolling_12m_voluntary
            / nullif(rolling_12m_avg_headcount, 0)
            * 100,
        2)                                              as rolling_12m_voluntary_rate,

        round(
            rolling_12m_involuntary
            / nullif(rolling_12m_avg_headcount, 0)
            * 100,
        2)                                              as rolling_12m_involuntary_rate,

        round(
            rolling_12m_regrettable
            / nullif(rolling_12m_avg_headcount, 0)
            * 100,
        2)                                              as rolling_12m_regrettable_rate,

        -- Flag rows where the full 12-month window is not yet available.
        -- Dashboards should visually distinguish these (e.g. dashed line).
        case
            when months_in_window < 12 then true
            else false
        end                                             as is_partial_window,

        months_in_window

    from rolling
    -- Exclude future months (date spine may project beyond today)
    where month_start <= date_trunc('month', current_date())

)

select * from final
