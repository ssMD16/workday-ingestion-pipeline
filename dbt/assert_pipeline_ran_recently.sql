-- =============================================================================
-- tests/assert_pipeline_ran_recently.sql
--
-- Asserts that the ingestion pipeline produced a successful run in the
-- last 25 hours, querying the Snowflake audit table directly.
--
-- Why a separate test from dbt source freshness?
--   dbt source freshness (defined in _workday__sources.yml) checks whether
--   new rows arrived in the raw tables. That check fires if Snowpipe stalled
--   even though Lambda ran successfully and files are sitting in S3.
--
--   This test checks the PIPELINE_AUDIT table directly — it validates that
--   the Lambda itself executed and recorded a SUCCESS entry. The combination
--   of both tests gives two distinct failure signals:
--
--     source freshness fails + audit test passes → Snowpipe is stalled,
--                                                   files are in S3 but not loaded
--
--     source freshness passes + audit test fails → Impossible in practice but
--                                                   would indicate audit write failure
--
--     source freshness fails + audit test fails → Lambda itself failed or
--                                                  did not run (EventBridge issue)
--
-- This distinction dramatically narrows the time to diagnose an incident.
--
-- Severity: error — if the pipeline has not run, dashboards are stale
--           and the team needs to be alerted immediately.
-- =============================================================================

with last_successful_run as (

    select
        max(completed_at) as last_success_at

    from pipeline_audit.ingestion.runs

    where
        entity = 'terminations'
        and status = 'SUCCESS'

),

check as (

    select
        last_success_at,
        current_timestamp()                                     as checked_at,
        datediff('hour', last_success_at, current_timestamp()) as hours_since_last_run,

        -- Test fails (returns a row) if last success was more than 25 hours ago
        case
            when last_success_at is null then true
            when datediff('hour', last_success_at, current_timestamp()) > 25 then true
            else false
        end                                                     as is_stale

    from last_successful_run

)

select
    last_success_at,
    checked_at,
    hours_since_last_run,
    'Pipeline has not run successfully in the last 25 hours' as failure_reason

from check

where is_stale = true
