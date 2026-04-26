-- =============================================================================
-- 05_snowpipe.sql
-- Creates two Snowpipe pipes — one for employees, one for terminations.
--
-- How Snowpipe works in this pipeline:
--   1. Lambda stages a Parquet file to S3
--      e.g. workday/raw/terminations/year=2024/month=06/day=30/20240630T020001Z_p000.parquet
--   2. S3 fires an event notification to an SQS queue owned by Snowflake
--   3. Snowpipe picks up the SQS message, reads the file from S3 via the stage,
--      and runs the COPY INTO statement defined in the pipe
--   4. Rows land in the raw table within seconds of the file arriving in S3
--
-- COPY INTO column mapping:
--   Parquet files produced by s3_stager._to_parquet() use lowercase column names
--   (e.g. "employee_id", "hire_date"). Snowflake column names are uppercase by
--   default. The $1:<field>::<type> syntax reads a named field from the Parquet
--   row and casts it to the target column type.
--
--   USE_LOGICAL_TYPE = TRUE in the file format means DATE and TIMESTAMP fields
--   are already typed correctly in the Parquet file, so most casts are just
--   confirming the type rather than converting strings.
--
-- AUTO_INGEST = TRUE:
--   Enables event-driven loading. Snowflake provides an SQS ARN after the pipe
--   is created (see SHOW PIPES below) which you configure as an S3 event
--   notification destination. Without this, Snowpipe must be triggered manually.
-- =============================================================================

USE DATABASE RAW_HR;
USE SCHEMA   RAW_HR.RAW;


-- -----------------------------------------------------------------------------
-- Pipe 1: Employees
-- Watches for new files under workday/raw/employees/
-- and loads them into RAW_HR.RAW.RAW_EMPLOYEES
-- -----------------------------------------------------------------------------

CREATE PIPE IF NOT EXISTS RAW_HR.RAW.PIPE_EMPLOYEES
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingests Workday employee Parquet files from S3 into RAW_EMPLOYEES.'
AS
COPY INTO RAW_HR.RAW.RAW_EMPLOYEES (
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    JOB_TITLE,
    DEPARTMENT,
    LOCATION_CITY,
    LOCATION_STATE,
    LOCATION_COUNTRY,
    HIRE_DATE,
    EMPLOYMENT_STATUS,
    WORKER_TYPE,
    MANAGER_ID,
    _PIPELINE_ENTITY,
    _PIPELINE_RUN_DATE,
    _PIPELINE_PAGE_NUM,
    _PIPELINE_WRITTEN_AT
)
FROM (
    SELECT
        $1:employee_id::VARCHAR,
        $1:first_name::VARCHAR,
        $1:last_name::VARCHAR,
        $1:email::VARCHAR,
        $1:job_title::VARCHAR,
        $1:department::VARCHAR,
        $1:location_city::VARCHAR,
        $1:location_state::VARCHAR,
        $1:location_country::VARCHAR,
        $1:hire_date::DATE,
        $1:employment_status::VARCHAR,
        $1:worker_type::VARCHAR,
        $1:manager_id::VARCHAR,
        $1:_pipeline_entity::VARCHAR,
        $1:_pipeline_run_date::DATE,
        $1:_pipeline_page_num::INTEGER,
        $1:_pipeline_written_at::TIMESTAMP_TZ
    FROM @RAW_HR.RAW.S3_WORKDAY_STAGE/employees/
    ( FILE_FORMAT => RAW_HR.RAW.PARQUET_FORMAT )
);


-- -----------------------------------------------------------------------------
-- Pipe 2: Terminations
-- Watches for new files under workday/raw/terminations/
-- and loads them into RAW_HR.RAW.RAW_TERMINATIONS
-- -----------------------------------------------------------------------------

CREATE PIPE IF NOT EXISTS RAW_HR.RAW.PIPE_TERMINATIONS
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingests Workday termination Parquet files from S3 into RAW_TERMINATIONS.'
AS
COPY INTO RAW_HR.RAW.RAW_TERMINATIONS (
    TERMINATION_ID,
    EMPLOYEE_ID,
    TERMINATION_DATE,
    LAST_DAY_OF_WORK,
    TERMINATION_TYPE,
    TERMINATION_REASON,
    TERMINATION_REASON_DETAIL,
    REHIRE_ELIGIBLE,
    RECORDED_BY,
    RECORDED_AT,
    _PIPELINE_ENTITY,
    _PIPELINE_RUN_DATE,
    _PIPELINE_PAGE_NUM,
    _PIPELINE_WRITTEN_AT
)
FROM (
    SELECT
        $1:termination_id::VARCHAR,
        $1:employee_id::VARCHAR,
        $1:termination_date::DATE,
        $1:last_day_of_work::DATE,
        $1:termination_type::VARCHAR,
        $1:termination_reason::VARCHAR,
        $1:termination_reason_detail::VARCHAR,
        $1:rehire_eligible::BOOLEAN,
        $1:recorded_by::VARCHAR,
        $1:recorded_at::TIMESTAMP_TZ,
        $1:_pipeline_entity::VARCHAR,
        $1:_pipeline_run_date::DATE,
        $1:_pipeline_page_num::INTEGER,
        $1:_pipeline_written_at::TIMESTAMP_TZ
    FROM @RAW_HR.RAW.S3_WORKDAY_STAGE/terminations/
    ( FILE_FORMAT => RAW_HR.RAW.PARQUET_FORMAT )
);


-- -----------------------------------------------------------------------------
-- Grant pipe permissions to pipeline role
-- -----------------------------------------------------------------------------

GRANT OPERATE ON PIPE RAW_HR.RAW.PIPE_EMPLOYEES    TO ROLE PIPELINE_ROLE;
GRANT OPERATE ON PIPE RAW_HR.RAW.PIPE_TERMINATIONS TO ROLE PIPELINE_ROLE;
GRANT MONITOR ON PIPE RAW_HR.RAW.PIPE_EMPLOYEES    TO ROLE PIPELINE_ROLE;
GRANT MONITOR ON PIPE RAW_HR.RAW.PIPE_TERMINATIONS TO ROLE PIPELINE_ROLE;


-- =============================================================================
-- POST-CREATION: Configure S3 Event Notifications
--
-- After running this script, run SHOW PIPES to retrieve the SQS ARN that
-- Snowflake generated for each pipe. You must then configure S3 event
-- notifications to send new-object events to those SQS queues.
-- =============================================================================

SHOW PIPES IN SCHEMA RAW_HR.RAW;

/*
  From the output, copy the notification_channel column value for each pipe.
  It will look like:
    arn:aws:sqs:us-east-1:123456789:sf-snowpipe-XXXX-employees
    arn:aws:sqs:us-east-1:123456789:sf-snowpipe-XXXX-terminations

  Then configure two S3 event notifications in the AWS console or via CLI:

  Notification 1 — Employees pipe:
    Bucket:      equipmentshare-data-lake
    Event type:  s3:ObjectCreated:*
    Prefix:      workday/raw/employees/
    Suffix:      .parquet
    Destination: SQS ARN from PIPE_EMPLOYEES notification_channel

  Notification 2 — Terminations pipe:
    Bucket:      equipmentshare-data-lake
    Event type:  s3:ObjectCreated:*
    Prefix:      workday/raw/terminations/
    Suffix:      .parquet
    Destination: SQS ARN from PIPE_TERMINATIONS notification_channel

  AWS CLI equivalent for the terminations pipe:
  -----------------------------------------------
  aws s3api put-bucket-notification-configuration \
    --bucket equipmentshare-data-lake \
    --notification-configuration '{
      "QueueConfigurations": [
        {
          "QueueArn": "<PIPE_TERMINATIONS_SQS_ARN>",
          "Events": ["s3:ObjectCreated:*"],
          "Filter": {
            "Key": {
              "FilterRules": [
                {"Name": "prefix", "Value": "workday/raw/terminations/"},
                {"Name": "suffix", "Value": ".parquet"}
              ]
            }
          }
        }
      ]
    }'
*/


-- =============================================================================
-- OPERATIONAL QUERIES
-- Use these to monitor pipe health and diagnose load failures.
-- =============================================================================

-- Check current pipe status and lag
SELECT SYSTEM$PIPE_STATUS('RAW_HR.RAW.PIPE_EMPLOYEES');
SELECT SYSTEM$PIPE_STATUS('RAW_HR.RAW.PIPE_TERMINATIONS');

-- View recent load history for terminations pipe (last 24 hours)
SELECT
    FILE_NAME,
    FILE_SIZE,
    ROW_COUNT,
    ROW_PARSED,
    FIRST_ERROR_MESSAGE,
    STATUS,
    LAST_LOAD_TIME
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME   => 'RAW_TERMINATIONS',
        START_TIME   => DATEADD('hour', -24, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;

-- Check for files that failed to load
SELECT
    FILE_NAME,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME   => 'RAW_TERMINATIONS',
        START_TIME   => DATEADD('day', -7, CURRENT_TIMESTAMP())
    )
)
WHERE STATUS = 'LOAD_FAILED'
ORDER BY LAST_LOAD_TIME DESC;
