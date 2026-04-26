-- =============================================================================
-- 03_raw_tables.sql
-- Creates the raw employee and termination tables in RAW_HR.RAW.
--
-- Design principles:
--   1. Columns match the Parquet schema produced by s3_stager._to_parquet()
--      exactly — no casting or renaming required in the COPY INTO statement.
--   2. All source columns are nullable. Raw tables never reject records for
--      null values — that is dbt's job in the staging layer.
--   3. Four _pipeline_ metadata columns are included at the end of each table.
--      These are injected by the stager and allow every row to be traced back
--      to the exact S3 file, run date, and page that produced it.
--   4. A _loaded_at TIMESTAMP_TZ column is added as a Snowflake DEFAULT so the
--      exact moment Snowpipe inserted each row is recorded automatically.
--   5. Tables use CLUSTER BY on the most common filter columns to improve
--      query performance for the 12-month rolling window calculations dbt
--      will run.
-- =============================================================================

USE DATABASE RAW_HR;
USE SCHEMA   RAW_HR.RAW;


-- -----------------------------------------------------------------------------
-- RAW_EMPLOYEES
-- One row per employee record as returned by the Workday Active_Employees
-- RaaS report. This is a full-refresh entity — all employees are reloaded
-- nightly, so the table accumulates one snapshot per run date.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RAW_HR.RAW.RAW_EMPLOYEES (

    -- Source fields — match the WorkdayClient employee dict keys exactly
    EMPLOYEE_ID         VARCHAR         COMMENT 'Workday employee identifier (e.g. EMP-1001)',
    FIRST_NAME          VARCHAR         COMMENT 'Employee first name',
    LAST_NAME           VARCHAR         COMMENT 'Employee last name',
    EMAIL               VARCHAR         COMMENT 'Work email address',
    JOB_TITLE           VARCHAR         COMMENT 'Current job title in Workday',
    DEPARTMENT          VARCHAR         COMMENT 'Department the employee belongs to',
    LOCATION_CITY       VARCHAR         COMMENT 'City of the employee primary work location',
    LOCATION_STATE      VARCHAR         COMMENT 'State / province of primary work location',
    LOCATION_COUNTRY    VARCHAR         COMMENT 'Country of primary work location',
    HIRE_DATE           DATE            COMMENT 'Original hire date',
    EMPLOYMENT_STATUS   VARCHAR         COMMENT 'Active | Terminated',
    WORKER_TYPE         VARCHAR         COMMENT 'Employee | Contractor',
    MANAGER_ID          VARCHAR         COMMENT 'Employee ID of the direct manager (nullable)',

    -- Pipeline metadata — injected by s3_stager._to_parquet()
    _PIPELINE_ENTITY        VARCHAR         COMMENT 'Always "employees" for this table',
    _PIPELINE_RUN_DATE      DATE            COMMENT 'Date the Lambda run executed',
    _PIPELINE_PAGE_NUM      INTEGER         COMMENT 'Zero-based page index within the run',
    _PIPELINE_WRITTEN_AT    TIMESTAMP_TZ    COMMENT 'UTC timestamp the S3 file was written',

    -- Snowflake-managed audit column
    _LOADED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
                            COMMENT 'UTC timestamp Snowpipe inserted this row'

)
CLUSTER BY (_PIPELINE_RUN_DATE, EMPLOYMENT_STATUS)
COMMENT = 'Raw employee records from Workday. Full snapshot loaded nightly. One row per employee per run date.';


-- -----------------------------------------------------------------------------
-- RAW_TERMINATIONS
-- One row per termination event as returned by the Workday Terminations
-- RaaS report. This is an incremental entity — only new terminations since
-- the last successful run are appended per load.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RAW_HR.RAW.RAW_TERMINATIONS (

    -- Source fields — match the WorkdayClient termination dict keys exactly
    TERMINATION_ID              VARCHAR         COMMENT 'Unique identifier for the termination event',
    EMPLOYEE_ID                 VARCHAR         COMMENT 'FK to the terminated employee',
    TERMINATION_DATE            DATE            COMMENT 'Official last day per Workday',
    LAST_DAY_OF_WORK            DATE            COMMENT 'Actual final working day',
    TERMINATION_TYPE            VARCHAR         COMMENT 'Voluntary | Involuntary',
    TERMINATION_REASON          VARCHAR         COMMENT 'Standardised reason code (e.g. Resignation, Layoff)',
    TERMINATION_REASON_DETAIL   VARCHAR         COMMENT 'Free-text detail / sub-reason entered by HR',
    REHIRE_ELIGIBLE             BOOLEAN         COMMENT 'Whether the employee is eligible for rehire',
    RECORDED_BY                 VARCHAR         COMMENT 'HR rep who entered the termination in Workday',
    RECORDED_AT                 TIMESTAMP_TZ    COMMENT 'ISO timestamp the transaction was entered in Workday',

    -- Pipeline metadata — injected by s3_stager._to_parquet()
    _PIPELINE_ENTITY        VARCHAR         COMMENT 'Always "terminations" for this table',
    _PIPELINE_RUN_DATE      DATE            COMMENT 'Date the Lambda run executed',
    _PIPELINE_PAGE_NUM      INTEGER         COMMENT 'Zero-based page index within the run',
    _PIPELINE_WRITTEN_AT    TIMESTAMP_TZ    COMMENT 'UTC timestamp the S3 file was written',

    -- Snowflake-managed audit column
    _LOADED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
                            COMMENT 'UTC timestamp Snowpipe inserted this row'

)
CLUSTER BY (TERMINATION_DATE, TERMINATION_TYPE)
COMMENT = 'Raw termination records from Workday. Incrementally appended by Snowpipe. One row per termination event.';


-- -----------------------------------------------------------------------------
-- Grant select to analyst role so dbt can read from raw tables
-- -----------------------------------------------------------------------------

GRANT SELECT ON RAW_HR.RAW.RAW_EMPLOYEES    TO ROLE ANALYST_ROLE;
GRANT SELECT ON RAW_HR.RAW.RAW_TERMINATIONS TO ROLE ANALYST_ROLE;
