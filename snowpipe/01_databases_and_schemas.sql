-- =============================================================================
-- 01_databases_and_schemas.sql
-- Creates the database and schema hierarchy for the Workday HR pipeline.
--
-- Schema strategy:
--   RAW_HR.RAW          — exact copy of what landed in S3. No transformations.
--                         Owned by the ingestion pipeline. dbt reads from here
--                         but never writes to it.
--   PIPELINE_AUDIT.INGESTION — pipeline run records written by the Lambda.
--                         Separate database so audit data survives even if the
--                         HR database is dropped or recreated.
--
-- Run this script once as SYSADMIN before any other pipeline objects are created.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- HR Raw database
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS RAW_HR
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw HR data ingested from Workday via Lambda + Snowpipe. No transformations applied.';

CREATE SCHEMA IF NOT EXISTS RAW_HR.RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw employee and termination tables. Loaded by Snowpipe from S3 Parquet files.';


-- -----------------------------------------------------------------------------
-- Pipeline audit database
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS PIPELINE_AUDIT
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Operational metadata for all data pipelines. Tracks run history, record counts, and S3 keys.';

CREATE SCHEMA IF NOT EXISTS PIPELINE_AUDIT.INGESTION
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Run records written by the Workday Lambda ingestion pipeline.';


-- -----------------------------------------------------------------------------
-- Warehouse
-- A dedicated XS warehouse for ingestion loads and dbt runs.
-- Auto-suspend after 60 seconds of inactivity to control cost.
-- -----------------------------------------------------------------------------

CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Workday ingestion pipeline and dbt transformations.';


-- -----------------------------------------------------------------------------
-- Roles
-- Keep ingestion credentials separate from analyst credentials.
-- -----------------------------------------------------------------------------

-- Service role used by the Lambda Snowflake connector
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE
    COMMENT = 'Used by the Workday Lambda ingestion pipeline.';

-- Analyst role for dbt and BI tools
CREATE ROLE IF NOT EXISTS ANALYST_ROLE
    COMMENT = 'Read access to transformed HR tables for dbt and dashboards.';

-- Grant warehouse usage to both roles
GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE ANALYST_ROLE;

-- Grant pipeline role full access to raw and audit schemas
GRANT ALL PRIVILEGES ON DATABASE RAW_HR           TO ROLE PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON DATABASE PIPELINE_AUDIT   TO ROLE PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA RAW_HR.RAW         TO ROLE PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA PIPELINE_AUDIT.INGESTION TO ROLE PIPELINE_ROLE;

-- Grant analyst role read access to raw schema (dbt source layer)
GRANT USAGE  ON DATABASE RAW_HR           TO ROLE ANALYST_ROLE;
GRANT USAGE  ON SCHEMA   RAW_HR.RAW       TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_HR.RAW TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_HR.RAW TO ROLE ANALYST_ROLE;
