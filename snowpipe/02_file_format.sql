-- =============================================================================
-- 02_file_format.sql
-- Defines the Parquet file format object Snowpipe uses to parse incoming files.
--
-- Why a named file format?
--   A named file format is reusable across the stage definition, both COPY INTO
--   statements, and both pipe objects. Changing compression or type handling
--   only requires updating this one object rather than every pipe.
--
-- Parquet-specific settings:
--   SNAPPY_COMPRESSION    matches the compression set in s3_stager._to_parquet()
--   BINARY_AS_TEXT = FALSE tells Snowflake to interpret Parquet BINARY columns
--                          as raw bytes rather than VARCHAR — avoids silent
--                          misinterpretation of binary fields if any are added.
--   USE_LOGICAL_TYPE = TRUE tells Snowflake to honour Parquet logical type
--                           annotations (DATE, TIMESTAMP, BOOLEAN) rather than
--                           falling back to raw physical types. This is what
--                           allows DATE columns from PyArrow to land as DATE
--                           in Snowflake rather than INT32.
-- =============================================================================

USE DATABASE RAW_HR;
USE SCHEMA   RAW_HR.RAW;


CREATE FILE FORMAT IF NOT EXISTS RAW_HR.RAW.PARQUET_FORMAT
    TYPE                = 'PARQUET'
    SNAPPY_COMPRESSION  = TRUE
    BINARY_AS_TEXT      = FALSE
    USE_LOGICAL_TYPE    = TRUE
    COMMENT = 'Parquet file format for Workday raw data files staged from S3 by the Lambda ingestion pipeline.';
