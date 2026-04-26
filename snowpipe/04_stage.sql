-- =============================================================================
-- 04_stage.sql
-- Creates the external S3 stage that points Snowflake at the data lake bucket.
--
-- What is a stage?
--   A Snowflake stage is a named pointer to an external storage location.
--   Snowpipe and COPY INTO both reference this stage object rather than
--   hardcoding S3 paths, so if the bucket or prefix ever changes, only
--   this object needs to be updated.
--
-- Authentication:
--   We use a Snowflake Storage Integration rather than embedding AWS credentials
--   directly in the stage. A Storage Integration creates a trust relationship
--   between the Snowflake account and an IAM role in AWS via an external ID,
--   which means:
--     - No AWS access keys stored in Snowflake (more secure)
--     - Credentials never expire (unlike access key rotation)
--     - AWS CloudTrail shows Snowflake as the actor, not a key holder
--
-- Setup sequence:
--   Step 1 — Create the storage integration (run as ACCOUNTADMIN)
--   Step 2 — Retrieve the IAM values Snowflake generated
--   Step 3 — Update the AWS IAM role trust policy with those values
--   Step 4 — Create the stage referencing the integration
-- =============================================================================

USE DATABASE RAW_HR;
USE SCHEMA   RAW_HR.RAW;


-- -----------------------------------------------------------------------------
-- Step 1: Storage Integration
-- Run as ACCOUNTADMIN. Creates the Snowflake-managed IAM principal.
-- Replace ALLOWED_LOCATIONS with your actual bucket and prefix.
-- -----------------------------------------------------------------------------

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_WORKDAY_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-workday-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://equipmentshare-data-lake/workday/raw/')
    COMMENT = 'Storage integration granting Snowflake read access to the Workday raw data prefix in S3.';


-- -----------------------------------------------------------------------------
-- Step 2: Retrieve Snowflake-generated IAM values
-- Run this query and copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.
-- You will need these to update the trust policy of your AWS IAM role.
-- -----------------------------------------------------------------------------

DESC INTEGRATION S3_WORKDAY_INTEGRATION;

/*
  Expected output fields:
    STORAGE_AWS_IAM_USER_ARN  — the AWS IAM user Snowflake created for this integration
                                e.g. arn:aws:iam::123456789:user/xxxx0000-s
    STORAGE_AWS_EXTERNAL_ID   — the external ID to include in the role trust policy
                                e.g. EQUIPMENTSHARE_SFCRole=3_xxxx=

  Use these values in your IAM role trust policy:
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
        },
        "Action": "sts:AssumeRole",
        "Condition": {
          "StringEquals": {
            "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
          }
        }
      }
    ]
  }

  The IAM role itself needs this S3 policy:
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:GetObjectVersion"],
        "Resource": "arn:aws:s3:::equipmentshare-data-lake/workday/raw/*"
      },
      {
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::equipmentshare-data-lake",
        "Condition": {
          "StringLike": {"s3:prefix": ["workday/raw/*"]}
        }
      }
    ]
  }
*/


-- -----------------------------------------------------------------------------
-- Step 3: External Stage
-- Run after the IAM trust policy has been updated with the values from Step 2.
-- The URL must point to the root prefix — Snowpipe will scan sub-paths.
-- -----------------------------------------------------------------------------

CREATE STAGE IF NOT EXISTS RAW_HR.RAW.S3_WORKDAY_STAGE
    STORAGE_INTEGRATION = S3_WORKDAY_INTEGRATION
    URL                 = 's3://equipmentshare-data-lake/workday/raw/'
    FILE_FORMAT         = RAW_HR.RAW.PARQUET_FORMAT
    COMMENT = 'External stage pointing to the Workday raw data prefix. Used by both Snowpipe pipes.';


-- -----------------------------------------------------------------------------
-- Verify the stage can list files
-- Run this after setup to confirm S3 connectivity is working.
-- -----------------------------------------------------------------------------

LIST @RAW_HR.RAW.S3_WORKDAY_STAGE;


-- -----------------------------------------------------------------------------
-- Grant stage usage to the pipeline role
-- -----------------------------------------------------------------------------

GRANT READ ON STAGE RAW_HR.RAW.S3_WORKDAY_STAGE TO ROLE PIPELINE_ROLE;
