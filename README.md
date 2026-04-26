# Workday → Snowflake HR Turnover Pipeline

A production-grade data engineering pipeline that extracts employee and termination data from the Workday RaaS API, stages it in Amazon S3 as Parquet, auto-loads it into Snowflake via Snowpipe, and transforms it into dashboard-ready 12-month rolling turnover metrics using dbt.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Component Guide](#component-guide)
  - [Mock Workday API](#1-mock-workday-api)
  - [Lambda Ingestion Pipeline](#2-lambda-ingestion-pipeline)
  - [Snowflake Setup](#3-snowflake-setup)
  - [dbt Transformation Layer](#4-dbt-transformation-layer)
- [Data Flow](#data-flow)
- [Incremental Load Strategy](#incremental-load-strategy)
- [Data Quality](#data-quality)
- [Environment Variables](#environment-variables)
- [IAM Permissions](#iam-permissions)
- [Deployment](#deployment)
- [Dashboard Output](#dashboard-output)
- [Project Timeline](#project-timeline)

---

## Overview

EquipmentShare's HR Analytics team needed visibility into employee turnover trends — 12-month rolling rates, voluntary vs. involuntary breakdowns, and geographic differences by state — but no termination data existed in Snowflake and no pipeline existed to bring it there.

This project delivers:

- A **Mock Workday API** (FastAPI) that simulates Workday's Reports-as-a-Service (RaaS) endpoints for development and testing without live credentials
- A **Lambda ingestion pipeline** that extracts employees and terminations incrementally, paginates large datasets, and stages Parquet files to S3
- A **Snowflake raw layer** with Snowpipe auto-ingestion triggered by S3 event notifications
- A **dbt project** with staging, dimension, fact, and rolling turnover models across two layers — plus a comprehensive test suite
- A **Snowflake audit table** that serves as the single source of truth for incremental pipeline state

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│  EventBridge (nightly cron)                                                     │
│       │                                                                         │
│       ▼                                                                         │
│  AWS Lambda ──► Secrets Manager (Workday + Snowflake credentials)               │
│       │                                                                         │
│       ├──► Snowflake Audit Table ◄── get last successful run date               │
│       │                                                                         │
│       ├──► Workday RaaS API (paginated, 500 records/page)                       │
│       │         │                                                               │
│       │         ▼                                                               │
│       │    s3://bucket/workday/raw/                                             │
│       │      ├── employees/year=YYYY/month=MM/day=DD/*.parquet                  │
│       │      └── terminations/year=YYYY/month=MM/day=DD/*.parquet               │
│       │                   │                                                     │
│       │             S3 ObjectCreated event                                      │
│       │                   │                                                     │
│       │                   ▼                                                     │
│       │            Snowpipe (SQS)                                               │
│       │                   │                                                     │
│       │                   ▼                                                     │
│       │         RAW_HR.RAW.RAW_EMPLOYEES                                        │
│       │         RAW_HR.RAW.RAW_TERMINATIONS                                     │
│       │                   │                                                     │
│       │                   ▼                                                     │
│       │              dbt run                                                    │
│       │         ┌─────────────────────────────────────────────┐                │
│       │         │  staging  →  dim_employees                  │                │
│       │         │           →  fct_terminations               │                │
│       │         │           →  fct_turnover_rolling_12m       │                │
│       │         └─────────────────────────────────────────────┘                │
│       │                   │                                                     │
│       └──► Snowflake Audit Table ◄── write SUCCESS / FAILED record             │
│                           │                                                     │
│                           ▼                                                     │
│                     Dashboard                                                   │
│              (12-month rolling turnover by state)                               │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
.
├── workday_mock_api/                  # Mock Workday RaaS API (FastAPI)
│   ├── main.py                        # API routes, auth, validation
│   ├── fake_data.py                   # Seed employee + termination records
│   ├── requirements.txt
│   └── README.md
│
├── workday_lambda/                    # AWS Lambda ingestion pipeline
│   ├── lambda_function.py             # Handler — orchestrates the full run
│   ├── workday_client.py              # Paginated HTTP client for Workday API
│   ├── s3_stager.py                   # Parquet serialisation + S3 upload
│   ├── snowflake_state_manager.py     # Reads/writes run state via Snowflake audit table
│   ├── secrets_helper.py             # Fetches credentials from Secrets Manager
│   ├── requirements.txt
│   └── README.md
│
├── snowflake_setup/                   # Snowflake DDL — run in order
│   ├── 01_databases_and_schemas.sql   # Databases, schemas, roles, warehouse
│   ├── 02_file_format.sql             # Parquet file format object
│   ├── 03_raw_tables.sql              # RAW_EMPLOYEES + RAW_TERMINATIONS DDL
│   ├── 04_stage.sql                   # External S3 stage + storage integration
│   ├── 05_snowpipe.sql                # PIPE_EMPLOYEES + PIPE_TERMINATIONS
│   └── 06_audit_table.sql             # PIPELINE_AUDIT.INGESTION.RUNS DDL
│
├── dbt_hr/                            # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   │   └── generic_tests.sql          # not_in_future, not_before_column, is_between
│   ├── models/
│   │   ├── staging/workday/
│   │   │   ├── _workday__sources.yml  # Source definitions + freshness checks
│   │   │   ├── _workday__models.yml   # Column docs + schema tests
│   │   │   ├── stg_workday__employees.sql
│   │   │   └── stg_workday__terminations.sql
│   │   └── marts/hr/
│   │       ├── _hr__models.yml        # Column docs + schema tests
│   │       ├── dim_employees.sql
│   │       ├── fct_terminations.sql
│   │       └── fct_turnover_rolling_12m.sql
│   └── tests/
│       ├── assert_pipeline_ran_recently.sql
│       ├── staging/
│       │   ├── assert_termination_after_hire_date.sql
│       │   ├── assert_no_future_termination_dates.sql
│       │   ├── assert_last_day_of_work_lte_termination_date.sql
│       │   └── assert_active_employees_have_no_termination.sql
│       └── marts/
│           ├── assert_turnover_rate_in_bounds.sql
│           ├── assert_headcount_never_negative.sql
│           ├── assert_regrettable_subset_of_voluntary.sql
│           ├── assert_termination_type_counts_sum_to_total.sql
│           └── assert_date_spine_is_contiguous.sql
│
└── README.md                          # This file
```

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11+ | Lambda + Mock API |
| Node.js | 18+ | (optional) PPTX generation script |
| dbt-snowflake | 1.7+ | dbt transformation layer |
| AWS CLI | 2.x | Deployment + S3 event notification setup |
| Snowflake account | — | Raw tables, Snowpipe, audit table |

---

## Quick Start

### 1. Start the Mock Workday API locally

```bash
cd workday_mock_api
pip install -r requirements.txt
python main.py
# API running at http://localhost:8000
# Swagger docs at http://localhost:8000/docs
```

Test it:
```bash
curl -u workday_svc_user:mock_password_123 \
  "http://localhost:8000/ccx/service/customreport2/equipmentshare/HR_Analytics/Terminations"
```

### 2. Set up Snowflake

Run the DDL scripts in order against your Snowflake account:

```sql
-- In Snowflake worksheet, run scripts 01 through 06 in order
-- See snowflake_setup/ for full instructions including IAM setup for the S3 stage
```

### 3. Run the Lambda locally

```bash
cd workday_lambda
pip install -r requirements.txt

# Set environment variables
export WORKDAY_BASE_URL=http://localhost:8000
export WORKDAY_TENANT=equipmentshare
export WORKDAY_SECRET_ARN=arn:aws:secretsmanager:...
export SNOWFLAKE_SECRET_ARN=arn:aws:secretsmanager:...
export S3_BUCKET=your-data-lake-bucket
export S3_PREFIX=workday/raw

# Invoke the handler directly (requires AWS credentials for S3 + Secrets Manager)
python -c "from lambda_function import handler; print(handler({'full_load': True}, None))"
```

### 4. Run dbt

```bash
cd dbt_hr
pip install dbt-snowflake
dbt deps               # install dbt-utils
dbt source freshness   # check raw table freshness
dbt run                # build all models
dbt test               # run all schema + singular tests
dbt docs generate && dbt docs serve  # browse lineage graph
```

---

## Component Guide

### 1. Mock Workday API

A FastAPI application that simulates Workday's RaaS (Reports-as-a-Service) URL pattern:

```
/ccx/service/customreport2/{tenant}/HR_Analytics/{report_name}
```

**Auth:** HTTP Basic Auth (mirrors Workday ISU authentication)

**Key endpoints:**

| Endpoint | Query Params | Description |
|---|---|---|
| `GET /health` | — | Liveness probe |
| `GET /.../Active_Employees` | `status`, `limit`, `offset` | All employees, paginated |
| `GET /.../Terminations` | `start_date`, `end_date`, `termination_type`, `limit`, `offset` | Terminations, paginated + incremental |

**Validation built in:**
- Unknown `status` or `termination_type` values → `422`
- `start_date` after `end_date` → `422`
- Future `start_date` or `end_date` → `422`

See [`workday_mock_api/README.md`](workday_mock_api/README.md) for full documentation.

---

### 2. Lambda Ingestion Pipeline

Five single-responsibility modules:

| File | Responsibility |
|---|---|
| `lambda_function.py` | Orchestrator — resolves date window, calls all modules, writes audit records |
| `workday_client.py` | Paginated HTTP client — yields 500 records at a time via Python generators |
| `s3_stager.py` | Parquet writer — converts pages to typed Parquet via PyArrow, uploads to S3 |
| `snowflake_state_manager.py` | State — queries audit table for last run date, writes run records |
| `secrets_helper.py` | Credentials — fetches from Secrets Manager, module-level caching |

**Pagination strategy:** `get_terminations_paginated()` and `get_employees_paginated()` are Python generators that yield one page at a time. Memory is bounded to 500 records regardless of total dataset size. Each page is staged to its own S3 file immediately, preserving partial progress if Lambda times out.

**S3 output format:**
```
s3://bucket/workday/raw/terminations/year=2024/month=06/day=30/20240630T020001Z_p000.parquet
```

See [`workday_lambda/README.md`](workday_lambda/README.md) for full documentation.

---

### 3. Snowflake Setup

Six DDL scripts to run once, in order:

| Script | What it creates |
|---|---|
| `01_databases_and_schemas.sql` | `RAW_HR`, `PIPELINE_AUDIT` databases, schemas, `INGESTION_WH`, `PIPELINE_ROLE`, `ANALYST_ROLE` |
| `02_file_format.sql` | `PARQUET_FORMAT` — Snappy compression, logical type annotations enabled |
| `03_raw_tables.sql` | `RAW_EMPLOYEES` and `RAW_TERMINATIONS` with typed columns + pipeline metadata columns |
| `04_stage.sql` | External S3 stage via Storage Integration (no hardcoded credentials) |
| `05_snowpipe.sql` | `PIPE_EMPLOYEES` and `PIPE_TERMINATIONS` with `AUTO_INGEST = TRUE` |
| `06_audit_table.sql` | `PIPELINE_AUDIT.INGESTION.RUNS` — state tracking + operations log |

**Important:** After running `05_snowpipe.sql`, run `SHOW PIPES` to retrieve the SQS ARNs, then configure S3 event notifications to point at those queues. Instructions are included as comments in the script.

---

### 4. dbt Transformation Layer

**Model lineage:**

```
RAW_EMPLOYEES    → stg_workday__employees    → dim_employees ──────────────────┐
                                                                                ├─► fct_turnover_rolling_12m
RAW_TERMINATIONS → stg_workday__terminations → fct_terminations ────────────────┘
```

**Key models:**

| Model | Materialisation | Description |
|---|---|---|
| `stg_workday__employees` | View | Deduplicated, typed, standardised employee records |
| `stg_workday__terminations` | View | Deduplicated termination events with derived fields |
| `dim_employees` | Table | Current-state employee dimension with tenure buckets and manager flag |
| `fct_terminations` | Table | Termination facts joined to employee context at exit |
| `fct_turnover_rolling_12m` | Table | 12-month rolling turnover rate by month and state |

**Rolling turnover formula:**

```
Rolling Turnover % =
  SUM(terminations in trailing 12 months)
  ─────────────────────────────────────── × 100
  AVG(end-of-month headcount over 12 months)
```

Computed in SQL using `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW` window functions, partitioned by `location_state`.

---

## Data Flow

```
Workday API (paginated)
    │
    │  JSON records, 500/page
    ▼
Lambda s3_stager.py
    │
    │  Parquet, Snappy compressed
    │  Typed columns (DATE, BOOLEAN, VARCHAR)
    │  4 _pipeline_ metadata columns injected
    ▼
S3: workday/raw/{entity}/year=YYYY/month=MM/day=DD/{ts}_p{N}.parquet
    │
    │  S3 ObjectCreated → SQS → Snowpipe
    ▼
RAW_HR.RAW.RAW_TERMINATIONS (typed columns, _LOADED_AT DEFAULT CURRENT_TIMESTAMP())
    │
    │  dbt staging (deduplicate, standardise, derive fields)
    ▼
RAW_HR.STAGING.STG_WORKDAY__TERMINATIONS
    │
    │  dbt marts (join, enrich, aggregate)
    ▼
RAW_HR.MARTS.FCT_TURNOVER_ROLLING_12M
    │
    │  BI tool query
    ▼
HR Turnover Dashboard
```

---

## Incremental Load Strategy

The pipeline uses the Snowflake audit table — not SSM or an external state store — as the single source of truth for incremental state.

**On each Lambda run:**

1. Query `PIPELINE_AUDIT.INGESTION.RUNS` for `MAX(run_date) WHERE entity = 'terminations' AND status = 'SUCCESS'`
2. Set `start_date = last_run_date + 1 day`
3. Set `end_date = today`
4. Extract only terminations within that window
5. After successful staging, write a `SUCCESS` audit record

**Why Snowflake as state?** If Snowpipe fails silently and no records land in Snowflake, the audit table has no `SUCCESS` record for that date. The next run automatically re-pulls the missing window — self-healing without manual intervention.

**Override options** (via EventBridge payload):

```json
{ "full_load": true }                          // Pull all records, ignore state
{ "start_date": "2024-01-01",
  "end_date":   "2024-03-31" }                 // Custom date window
```

---

## Data Quality

Tests are applied at three layers with two severity levels.

### Schema tests (YAML-declared)

Applied to every model via `_workday__models.yml` and `_hr__models.yml`:

- `unique` + `not_null` on all primary keys
- `accepted_values` on `employment_status`, `worker_type`, `termination_type`, `termination_reason`, `tenure_bucket`
- `relationships` — every termination `employee_key` must exist in `dim_employees`
- Custom generic tests: `not_in_future`, `not_before_column`, `is_between`

### Singular tests

| Test | Layer | Severity | What It Catches |
|---|---|---|---|
| `assert_termination_after_hire_date` | Staging | error | Termination date before hire date |
| `assert_no_future_termination_dates` | Staging | error | Future termination dates inflating current metrics |
| `assert_last_day_of_work_lte_termination_date` | Staging | error | Last day after official termination date |
| `assert_active_employees_have_no_termination` | Staging | warn | HR data entry lag — employee active and terminated simultaneously |
| `assert_headcount_never_negative` | Marts | error | Negative denominator corrupts all turnover rates |
| `assert_regrettable_subset_of_voluntary` | Marts | error | Regrettable count exceeds voluntary count — logic bug |
| `assert_termination_type_counts_sum_to_total` | Marts | error | Voluntary + involuntary ≠ total |
| `assert_date_spine_is_contiguous` | Marts | error | Gap in monthly spine — rolling window skips a period |
| `assert_turnover_rate_in_bounds` | Marts | warn | Rate above 150% — almost always a calculation error |
| `assert_pipeline_ran_recently` | Cross-layer | error | Lambda has not run successfully in 25 hours |

### Source freshness

Defined in `_workday__sources.yml`:
- Warn after 25 hours without new data
- Error after 48 hours

---

## Environment Variables

### Lambda

| Variable | Description | Example |
|---|---|---|
| `WORKDAY_BASE_URL` | Base URL of the Workday or Mock API | `http://localhost:8000` |
| `WORKDAY_TENANT` | Workday tenant identifier | `equipmentshare` |
| `WORKDAY_SECRET_ARN` | Secrets Manager ARN — Workday credentials | `arn:aws:secretsmanager:...` |
| `SNOWFLAKE_SECRET_ARN` | Secrets Manager ARN — Snowflake credentials | `arn:aws:secretsmanager:...` |
| `S3_BUCKET` | Target S3 bucket | `equipmentshare-data-lake` |
| `S3_PREFIX` | Root prefix inside the bucket | `workday/raw` |

### Secrets Manager — Workday secret

```json
{
  "username": "workday_svc_user",
  "password": "your_password"
}
```

### Secrets Manager — Snowflake secret

```json
{
  "account":   "xy12345.us-east-1",
  "user":      "pipeline_svc_user",
  "password":  "your_password",
  "warehouse": "INGESTION_WH",
  "database":  "pipeline_audit",
  "schema":    "ingestion",
  "role":      "PIPELINE_ROLE"
}
```

### dbt (profiles.yml)

```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=analyst_user
export SNOWFLAKE_PASSWORD=your_password
```

---

## IAM Permissions

Minimum permissions required on the Lambda execution role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::equipmentshare-data-lake/workday/*"
    },
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": [
        "arn:aws:secretsmanager:us-east-1:*:secret:workday*",
        "arn:aws:secretsmanager:us-east-1:*:secret:snowflake*"
      ]
    }
  ]
}
```

The S3 stage uses a Snowflake Storage Integration (IAM role + external ID trust relationship). See `snowflake_setup/04_stage.sql` for the exact trust policy and S3 bucket policy required.

---

## Deployment

### Lambda

```bash
cd workday_lambda

# Install dependencies into package directory
pip install -r requirements.txt -t ./package/

# Copy source files
cp *.py ./package/

# Create zip
cd package && zip -r ../workday_ingestion.zip . && cd ..

# Deploy
aws lambda update-function-code \
  --function-name workday-ingestion \
  --zip-file fileb://workday_ingestion.zip

# Set environment variables
aws lambda update-function-configuration \
  --function-name workday-ingestion \
  --environment "Variables={
    WORKDAY_BASE_URL=https://your-workday-tenant.workday.com,
    WORKDAY_TENANT=equipmentshare,
    WORKDAY_SECRET_ARN=arn:aws:secretsmanager:...,
    SNOWFLAKE_SECRET_ARN=arn:aws:secretsmanager:...,
    S3_BUCKET=equipmentshare-data-lake,
    S3_PREFIX=workday/raw
  }"
```

### EventBridge schedule

```bash
# Trigger nightly at 2am UTC
aws events put-rule \
  --name workday-ingestion-nightly \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED

aws events put-targets \
  --rule workday-ingestion-nightly \
  --targets "Id=lambda,Arn=arn:aws:lambda:us-east-1:ACCOUNT:function:workday-ingestion"
```

### dbt Cloud (production)

Configure a dbt Cloud job with:
- **Commands:** `dbt source freshness`, `dbt run`, `dbt test`
- **Schedule:** Daily, after the Lambda window (e.g. 3am UTC)
- **Target:** `prod`

---

## Dashboard Output

`fct_turnover_rolling_12m` is the primary model for the HR turnover dashboard. One row per month per state, containing:

| Metric | Description |
|---|---|
| `rolling_12m_turnover_rate` | Total turnover % over trailing 12 months |
| `rolling_12m_voluntary_rate` | Voluntary exits as % of avg headcount |
| `rolling_12m_involuntary_rate` | Involuntary exits as % of avg headcount |
| `rolling_12m_regrettable_rate` | Voluntary exits with 2+ years tenure as % |
| `eom_headcount` | End-of-month active employee count |
| `is_partial_window` | True if fewer than 12 months of history available |

`fct_terminations` supports drill-through from the dashboard into individual exits with full employee context: department, location, job title, tenure at exit, and reason detail.

---

## Project Timeline

| Phase | Weeks | Deliverables |
|---|---|---|
| 1 — Foundation | 1–2 | Mock API, Lambda script, S3 Parquet staging, Secrets Manager |
| 2 — Raw Layer | 2–3 | Snowflake schemas, file format, stage, raw tables, Snowpipe |
| 3 — dbt Models | 3–4 | Staging models, dim/fct marts, rolling turnover model, tests |
| 4 — Production | 4–5 | EventBridge schedule, CloudWatch alarms, dbt Cloud, sign-off |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source system | Workday RaaS API |
| Mock API | Python · FastAPI · Uvicorn |
| Orchestration | AWS EventBridge |
| Compute | AWS Lambda |
| Credentials | AWS Secrets Manager |
| Storage | Amazon S3 (Parquet, Hive-partitioned) |
| Auto-ingestion | Snowflake Snowpipe + SQS |
| Data warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Pipeline state | Snowflake audit table |
| Serialisation | Pandas · PyArrow (Snappy Parquet) |
