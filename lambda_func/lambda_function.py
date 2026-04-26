"""
lambda_function.py
------------------
AWS Lambda entry point for the Workday → S3 ingestion pipeline.

Execution flow:
  1. EventBridge triggers this handler on a nightly schedule
  2. Credentials are fetched from Secrets Manager (Workday + Snowflake)
  3. SnowflakeStateManager queries the audit table for the last successful
     run date — the single source of truth for incremental state
  4. WorkdayClient yields records one page at a time via paginated generators
  5. S3Stager writes each page to its own NDJSON file in S3 immediately,
     keeping memory usage flat regardless of total dataset size
  6. SnowflakeStateManager writes one audit record per entity summarising
     the full run (total records, all S3 keys staged)
  7. Any failure is logged and re-raised so Lambda marks the invocation
     as FAILED, triggering CloudWatch Alarms

Pagination strategy:
  Instead of loading all records into memory before writing to S3, the handler
  iterates the paginated generators page by page. Each page is staged to S3
  immediately after it is received, so:
    - Memory is bounded to PAGE_SIZE records at any moment (default 500)
    - Partial progress is preserved in S3 if Lambda times out mid-run
    - No single API call can time out due to a massive response payload

Environment variables:
  WORKDAY_BASE_URL          Base URL of the Mock (or real) Workday API
  WORKDAY_TENANT            Workday tenant identifier
  WORKDAY_SECRET_ARN        ARN of Secrets Manager secret — Workday credentials
  SNOWFLAKE_SECRET_ARN      ARN of Secrets Manager secret — Snowflake credentials
  S3_BUCKET                 Target S3 bucket name
  S3_PREFIX                 Root prefix inside the bucket  (e.g. workday/raw)
"""

import json
import logging
import os
import traceback
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from workday_client import WorkdayClient
from s3_stager import S3Stager
from snowflake_state_manager import SnowflakeStateManager
from secrets_helper import get_workday_credentials, get_snowflake_credentials

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Config — read once at cold start, not inside the handler
# ---------------------------------------------------------------------------

WORKDAY_BASE_URL     = os.environ["WORKDAY_BASE_URL"]
WORKDAY_TENANT       = os.environ["WORKDAY_TENANT"]
WORKDAY_SECRET_ARN   = os.environ["WORKDAY_SECRET_ARN"]
SNOWFLAKE_SECRET_ARN = os.environ["SNOWFLAKE_SECRET_ARN"]
S3_BUCKET            = os.environ["S3_BUCKET"]
S3_PREFIX            = os.environ.get("S3_PREFIX", "workday/raw")


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def handler(event: dict, context) -> dict:
    """
    Main Lambda handler.

    Accepts an optional override in the EventBridge payload:
      {
        "start_date": "2024-01-01",   # override incremental window start
        "end_date":   "2024-03-31",   # override incremental window end
        "full_load":  true            # ignore state, pull all records
      }

    Returns a summary dict logged to CloudWatch and readable by Step Functions.
    """
    logger.info("Workday ingestion Lambda started.")
    logger.info("Event payload: %s", json.dumps(event))

    run_date = date.today()
    state: Optional[SnowflakeStateManager] = None

    try:
        # ----------------------------------------------------------------
        # 1. Resolve credentials from Secrets Manager
        # ----------------------------------------------------------------
        logger.info("Fetching credentials from Secrets Manager.")
        workday_creds   = get_workday_credentials(WORKDAY_SECRET_ARN)
        snowflake_creds = get_snowflake_credentials(SNOWFLAKE_SECRET_ARN)

        # ----------------------------------------------------------------
        # 2. Initialise clients
        # ----------------------------------------------------------------
        workday = WorkdayClient(
            base_url=WORKDAY_BASE_URL,
            tenant=WORKDAY_TENANT,
            username=workday_creds["username"],
            password=workday_creds["password"],
        )

        stager = S3Stager(bucket=S3_BUCKET, prefix=S3_PREFIX)

        state = SnowflakeStateManager(
            account=snowflake_creds["account"],
            user=snowflake_creds["user"],
            password=snowflake_creds["password"],
            warehouse=snowflake_creds["warehouse"],
            database=snowflake_creds["database"],
            schema=snowflake_creds["schema"],
            role=snowflake_creds.get("role"),
        )

        # ----------------------------------------------------------------
        # 3. Resolve incremental date window from Snowflake audit table
        # ----------------------------------------------------------------
        full_load  = event.get("full_load", False)
        start_date = _resolve_start_date(event, state, full_load)
        end_date   = _resolve_end_date(event, run_date)

        logger.info(
            "Incremental window: %s → %s  (full_load=%s)",
            start_date, end_date, full_load,
        )

        # ----------------------------------------------------------------
        # 4. Extract and stage employees — paginated
        # ----------------------------------------------------------------
        emp_started_at  = datetime.now(timezone.utc)
        emp_keys, emp_total = _stage_paginated(
            pages=workday.get_employees_paginated(status="All"),
            entity="employees",
            run_date=run_date,
            stager=stager,
        )
        logger.info(
            "Employees complete: %d record(s) across %d page(s).",
            emp_total, len(emp_keys),
        )

        state.record_run(
            entity="employees",
            run_date=run_date,
            records_staged=emp_total,
            s3_key=json.dumps(emp_keys),   # store all page keys as a JSON list
            status="SUCCESS",
            started_at=emp_started_at,
        )

        # ----------------------------------------------------------------
        # 5. Extract and stage terminations — paginated, incremental
        # ----------------------------------------------------------------
        term_started_at = datetime.now(timezone.utc)
        term_keys, term_total = _stage_paginated(
            pages=workday.get_terminations_paginated(
                start_date=start_date,
                end_date=end_date,
            ),
            entity="terminations",
            run_date=run_date,
            stager=stager,
        )
        logger.info(
            "Terminations complete: %d record(s) across %d page(s).",
            term_total, len(term_keys),
        )

        state.record_run(
            entity="terminations",
            run_date=run_date,
            records_staged=term_total,
            s3_key=json.dumps(term_keys),
            status="SUCCESS",
            started_at=term_started_at,
        )

        # ----------------------------------------------------------------
        # 6. Return summary
        # ----------------------------------------------------------------
        summary = {
            "status":              "SUCCESS",
            "run_date":            str(run_date),
            "incremental_window":  {"start": str(start_date), "end": str(end_date)},
            "employees": {
                "total_records": emp_total,
                "pages_staged":  len(emp_keys),
                "s3_keys":       emp_keys,
            },
            "terminations": {
                "total_records": term_total,
                "pages_staged":  len(term_keys),
                "s3_keys":       term_keys,
            },
        }
        logger.info("Run summary: %s", json.dumps(summary))
        return summary

    except Exception as exc:
        if state:
            try:
                state.record_run(
                    entity="pipeline",
                    run_date=run_date,
                    records_staged=0,
                    s3_key=None,
                    status="FAILED",
                    started_at=datetime.now(timezone.utc),
                    error_message=str(exc),
                )
            except Exception as audit_exc:
                logger.error("Could not write FAILED audit record: %s", audit_exc)

        logger.error("Ingestion pipeline failed: %s", exc)
        logger.error(traceback.format_exc())
        raise

    finally:
        if state:
            state.close()


# ---------------------------------------------------------------------------
# Private Helpers
# ---------------------------------------------------------------------------

def _stage_paginated(
    pages,
    entity:   str,
    run_date: date,
    stager:   S3Stager,
) -> tuple[list[str], int]:
    """
    Iterate a paginated generator, staging each page to S3 immediately.

    This is the core of the timeout/memory fix:
      - Each page is processed and uploaded as soon as it arrives from the API
      - Memory never holds more than one page of records at a time
      - Partial progress is durable in S3 if the Lambda is killed mid-run

    Args:
        pages:    A generator yielding list[dict] — one page per iteration.
        entity:   "employees" | "terminations" — used in S3 key and logging.
        run_date: Today's date, used for S3 partitioning.
        stager:   S3Stager instance to write each page.

    Returns:
        A tuple of:
          - s3_keys: list of S3 keys written, one per page
          - total:   total record count across all pages
    """
    s3_keys = []
    total   = 0

    for page_num, page in enumerate(pages):
        page_count = len(page)
        total     += page_count

        logger.info(
            "Staging %s page %d: %d record(s) (running total: %d).",
            entity, page_num, page_count, total,
        )

        key = stager.stage(
            data=page,
            entity=entity,
            run_date=run_date,
            page_num=page_num,
        )

        if key:
            s3_keys.append(key)

    if not s3_keys:
        logger.warning(
            "No pages staged for entity '%s' on %s. "
            "This may indicate no new records in the incremental window.",
            entity, run_date,
        )

    return s3_keys, total


def _resolve_start_date(
    event:     dict,
    state:     SnowflakeStateManager,
    full_load: bool,
) -> Optional[date]:
    """
    Priority order for start_date:
      1. Explicit override in EventBridge payload  ("start_date" key)
      2. full_load flag → no start_date filter (return None)
      3. Last successful run date from Snowflake audit table + 1 day
      4. Fallback: 30 days ago  (first-ever run, no audit records yet)
    """
    if "start_date" in event:
        return date.fromisoformat(event["start_date"])

    if full_load:
        logger.info("full_load=True. No start_date filter applied.")
        return None

    last_date = state.get_last_loaded_date(entity="terminations")

    if last_date:
        start = last_date + timedelta(days=1)
        logger.info("Incremental start_date derived from Snowflake: %s", start)
        return start

    fallback = date.today() - timedelta(days=30)
    logger.warning(
        "No prior successful runs in Snowflake audit table. "
        "Defaulting to 30-day lookback: %s",
        fallback,
    )
    return fallback


def _resolve_end_date(event: dict, run_date: date) -> date:
    """
    Priority order for end_date:
      1. Explicit override in EventBridge payload  ("end_date" key)
      2. Today's date  (default)
    """
    if "end_date" in event:
        return date.fromisoformat(event["end_date"])
    return run_date
