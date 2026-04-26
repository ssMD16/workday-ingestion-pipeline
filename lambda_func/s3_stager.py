"""
s3_stager.py
------------
Writes raw API payloads to S3 as columnar Parquet files.

Why Parquet over NDJSON for this pipeline:
  - Workday's employee and termination schemas are stable and well-known,
    so schema-on-read flexibility is not needed here.
  - Parquet stores data in typed columns rather than raw JSON strings, so
    Snowflake's COPY INTO maps directly to named columns with no VARIANT
    parsing or casting required.
  - Parquet is compressed by default (Snappy), making files significantly
    smaller than equivalent NDJSON — important at scale.
  - Date and boolean fields are stored as native Parquet types, eliminating
    the string-to-type casting needed when loading from JSON.
  - dbt staging models require no flattening step — columns already exist
    as proper SQL types when the data arrives in Snowflake.

File layout:
  s3://{bucket}/{prefix}/{entity}/year=YYYY/month=MM/day=DD/{timestamp}_p{N}.parquet

  Hive-style date partitioning means Snowflake, Athena, and Glue can all
  prune partitions efficiently without scanning the full bucket.
  The _p{N} page suffix ensures paginated batches never overwrite each other.

Pipeline metadata columns:
  Every Parquet file includes three additional columns injected by the stager
  (not present in the source API response):
    _pipeline_entity      varchar  — "employees" | "terminations"
    _pipeline_run_date    date     — the date this Lambda run executed
    _pipeline_page_num    int      — zero-based page index within this run
    _pipeline_written_at  timestamp — UTC timestamp of the S3 write

  These columns let Snowflake raw tables self-document their origin without
  needing a separate metadata file or VARIANT envelope.
"""

import io
import logging
from datetime import date, datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3Stager
# ---------------------------------------------------------------------------

class S3Stager:
    """
    Stages raw API payloads to S3 as partitioned Parquet files.

    Paginated usage (one call per page from the WorkdayClient generator):
        stager = S3Stager(bucket="my-data-lake", prefix="workday/raw")
        for page_num, page in enumerate(workday.get_terminations_paginated(...)):
            s3_key = stager.stage(
                data=page,
                entity="terminations",
                run_date=date.today(),
                page_num=page_num,
            )

    Single-call usage (non-paginated, small datasets):
        s3_key = stager.stage(data=records, entity="employees", run_date=date.today())
    """

    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._s3    = boto3.client("s3")

    # -----------------------------------------------------------------------
    # Public
    # -----------------------------------------------------------------------

    def stage(
        self,
        data:     list[dict],
        entity:   str,
        run_date: date,
        page_num: int = 0,
    ) -> str:
        """
        Convert a list of API records to Parquet and upload to S3.

        Args:
            data:     List of dicts for this page returned by WorkdayClient.
            entity:   "employees" | "terminations" — used in S3 key + metadata column.
            run_date: The date of this pipeline run — used for S3 partitioning.
            page_num: Zero-based page index. Included in the S3 filename so each
                      paginated batch produces a distinct, non-conflicting file.
                      Defaults to 0 for non-paginated single-call usage.

        Returns:
            The S3 key the file was written to, or None if data was empty.

        Raises:
            S3StageError: if the Parquet conversion or S3 upload fails.
        """
        if not data:
            logger.warning(
                "No records to stage for entity '%s' page %d on %s. "
                "Skipping S3 write.",
                entity, page_num, run_date,
            )
            return None

        s3_key     = self._build_key(entity, run_date, page_num)
        parquet_bytes = self._to_parquet(data, entity, run_date, page_num)

        logger.info(
            "Staging %d record(s) [page %d] as Parquet (%d bytes) → s3://%s/%s",
            len(data), page_num, len(parquet_bytes), self.bucket, s3_key,
        )

        self._upload(s3_key, parquet_bytes)
        return s3_key

    # -----------------------------------------------------------------------
    # Private
    # -----------------------------------------------------------------------

    def _build_key(self, entity: str, run_date: date, page_num: int) -> str:
        """
        Build a Hive-style partitioned S3 key with a page suffix.

        Examples:
          workday/raw/employees/year=2024/month=06/day=30/20240630T020000Z_p000.parquet
          workday/raw/terminations/year=2024/month=06/day=30/20240630T020001Z_p003.parquet

        Zero-padded page number (3 digits) ensures lexicographic sort in S3
        matches numeric order for up to 999 pages per run.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return (
            f"{self.prefix}/{entity}"
            f"/year={run_date.year}"
            f"/month={run_date.month:02d}"
            f"/day={run_date.day:02d}"
            f"/{timestamp}_p{page_num:03d}.parquet"
        )

    @staticmethod
    def _to_parquet(
        data:     list[dict],
        entity:   str,
        run_date: date,
        page_num: int,
    ) -> bytes:
        """
        Convert a list of dicts to a Parquet byte stream.

        Steps:
          1. Build a Pandas DataFrame from the raw API records.
          2. Inject four pipeline metadata columns.
          3. Serialise to Parquet in memory using PyArrow (Snappy compression).
          4. Return the raw bytes — ready to upload directly to S3.

        Metadata columns are injected here rather than in the Lambda handler
        so they are guaranteed to be present in every file regardless of how
        stage() is called.

        Type coercions applied:
          - date strings ("YYYY-MM-DD") → pd.Timestamp (stored as Parquet DATE)
          - bool fields remain bool
          - string fields remain object (stored as Parquet STRING)
          - Parquet natively handles None/NaN as nulls
        """
        written_at = datetime.now(timezone.utc)

        df = pd.DataFrame(data)

        # Coerce known date columns to proper date types so Snowflake receives
        # them as DATE rather than VARCHAR — avoids casting in dbt staging models.
        date_columns = [
            "hire_date",
            "termination_date",
            "last_day_of_work",
        ]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        # Inject pipeline metadata as dedicated columns
        df["_pipeline_entity"]     = entity
        df["_pipeline_run_date"]   = run_date
        df["_pipeline_page_num"]   = page_num
        df["_pipeline_written_at"] = written_at

        # Serialise to Parquet in memory — no temp files, no disk I/O
        buffer = io.BytesIO()
        df.to_parquet(
            buffer,
            engine="pyarrow",
            compression="snappy",   # fast + good compression ratio
            index=False,            # drop the DataFrame row index
        )
        buffer.seek(0)
        return buffer.read()

    def _upload(self, s3_key: str, parquet_bytes: bytes) -> None:
        """
        Upload Parquet bytes to S3 with server-side encryption.
        Raises S3StageError on failure.
        """
        try:
            self._s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
                ServerSideEncryption="AES256",
            )
        except (BotoCoreError, ClientError) as exc:
            raise S3StageError(
                f"Failed to write s3://{self.bucket}/{s3_key}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Custom Exception
# ---------------------------------------------------------------------------

class S3StageError(Exception):
    """Raised when a Parquet serialisation or S3 upload fails."""
