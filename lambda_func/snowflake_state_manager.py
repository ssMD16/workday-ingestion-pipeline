"""
snowflake_state_manager.py
--------------------------
Tracks pipeline state entirely inside Snowflake using a dedicated audit table.

Why an audit table instead of querying the raw terminations table directly?
  - The raw terminations table in Snowflake holds NDJSON as a VARIANT column.
    Parsing MAX(termination_date) from a VARIANT requires awkward casting and
    depends on the raw schema never changing.
  - An audit table is schema-stable, purpose-built for state tracking, and
    doubles as an operations log — you can see every run, its record counts,
    its S3 keys, and whether it succeeded or failed.
  - Querying MAX(run_date) WHERE status = 'SUCCESS' from the audit table is
    clean, fast, and unambiguous — no JSON parsing required.

Audit table DDL (run this once in Snowflake before first pipeline execution):

    CREATE DATABASE IF NOT EXISTS pipeline_audit;
    CREATE SCHEMA  IF NOT EXISTS pipeline_audit.ingestion;

    CREATE TABLE IF NOT EXISTS pipeline_audit.ingestion.runs (
        run_id          VARCHAR         NOT NULL,
        run_date        DATE            NOT NULL,
        entity          VARCHAR         NOT NULL,   -- 'employees' | 'terminations'
        records_staged  INTEGER         NOT NULL,
        s3_key          VARCHAR,
        status          VARCHAR         NOT NULL,   -- 'SUCCESS' | 'FAILED'
        started_at      TIMESTAMP_TZ    NOT NULL,
        completed_at    TIMESTAMP_TZ,
        error_message   VARCHAR,
        PRIMARY KEY (run_id)
    );

State query:
    SELECT MAX(run_date)
    FROM   pipeline_audit.ingestion.runs
    WHERE  entity = 'terminations'
    AND    status = 'SUCCESS';
"""

import logging
import uuid
from datetime import date, datetime, timezone
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError

logger = logging.getLogger(__name__)

# Fully-qualified audit table name
AUDIT_TABLE = "pipeline_audit.ingestion.runs"


class SnowflakeStateManager:
    """
    Reads and writes pipeline run state via a Snowflake audit table.

    A single SnowflakeStateManager instance is created per Lambda invocation.
    It opens one Snowflake connection and reuses it for both the state read
    at the start of the run and the audit write at the end.

    Usage:
        state = SnowflakeStateManager(
            account="my_account",
            user="my_user",
            password="my_password",
            warehouse="my_warehouse",
            database="pipeline_audit",
            schema="ingestion",
            role="pipeline_role",
        )

        last_date  = state.get_last_loaded_date()      # called before extraction
        state.record_run(entity, records, s3_key, ...) # called after staging
    """

    def __init__(
        self,
        account:   str,
        user:      str,
        password:  str,
        warehouse: str,
        database:  str,
        schema:    str,
        role:      Optional[str] = None,
    ):
        self._conn = self._connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
        )

    # -----------------------------------------------------------------------
    # Public
    # -----------------------------------------------------------------------

    def get_last_loaded_date(self, entity: str = "terminations") -> Optional[date]:
        """
        Query the audit table for the most recent successful run date.

        Args:
            entity: The entity to check. Defaults to "terminations" since
                    that drives the incremental window for this pipeline.

        Returns:
            The most recent successful run_date as a date object,
            or None if no successful run exists yet (first-ever execution).
        """
        sql = f"""
            SELECT MAX(run_date) AS last_run_date
            FROM   {AUDIT_TABLE}
            WHERE  entity = %s
            AND    status = 'SUCCESS'
        """
        try:
            cursor = self._conn.cursor(DictCursor)
            cursor.execute(sql, (entity,))
            row = cursor.fetchone()

            last_date = row["LAST_RUN_DATE"] if row else None

            if last_date:
                logger.info(
                    "Last successful '%s' load date from Snowflake: %s",
                    entity, last_date,
                )
            else:
                logger.warning(
                    "No successful '%s' runs found in audit table. "
                    "Assuming first run.",
                    entity,
                )

            return last_date

        except (DatabaseError, ProgrammingError) as exc:
            raise SnowflakeStateError(
                f"Failed to query last loaded date for '{entity}': {exc}"
            ) from exc

    def record_run(
        self,
        entity:         str,
        run_date:       date,
        records_staged: int,
        s3_key:         Optional[str],
        status:         str,
        started_at:     datetime,
        error_message:  Optional[str] = None,
    ) -> str:
        """
        Write a pipeline run record to the Snowflake audit table.

        Called twice per entity per Lambda invocation:
          - Once with status='SUCCESS' after a successful stage
          - Once with status='FAILED' if an exception is caught

        Args:
            entity:         'employees' | 'terminations'
            run_date:       The pipeline run date (today).
            records_staged: Number of records written to S3.
            s3_key:         The S3 key the file was staged to.
            status:         'SUCCESS' | 'FAILED'
            started_at:     Timestamp when this entity's extraction began.
            error_message:  Exception message if status is 'FAILED'.

        Returns:
            The generated run_id UUID string.
        """
        run_id       = str(uuid.uuid4())
        completed_at = datetime.now(timezone.utc)

        sql = f"""
            INSERT INTO {AUDIT_TABLE} (
                run_id, run_date, entity, records_staged,
                s3_key, status, started_at, completed_at, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            run_id,
            run_date,
            entity,
            records_staged,
            s3_key,
            status,
            started_at,
            completed_at,
            error_message,
        )

        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, params)
            self._conn.commit()

            logger.info(
                "Audit record written: run_id=%s entity=%s status=%s records=%d",
                run_id, entity, status, records_staged,
            )
            return run_id

        except (DatabaseError, ProgrammingError) as exc:
            raise SnowflakeStateError(
                f"Failed to write audit record for '{entity}': {exc}"
            ) from exc

    def close(self) -> None:
        """Explicitly close the Snowflake connection."""
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed.")

    # -----------------------------------------------------------------------
    # Private
    # -----------------------------------------------------------------------

    @staticmethod
    def _connect(
        account:   str,
        user:      str,
        password:  str,
        warehouse: str,
        database:  str,
        schema:    str,
        role:      Optional[str],
    ) -> snowflake.connector.SnowflakeConnection:
        """
        Open a Snowflake connection.
        Raises SnowflakeStateError with a clear message on failure.
        """
        connect_kwargs = dict(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )
        if role:
            connect_kwargs["role"] = role

        try:
            conn = snowflake.connector.connect(**connect_kwargs)
            logger.info(
                "Snowflake connection opened. account=%s database=%s schema=%s",
                account, database, schema,
            )
            return conn

        except DatabaseError as exc:
            raise SnowflakeStateError(
                f"Could not connect to Snowflake (account='{account}'): {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Custom Exception
# ---------------------------------------------------------------------------

class SnowflakeStateError(Exception):
    """Raised when a Snowflake state read or write fails."""
