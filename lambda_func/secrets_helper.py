"""
secrets_helper.py
-----------------
Fetches API and database credentials from AWS Secrets Manager.

Why Secrets Manager and not environment variables?
  - Environment variables in Lambda are visible in plaintext in the AWS console
    to anyone with iam:GetFunctionConfiguration permission.
  - Secrets Manager encrypts values with KMS, rotates them on a schedule,
    and provides a full audit trail of every access in CloudTrail.
  - Credentials are fetched at runtime, not baked into the deployment package.

Two secrets are used by this pipeline:

  Workday secret (JSON):
    {
      "username": "workday_svc_user",
      "password": "mock_password_123"
    }

  Snowflake secret (JSON):
    {
      "account":   "xy12345.us-east-1",
      "user":      "pipeline_svc_user",
      "password":  "snowflake_password",
      "warehouse": "INGESTION_WH",
      "database":  "pipeline_audit",
      "schema":    "ingestion",
      "role":      "PIPELINE_ROLE"
    }
"""

import json
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Module-level cache: credentials are fetched once per Lambda cold start
# and reused across warm invocations. Avoids a Secrets Manager API call
# on every single Lambda execution.
_workday_credentials_cache:   dict = {}
_snowflake_credentials_cache: dict = {}


def get_workday_credentials(secret_arn: str) -> dict:
    """
    Fetch and cache Workday credentials from Secrets Manager.

    Args:
        secret_arn: Full ARN of the Secrets Manager secret.

    Returns:
        Dict with "username" and "password" keys.

    Raises:
        SecretsError: if the secret cannot be retrieved or parsed.
    """
    global _workday_credentials_cache

    if _workday_credentials_cache:
        logger.info("Using cached Workday credentials (warm start).")
        return _workday_credentials_cache

    logger.info("Fetching Workday credentials from Secrets Manager.")
    credentials = _fetch_secret(secret_arn, required_keys=("username", "password"))
    _workday_credentials_cache = credentials
    logger.info("Workday credentials fetched and cached successfully.")
    return _workday_credentials_cache


def get_snowflake_credentials(secret_arn: str) -> dict:
    """
    Fetch and cache Snowflake connection credentials from Secrets Manager.

    Args:
        secret_arn: Full ARN of the Secrets Manager secret.

    Returns:
        Dict with keys: account, user, password, warehouse, database, schema.
        The "role" key is optional and may not be present.

    Raises:
        SecretsError: if the secret cannot be retrieved or parsed.
    """
    global _snowflake_credentials_cache

    if _snowflake_credentials_cache:
        logger.info("Using cached Snowflake credentials (warm start).")
        return _snowflake_credentials_cache

    logger.info("Fetching Snowflake credentials from Secrets Manager.")
    credentials = _fetch_secret(
        secret_arn,
        required_keys=("account", "user", "password", "warehouse", "database", "schema"),
    )
    _snowflake_credentials_cache = credentials
    logger.info("Snowflake credentials fetched and cached successfully.")
    return _snowflake_credentials_cache


# ---------------------------------------------------------------------------
# Shared private fetch helper
# ---------------------------------------------------------------------------

def _fetch_secret(secret_arn: str, required_keys: tuple) -> dict:
    """
    Retrieve a JSON secret from Secrets Manager and validate required keys.
    Shared by both get_workday_credentials and get_snowflake_credentials.
    """
    client = boto3.client("secretsmanager")

    try:
        response      = client.get_secret_value(SecretId=secret_arn)
        secret_string = response.get("SecretString")

        if not secret_string:
            raise SecretsError(
                f"Secret '{secret_arn}' exists but has no SecretString value."
            )

        credentials = json.loads(secret_string)

        for required_key in required_keys:
            if required_key not in credentials:
                raise SecretsError(
                    f"Secret '{secret_arn}' is missing required key: '{required_key}'"
                )

        return credentials

    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]

        if error_code == "ResourceNotFoundException":
            raise SecretsError(
                f"Secret not found: '{secret_arn}'. "
                "Verify the ARN and that the Lambda execution role has "
                "secretsmanager:GetSecretValue permission."
            ) from exc

        if error_code == "AccessDeniedException":
            raise SecretsError(
                f"Lambda execution role does not have permission to read "
                f"secret '{secret_arn}'. Attach secretsmanager:GetSecretValue "
                "to the role's IAM policy."
            ) from exc

        raise SecretsError(
            f"Unexpected error fetching secret '{secret_arn}': {exc}"
        ) from exc

    except json.JSONDecodeError as exc:
        raise SecretsError(
            f"Secret '{secret_arn}' value is not valid JSON: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Custom Exception
# ---------------------------------------------------------------------------

class SecretsError(Exception):
    """Raised when credentials cannot be retrieved from Secrets Manager."""
