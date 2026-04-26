"""
workday_client.py
-----------------
Thin HTTP client that wraps calls to the Mock Workday (RaaS) API.

Responsibilities:
  - Authenticate every request with HTTP Basic Auth
  - Build the correct RaaS URL for each endpoint
  - Handle HTTP errors and surface them as clean Python exceptions
  - Implement retry logic with exponential backoff for transient failures
  - Expose paginated generator methods so large datasets are fetched in
    bounded chunks rather than one massive request, keeping memory flat
    and eliminating API response timeouts

Pagination design:
  get_employees_paginated() and get_terminations_paginated() are Python
  generators — they yield one page (list[dict]) at a time using the
  limit/offset pattern supported by Workday RaaS. The caller (lambda_function)
  processes and stages each page immediately, so memory usage is bounded
  to PAGE_SIZE records regardless of total dataset size.

  The original non-paginated methods (get_employees, get_terminations) are
  retained for single-record lookups and unit testing convenience.
"""

import logging
from datetime import date
from typing import Generator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Retry on these HTTP status codes (server-side transient errors)
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# How long to wait for a response before giving up (seconds)
REQUEST_TIMEOUT = 30

# Maximum retries for transient failures
MAX_RETRIES = 3

# Base delay for exponential backoff (seconds)
BACKOFF_FACTOR = 2

# Number of records to request per paginated API call.
# 500 is a safe default — large enough to minimise round trips,
# small enough that each response serialises and uploads in well
# under the Lambda timeout even on a slow network.
PAGE_SIZE = 500


# ---------------------------------------------------------------------------
# WorkdayClient
# ---------------------------------------------------------------------------

class WorkdayClient:
    """
    HTTP client for the Mock Workday RaaS API.

    Paginated usage (preferred for large datasets):
        client = WorkdayClient(...)
        for page_num, page in enumerate(client.get_terminations_paginated(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )):
            # process one page of up to PAGE_SIZE records
            stager.stage(data=page, entity="terminations", page_num=page_num, ...)

    Single-call usage (for lookups and unit tests):
        terminations = client.get_terminations(...)
    """

    def __init__(
        self,
        base_url: str,
        tenant: str,
        username: str,
        password: str,
    ):
        self.base_url = base_url.rstrip("/")
        self.tenant   = tenant
        self._session = self._build_session(username, password)

    # -----------------------------------------------------------------------
    # Paginated generators (use these in the Lambda handler)
    # -----------------------------------------------------------------------

    def get_employees_paginated(
        self,
        status:    Optional[str] = "All",
        page_size: int = PAGE_SIZE,
    ) -> Generator[list[dict], None, None]:
        """
        Yield employee records one page at a time.

        Each iteration makes exactly one API call fetching up to `page_size`
        records. Stops when the API returns fewer records than requested,
        signalling the final page has been reached.

        Args:
            status:    "Active" | "Terminated" | "All"  (default "All")
            page_size: Records per API call (default PAGE_SIZE).

        Yields:
            list[dict] — one page of employee records.
        """
        base_params: dict = {}
        if status and status != "All":
            base_params["status"] = status

        yield from self._paginate(
            url=self._raas_url("Active_Employees"),
            base_params=base_params,
            page_size=page_size,
            entity_label="employees",
        )

    def get_terminations_paginated(
        self,
        start_date:       Optional[date] = None,
        end_date:         Optional[date] = None,
        termination_type: Optional[str]  = None,
        page_size:        int            = PAGE_SIZE,
    ) -> Generator[list[dict], None, None]:
        """
        Yield termination records one page at a time.

        Args:
            start_date:       Only return terminations on or after this date.
            end_date:         Only return terminations on or before this date.
            termination_type: "Voluntary" | "Involuntary"
            page_size:        Records per API call (default PAGE_SIZE).

        Yields:
            list[dict] — one page of termination records.
        """
        base_params: dict = {}
        if start_date:
            base_params["start_date"] = str(start_date)
        if end_date:
            base_params["end_date"] = str(end_date)
        if termination_type:
            base_params["termination_type"] = termination_type

        yield from self._paginate(
            url=self._raas_url("Terminations"),
            base_params=base_params,
            page_size=page_size,
            entity_label="terminations",
        )

    # -----------------------------------------------------------------------
    # Original single-call methods (retained for lookups and unit tests)
    # -----------------------------------------------------------------------

    def get_employees(self, status: Optional[str] = "All") -> list[dict]:
        """
        Fetch ALL employee records in a single call.
        Only appropriate for small datasets or unit tests.
        For production use, prefer get_employees_paginated().
        """
        params = {}
        if status and status != "All":
            params["status"] = status

        url      = self._raas_url("Active_Employees")
        response = self._get(url, params=params)
        records  = response.get("data", [])
        logger.info("Received %d employee record(s) (single call).", len(records))
        return records

    def get_terminations(
        self,
        start_date:       Optional[date] = None,
        end_date:         Optional[date] = None,
        termination_type: Optional[str]  = None,
    ) -> list[dict]:
        """
        Fetch ALL termination records in a single call.
        Only appropriate for small datasets or unit tests.
        For production use, prefer get_terminations_paginated().
        """
        params = {}
        if start_date:
            params["start_date"] = str(start_date)
        if end_date:
            params["end_date"] = str(end_date)
        if termination_type:
            params["termination_type"] = termination_type

        url      = self._raas_url("Terminations")
        response = self._get(url, params=params)
        records  = response.get("data", [])
        logger.info("Received %d termination record(s) (single call).", len(records))
        return records

    def get_employee_by_id(self, employee_id: str) -> Optional[dict]:
        """Fetch a single employee record by ID. Returns None if not found."""
        url = self._raas_url(f"Employee/{employee_id}")
        try:
            response = self._get(url)
            data     = response.get("data", [])
            return data[0] if data else None
        except WorkdayNotFoundError:
            logger.warning("Employee '%s' not found.", employee_id)
            return None

    def get_termination_by_id(self, termination_id: str) -> Optional[dict]:
        """Fetch a single termination record by ID. Returns None if not found."""
        url = self._raas_url(f"Terminations/{termination_id}")
        try:
            response = self._get(url)
            data     = response.get("data", [])
            return data[0] if data else None
        except WorkdayNotFoundError:
            logger.warning("Termination '%s' not found.", termination_id)
            return None

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _paginate(
        self,
        url:          str,
        base_params:  dict,
        page_size:    int,
        entity_label: str,
    ) -> Generator[list[dict], None, None]:
        """
        Core pagination loop shared by all paginated public methods.

        Sends successive GET requests with limit/offset params, yielding
        each page as a list of dicts. Stops when:
          - The API returns an empty data array, OR
          - The API returns fewer records than page_size (last page signal)

        Args:
            url:          The fully-built RaaS URL.
            base_params:  Filter params already resolved by the caller
                          (dates, status, etc.). limit/offset are added here.
            page_size:    Max records per request.
            entity_label: Used only for log messages.
        """
        offset     = 0
        page_num   = 0
        total_seen = 0

        while True:
            params = {**base_params, "limit": page_size, "offset": offset}

            logger.info(
                "Fetching %s page %d (limit=%d offset=%d)",
                entity_label, page_num, page_size, offset,
            )

            response = self._get(url, params=params)
            records  = response.get("data", [])
            count    = len(records)

            if count == 0:
                logger.info(
                    "No records returned for %s at offset=%d. Pagination complete.",
                    entity_label, offset,
                )
                break

            total_seen += count
            logger.info(
                "Page %d: received %d %s record(s) (total so far: %d).",
                page_num, count, entity_label, total_seen,
            )

            yield records

            # Fewer records than page_size means this was the last page
            if count < page_size:
                logger.info(
                    "Received %d < page_size %d. Final page reached for %s.",
                    count, page_size, entity_label,
                )
                break

            offset   += page_size
            page_num += 1

    def _raas_url(self, report_name: str) -> str:
        """Build the full RaaS URL for a given report name."""
        return (
            f"{self.base_url}/ccx/service/customreport2"
            f"/{self.tenant}/HR_Analytics/{report_name}"
        )

    def _get(self, url: str, params: Optional[dict] = None) -> dict:
        """
        Execute a GET request and return the parsed JSON body.
        Raises a typed WorkdayError subclass on failure.
        """
        try:
            response = self._session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            _raise_for_status(response)
            return response.json()

        except requests.exceptions.Timeout:
            raise WorkdayTimeoutError(
                f"Request timed out after {REQUEST_TIMEOUT}s: {url}"
            )
        except requests.exceptions.ConnectionError as exc:
            raise WorkdayConnectionError(
                f"Could not connect to Workday API at {url}: {exc}"
            )

    @staticmethod
    def _build_session(username: str, password: str) -> requests.Session:
        """
        Build a requests.Session with Basic Auth, retry strategy,
        and connection pooling reused across all pages in one run.
        """
        session = requests.Session()
        session.auth = (username, password)

        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://",  adapter)
        session.mount("https://", adapter)

        return session


# ---------------------------------------------------------------------------
# Status → exception mapper
# ---------------------------------------------------------------------------

def _raise_for_status(response: requests.Response) -> None:
    """
    Map HTTP status codes to typed exceptions instead of the generic
    requests.HTTPError, so callers can handle specific failure modes.
    """
    code = response.status_code

    if code == 200:
        return
    if code == 401:
        raise WorkdayAuthError(
            "Authentication failed. Check Workday ISU credentials."
        )
    if code == 404:
        raise WorkdayNotFoundError(
            f"Resource not found: {response.url}"
        )
    if code == 422:
        raise WorkdayValidationError(
            f"Validation error from Workday API: {response.text}"
        )
    if code == 429:
        raise WorkdayRateLimitError(
            "Workday API rate limit exceeded. Retry after back-off."
        )
    if code >= 500:
        raise WorkdayServerError(
            f"Workday server error ({code}): {response.text}"
        )

    # Catch-all for unexpected codes
    raise WorkdayError(
        f"Unexpected HTTP {code} from Workday API: {response.text}"
    )


# ---------------------------------------------------------------------------
# Custom Exception Hierarchy
# ---------------------------------------------------------------------------

class WorkdayError(Exception):
    """Base exception for all Workday client errors."""

class WorkdayAuthError(WorkdayError):
    """Raised on 401 — bad credentials."""

class WorkdayNotFoundError(WorkdayError):
    """Raised on 404 — resource does not exist."""

class WorkdayValidationError(WorkdayError):
    """Raised on 422 — invalid query parameters."""

class WorkdayRateLimitError(WorkdayError):
    """Raised on 429 — too many requests."""

class WorkdayServerError(WorkdayError):
    """Raised on 5xx — server-side failure."""

class WorkdayTimeoutError(WorkdayError):
    """Raised when the request times out."""

class WorkdayConnectionError(WorkdayError):
    """Raised when the API is unreachable."""
