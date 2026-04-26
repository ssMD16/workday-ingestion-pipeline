"""
Mock Workday API
Simulates Workday's Reports-as-a-Service (RaaS) and REST API endpoints
for termination and employee data. Used for local development and testing
of the Snowflake ingestion pipeline.

Validation added:
  - Date range: start_date must not be after end_date
  - Enum guards: status and termination_type must be known values
  - Future date guard: start_date / end_date cannot be in the future
"""

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import Optional
from datetime import date
import secrets
import uvicorn
from fake_data import EMPLOYEES, TERMINATIONS

# ---------------------------------------------------------------------------
# App & Auth Setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Mock Workday API",
    description="Simulates Workday RaaS & REST endpoints for HR pipeline development.",
    version="1.0.0",
)

security = HTTPBasic()

MOCK_USERNAME = "workday_svc_user"
MOCK_PASSWORD = "mock_password_123"

# ---------------------------------------------------------------------------
# Validation Constants
# ---------------------------------------------------------------------------

VALID_EMPLOYMENT_STATUSES = {"Active", "Terminated", "All"}
VALID_TERMINATION_TYPES   = {"Voluntary", "Involuntary"}
VALID_WORKER_TYPES        = {"Employee", "Contractor"}


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Basic auth — mirrors Workday's ISU (Integration System User) auth model."""
    valid_user = secrets.compare_digest(credentials.username, MOCK_USERNAME)
    valid_pass = secrets.compare_digest(credentials.password, MOCK_PASSWORD)
    if not (valid_user and valid_pass):
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials. Check username and password.",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# ---------------------------------------------------------------------------
# Validation Helpers
# ---------------------------------------------------------------------------

def validate_employment_status(status: Optional[str]) -> None:
    """
    Reject unknown employment status values early.
    Prevents silent empty results from typos like ?status=active (lowercase).
    """
    if status and status not in VALID_EMPLOYMENT_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid status '{status}'. "
                f"Accepted values: {sorted(VALID_EMPLOYMENT_STATUSES)}"
            ),
        )


def validate_termination_type(termination_type: Optional[str]) -> None:
    """
    Reject unknown termination_type values.
    Real Workday enforces a controlled list — mirror that behaviour here.
    """
    if termination_type and termination_type not in VALID_TERMINATION_TYPES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid termination_type '{termination_type}'. "
                f"Accepted values: {sorted(VALID_TERMINATION_TYPES)}"
            ),
        )


def validate_date_range(start_date: Optional[date], end_date: Optional[date]) -> None:
    """
    Three date-range guards:
      1. start_date cannot be after end_date.
      2. start_date cannot be in the future (no data exists yet).
      3. end_date cannot be in the future (would return an incomplete window).
    """
    today = date.today()

    if start_date and start_date > today:
        raise HTTPException(
            status_code=422,
            detail=f"start_date '{start_date}' cannot be in the future.",
        )

    if end_date and end_date > today:
        raise HTTPException(
            status_code=422,
            detail=f"end_date '{end_date}' cannot be in the future.",
        )

    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=422,
            detail=(
                f"start_date '{start_date}' cannot be after "
                f"end_date '{end_date}'."
            ),
        )


# ---------------------------------------------------------------------------
# Response Models
# ---------------------------------------------------------------------------

class Employee(BaseModel):
    employee_id: str
    first_name: str
    last_name: str
    email: str
    job_title: str
    department: str
    location_city: str
    location_state: str
    location_country: str
    hire_date: date
    employment_status: str  # "Active" | "Terminated"
    worker_type: str        # "Employee" | "Contractor"
    manager_id: Optional[str]


class TerminationRecord(BaseModel):
    termination_id: str
    employee_id: str
    termination_date: date
    last_day_of_work: date
    termination_type: str           # "Voluntary" | "Involuntary"
    termination_reason: str         # "Resignation", "Performance", "Layoff", etc.
    termination_reason_detail: str  # Free-text detail / sub-reason
    rehire_eligible: bool
    recorded_by: str                # HR rep who entered the transaction
    recorded_at: str                # ISO timestamp of data entry


class RaaSReport(BaseModel):
    """Wrapper that mimics Workday's Reports-as-a-Service JSON envelope."""
    report_name: str
    generated_at: str
    tenant: str
    total_records: int
    data: list


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Utility"])
def health_check():
    """Simple liveness probe — no auth required."""
    return {"status": "ok", "service": "mock-workday-api"}


# ---------------------------------------------------------------------------
# Employee Endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/ccx/service/customreport2/{tenant}/HR_Analytics/Active_Employees",
    response_model=RaaSReport,
    tags=["Employees"],
    summary="RaaS — All Active Employees",
)
def get_active_employees(
    tenant: str,
    status: Optional[str] = Query(None, description="Filter by status: Active | Terminated | All"),
    limit:  Optional[int] = Query(None, description="Max records to return (pagination)"),
    offset: Optional[int] = Query(0,    description="Number of records to skip (pagination)"),
    _: str = Depends(verify_credentials),
):
    """
    Mirrors the Workday RaaS endpoint pattern:
      /ccx/service/customreport2/{tenant}/{owner}/{report_name}
    Returns all employees, optionally filtered by employment_status.
    Supports limit/offset pagination for large dataset extraction.
    """
    validate_employment_status(status)

    filtered = EMPLOYEES
    if status and status != "All":
        filtered = [e for e in EMPLOYEES if e["employment_status"] == status]

    # Apply pagination if limit is provided
    paginated = filtered[offset : offset + limit] if limit else filtered[offset:]

    return RaaSReport(
        report_name="Active_Employees",
        generated_at="2025-04-01T08:00:00Z",
        tenant=tenant,
        total_records=len(paginated),
        data=paginated,
    )


@app.get(
    "/ccx/service/customreport2/{tenant}/HR_Analytics/Employee/{employee_id}",
    response_model=RaaSReport,
    tags=["Employees"],
    summary="RaaS — Single Employee by ID",
)
def get_employee_by_id(
    tenant: str,
    employee_id: str,
    _: str = Depends(verify_credentials),
):
    """Fetch a single employee record by employee_id."""
    match = [e for e in EMPLOYEES if e["employee_id"] == employee_id]
    if not match:
        raise HTTPException(status_code=404, detail=f"Employee '{employee_id}' not found.")

    return RaaSReport(
        report_name="Employee_Detail",
        generated_at="2025-04-01T08:00:00Z",
        tenant=tenant,
        total_records=1,
        data=match,
    )


# ---------------------------------------------------------------------------
# Termination Endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/ccx/service/customreport2/{tenant}/HR_Analytics/Terminations",
    response_model=RaaSReport,
    tags=["Terminations"],
    summary="RaaS — All Termination Records",
)
def get_terminations(
    tenant: str,
    start_date:       Optional[date] = Query(None, description="Filter terminations on or after this date (YYYY-MM-DD)"),
    end_date:         Optional[date] = Query(None, description="Filter terminations on or before this date (YYYY-MM-DD)"),
    termination_type: Optional[str]  = Query(None, description="Voluntary | Involuntary"),
    limit:            Optional[int]  = Query(None, description="Max records to return (pagination)"),
    offset:           Optional[int]  = Query(0,    description="Number of records to skip (pagination)"),
    _: str = Depends(verify_credentials),
):
    """
    Core endpoint for the ingestion pipeline.
    Supports date-range filtering for incremental loads and limit/offset
    pagination so the WorkdayClient can pull large datasets in bounded chunks.
    """
    validate_date_range(start_date, end_date)
    validate_termination_type(termination_type)

    filtered = TERMINATIONS

    if start_date:
        filtered = [t for t in filtered if date.fromisoformat(t["termination_date"]) >= start_date]
    if end_date:
        filtered = [t for t in filtered if date.fromisoformat(t["termination_date"]) <= end_date]
    if termination_type:
        filtered = [t for t in filtered if t["termination_type"] == termination_type]

    # Apply pagination if limit is provided
    paginated = filtered[offset : offset + limit] if limit else filtered[offset:]

    return RaaSReport(
        report_name="Terminations",
        generated_at="2025-04-01T08:00:00Z",
        tenant=tenant,
        total_records=len(paginated),
        data=paginated,
    )


@app.get(
    "/ccx/service/customreport2/{tenant}/HR_Analytics/Terminations/{termination_id}",
    response_model=RaaSReport,
    tags=["Terminations"],
    summary="RaaS — Single Termination by ID",
)
def get_termination_by_id(
    tenant: str,
    termination_id: str,
    _: str = Depends(verify_credentials),
):
    """Fetch a single termination record by termination_id."""
    match = [t for t in TERMINATIONS if t["termination_id"] == termination_id]
    if not match:
        raise HTTPException(status_code=404, detail=f"Termination '{termination_id}' not found.")

    return RaaSReport(
        report_name="Termination_Detail",
        generated_at="2025-04-01T08:00:00Z",
        tenant=tenant,
        total_records=1,
        data=match,
    )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
