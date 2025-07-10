import os
import datetime as dt
import azure.functions as func
from fetch_fuel_rates import main as fetch_main

def get_prev_week_and_month():
    today = dt.date.today()
    # Last week’s Monday (start of the week for API)
    prev_monday = today - dt.timedelta(days=today.weekday() + 7)
    weekly_start = prev_monday.isoformat()      # e.g. "2025-06-22"

    # First day of last month
    first_this_month = today.replace(day=1)
    last_month = first_this_month - dt.timedelta(days=1)
    first_last_month = last_month.replace(day=1)
    monthly_start = first_last_month.isoformat()  # e.g. "2025-05-01"

    return weekly_start, monthly_start

def main(timer: func.TimerRequest) -> None:
    # Allow override for testing
    override = os.getenv("START_DATE_OVERRIDE")
    if override:
        # In testing mode, run a dry_run for the given ISO date
        fetch_main(override, dry_run=True)
        return

    # Normal scheduled run: fetch just last week and last month
    weekly_start, monthly_start = get_prev_week_and_month()
    fetch_main(weekly_start, dry_run=False)
    fetch_main(monthly_start, dry_run=False)

# To test locally with an override:
#   Set environment variable START_DATE_OVERRIDE to "2024-01-01"
# Then run: func start
# Or manually invoke via:
#   curl -X POST http://localhost:7071/admin/functions/FetchFuelRates -d '{}'
