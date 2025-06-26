import datetime as dt
import azure.functions as func # type: ignore

from .fetch_fuel_rates import main as fetch_main

def main(timer: func.TimerRequest) -> None:
    # Compute dates for weekly & monthly pulls
    today = dt.datetime.now(dt.timezone.utc).date()
    start_week  = (today - dt.timedelta(days=7)).strftime("%Y%m%d")
    start_month = today.strftime("%Y%m")

    # Run your logic (dry_run=False by default)
    fetch_main(start_week, dry_run=False)
    fetch_main(start_month, dry_run=False)
