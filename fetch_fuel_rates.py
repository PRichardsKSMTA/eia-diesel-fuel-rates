import os
import datetime as dt
import calendar
import requests
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# Configuration from environment
API_KEY  = os.getenv("EIA_API_KEY")
SERVER   = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USER     = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASSWORD")
DRIVER   = os.getenv("DRIVER")

# Map our EIA series IDs to time spans
SERIES = {
    "Weekly":  "PET.EMD_EPD2D_PTE_NUS_DPG.W",
    "Monthly": "PET.EMD_EPD2D_PTE_NUS_DPG.M"
}

def get_eia_data(series_id: str, start: str):
    """Fetch raw data from EIA API v2 using backward-compatibility series endpoint."""
    url = f"https://api.eia.gov/v2/seriesid/{series_id}"
    params = {"api_key": API_KEY, "start": start}
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    payload = resp.json()
    # v1-compatible payload: series array
    if "series" in payload:
        return payload["series"][0]["data"]  # list of [period, value]
    # v2 payload: list of dicts under response.data
    records = payload.get("response", {}).get("data", [])
    return [(rec.get("period"), rec.get("value")) for rec in records]


def compute_begin_end(eff_date: dt.date, span: str):
    """Compute BEGIN_DT and END_DT based on span."""
    if span == "Monthly":
        # Monthly: first and last day of the month
        first = eff_date.replace(day=1)
        last = eff_date.replace(day=calendar.monthrange(eff_date.year, eff_date.month)[1])
    else:
        # Weekly: effective date is Monday; period covers the week prior (Sunday–Saturday)
        # Compute Saturday of the week prior: eff_date (Monday) minus 2 days
        last = eff_date - dt.timedelta(days=2)
        # Compute Sunday of the week prior: Saturday minus 6 days
        first = last - dt.timedelta(days=6)
    return first, last


def upsert_records(cursor, records):
    """Merge new records into dbo.EIA_DIESEL_FUEL_RATES."""
    merge_sql = (
        "MERGE dbo.EIA_DIESEL_FUEL_RATES AS T "
        "USING (VALUES (?, ?, ?, ?, ?)) AS S(EFFECTIVE_DT, TIME_SPAN, FUEL_RATE, BEGIN_DT, END_DT) "
        "ON T.EFFECTIVE_DT = S.EFFECTIVE_DT AND T.TIME_SPAN = S.TIME_SPAN "
        "WHEN NOT MATCHED THEN INSERT (EFFECTIVE_DT, TIME_SPAN, FUEL_RATE, BEGIN_DT, END_DT) "
        "VALUES (S.EFFECTIVE_DT, S.TIME_SPAN, S.FUEL_RATE, S.BEGIN_DT, S.END_DT);"
    )
    for rec in records:
        cursor.execute(
            merge_sql,
            rec["eff_date"], rec["span"], rec["rate"], rec["begin_dt"], rec["end_dt"]
        )


def main(start_date: str, dry_run: bool = False):
    """
    Fetch EIA diesel price data from `start_date` through today.
    If dry_run is True, prints a DataFrame of records instead of upserting.
    """
    # Determine threshold date
    if len(start_date) == 8:
        threshold_date = dt.datetime.strptime(start_date, "%Y%m%d").date()
    elif len(start_date) == 6:
        threshold_date = dt.datetime.strptime(start_date, "%Y%m").date().replace(day=1)
    else:
        raise ValueError("start_date must be YYYYMMDD for weekly or YYYYMM for monthly")
    today = dt.date.today()

    all_records = []

    for span, sid in SERIES.items():
        try:
            raw = get_eia_data(sid, start_date)
        except requests.HTTPError as e:
            print(f"Skipping {span} fetch: {e}")
            continue

        for period, price in raw:
            # Skip missing price records
            if price is None:
                print(f"Skipping {span} record with missing price for period={period}")
                continue

            # Parse period string
            if span == "Weekly":
                if "-" in period:
                    eff = dt.datetime.strptime(period, "%Y-%m-%d").date()
                else:
                    eff = dt.datetime.strptime(period, "%Y%m%d").date()
            else:
                if "-" in period:
                    eff = dt.datetime.strptime(period, "%Y-%m").date()
                else:
                    eff = dt.datetime.strptime(period, "%Y%m").date()

            # Filter by threshold and today
            if eff < threshold_date or eff > today:
                continue

            # Compute date range
            begin, end = compute_begin_end(eff, span)
            all_records.append({
                "eff_date": eff,
                "span": span,
                "rate": float(price),
                "begin_dt": begin,
                "end_dt": end
            })

    if dry_run:
        import pandas as pd
        df = pd.DataFrame([
            {"EFFECTIVE_DT": r["eff_date"], "TIME_SPAN": r["span"],
             "FUEL_RATE": r["rate"], "BEGIN_DT": r["begin_dt"], "END_DT": r["end_dt"]}
            for r in all_records
        ])
        print(f"Dry-run mode: collected {len(df)} records")
        print(df)
        return

    # Connect and upsert
    conn_str = (
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USER};PWD={PASSWORD}"
    )
    cnxn = pyodbc.connect(conn_str, autocommit=True)
    cursor = cnxn.cursor()

    upsert_records(cursor, all_records)
    cursor.close()
    cnxn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and upsert EIA diesel fuel rates using APIv2 backward-compat endpoint."
    )
    parser.add_argument(
        "--start_date", required=True,
        help="YYYYMMDD for weekly; YYYYMM for monthly"
    )
    parser.add_argument(
        "--dry_run", action="store_true",
        help="If set, print collected records instead of upserting."
    )
    args = parser.parse_args()
    main(args.start_date, args.dry_run)
