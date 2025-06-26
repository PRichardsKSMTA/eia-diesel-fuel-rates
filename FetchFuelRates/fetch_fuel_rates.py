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
    """Fetch raw JSON data from EIA API starting at `start`."""
    url = "https://api.eia.gov/series/"
    params = {
        "api_key": API_KEY,
        "series_id": series_id,
        "start": start
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()["series"][0]["data"]  # list of [date_str, price]

def compute_begin_end(eff_date: dt.date, span: str):
    """Compute BEGIN_DT and END_DT based on span."""
    if span == "Monthly":
        first = eff_date.replace(day=1)
        last = eff_date.replace(day=calendar.monthrange(eff_date.year, eff_date.month)[1])
    else:  # Weekly (API date is Monday)
        first = eff_date - dt.timedelta(days=1)  # Sunday prior
        last  = first + dt.timedelta(days=6)     # Saturday after
    return first, last

def upsert_records(cursor, records):
    """Merge new records into dbo.EIA_DIESEL_FUEL_RATES."""
    merge_sql = """
    MERGE dbo.EIA_DIESEL_FUEL_RATES AS T
    USING (VALUES (?, ?, ?, ?, ?)) AS S(EFFECTIVE_DATE, TIME_SPAN, FUEL_RATE, BEGIN_DT, END_DT)
      ON T.EFFECTIVE_DATE = S.EFFECTIVE_DATE
     AND T.TIME_SPAN       = S.TIME_SPAN
    WHEN NOT MATCHED THEN
      INSERT (EFFECTIVE_DATE, TIME_SPAN, FUEL_RATE, BEGIN_DT, END_DT)
      VALUES (S.EFFECTIVE_DATE, S.TIME_SPAN, S.FUEL_RATE, S.BEGIN_DT, S.END_DT);
    """
    for rec in records:
        cursor.execute(
            merge_sql,
            rec["eff_date"], rec["span"], rec["rate"],
            rec["begin_dt"], rec["end_dt"]
        )


def main(start_date: str, dry_run: bool = False):
    """
    Fetch EIA diesel price data from `start_date`.
    If dry_run is True, prints a DataFrame of records instead of upserting.
    """
    all_records = []

    for span, sid in SERIES.items():
        raw_data = get_eia_data(sid, start_date)
        for date_str, price in raw_data:
            # Parse API date string
            if span == "Weekly":
                eff = dt.datetime.strptime(date_str, "%Y%m%d").date()
            else:
                eff = dt.datetime.strptime(date_str, "%Y%m").date()

            begin, end = compute_begin_end(eff, span)
            all_records.append({
                "eff_date": eff,
                "span": span,
                "rate": float(price),
                "begin_dt": begin,
                "end_dt": end
            })

    if dry_run:
        # Show collected records without writing to DB
        import pandas as pd # type: ignore
        df = pd.DataFrame([
            {
                "EFFECTIVE_DATE": r["eff_date"],
                "TIME_SPAN": r["span"],
                "FUEL_RATE": r["rate"],
                "BEGIN_DT": r["begin_dt"],
                "END_DT": r["end_dt"]
            }
            for r in all_records
        ])
        print(f"Dry-run mode: collected {len(df)} records")
        print(df)
        return

    # Connect to Azure SQL and upsert
    conn_str = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USER};PWD={PASSWORD}"
    )
    cnxn = pyodbc.connect(conn_str, autocommit=True)
    cursor = cnxn.cursor()

    upsert_records(cursor, all_records)
    cursor.close()
    cnxn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch and upsert EIA diesel fuel rates.")
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
