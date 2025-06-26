# EIA Diesel Fuel Rates Azure Function

A Python-based Azure Function that fetches national average diesel fuel prices (weekly & monthly) from the U.S. Department of Energy (EIA) API and upserts them into an Azure SQL table (`dbo.EIA_DIESEL_FUEL_RATES`).

---

## Features

* **Initial Backfill**: Pulls historical data from a specified start date (e.g. `2025-01-01`) through today.
* **Incremental Updates**: Scheduled weekly on Tuesdays at 23:00 UTC to fetch only the previous week and previous month.
* **Idempotent Upsert**: Uses a SQL `MERGE` statement keyed on `EFFECTIVE_DT + TIME_SPAN` to avoid duplicates.
* **Override Mode**: Environment variable `START_DATE_OVERRIDE` allows one-off dry-run backfills for testing.

---

## Repository Structure

```text
└── 📁eia-diesel-fuel-rates
    └── 📁FetchFuelRates
        ├── __init__.py
        ├── fetch_fuel_rates.py
        ├── function.json
    ├── .env
    ├── .funcignore
    ├── .gitignore
    ├── fetch_fuel_rates.py
    ├── host.json
    ├── LICENSE
    ├── local.settings.json
    ├── README.md
    └── requirements.txt
```

---

## Prerequisites

* Python 3.8+ (tested on 3.10–3.12)
* Azure Functions Core Tools v4
* Azure CLI (for deployment)
* An Azure Storage account (for timer trigger in Azure and locally via Azurite or prod connection string)
* An Azure SQL Database with table `dbo.EIA_DIESEL_FUEL_RATES`:

  ```sql
  CREATE TABLE dbo.EIA_DIESEL_FUEL_RATES (
    RECORD_ID      INT IDENTITY PRIMARY KEY,
    EFFECTIVE_DT   DATE        NOT NULL,
    TIME_SPAN      VARCHAR(10) NOT NULL,
    FUEL_RATE      DECIMAL(10,4) NOT NULL,
    INSERTED_DTTM  DATETIME2   DEFAULT SYSUTCDATETIME(),
    BEGIN_DT       DATE        NOT NULL,
    END_DT         DATE        NOT NULL
  );
  ```

---

## Configuration

### .env (local development)

Create a `.env` file in the project root:

```ini
EIA_API_KEY=YOUR_EIA_API_KEY
SQL_SERVER=your_server.database.windows.net
SQL_DATABASE=your_database
SQL_USER=your_username
SQL_PASSWORD=your_password
DRIVER={ODBC Driver 18 for SQL Server}
```

### local.settings.json (for local Functions host)

> **Note**: This file should *not* be checked into source control.

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "EIA_API_KEY":      "YOUR_EIA_API_KEY",
    "SQL_SERVER":       "your_server.database.windows.net",
    "SQL_DATABASE":     "your_database",
    "SQL_USER":         "your_username",
    "SQL_PASSWORD":     "your_password",
    "DRIVER":           "{ODBC Driver 18 for SQL Server}",
    "START_DATE_OVERRIDE": "2024-01-01"   
  }
}
```

When `START_DATE_OVERRIDE` is set, the function runs a *dry-run* backfill from that date and prints the records to console.

---

## Installation & Local Testing

1. **Clone** the repo and create a virtual environment:

   ```bash
   git clone <repo_url>
   cd eia-diesel-fuel-rates
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Populate** `.env` and `local.settings.json` as above.

3. **Start** a local Azure storage emulator (Azurite) or configure `AzureWebJobsStorage` to your real account.

4. **Run** the Functions host:

   ```bash
   func start --verbose
   ```

5. **Trigger** override (dry-run) manually:

   ```bash
   curl -X POST http://localhost:7071/admin/functions/FetchFuelRates -d '{}'
   ```

You should see logs showing your backfilled records or merge statements.

---

## Usage

### One-time Backfill

```bash
python fetch_fuel_rates.py --start_date 2025-01-01
```

### Dry-Run

```bash
python fetch_fuel_rates.py --start_date 2025-01-01 --dry_run
```

### Scheduled Incremental Load

Deployed as an Azure Function with `function.json` timer trigger:

```json
{
  "bindings": [
    {
      "name": "timer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "0 0 23 * * Tue"
    }
  ]
}
```

This runs every Tuesday at 23:00 UTC, fetching only the previous week and previous month.

---

## Azure Deployment

1. **Login & select subscription**:

   ```bash
   az login
   az account set --subscription <your-subscription-id>
   ```

2. **Create** (or use) a Function App with Python runtime and system‑assigned identity.

3. **Publish**:

   ```bash
   func azure functionapp publish <YourFunctionAppName>
   ```

4. **Verify** via `func azure functionapp logstream <YourFunctionAppName>` and manual POST.

---

## License

This project is licensed under the [MIT License](LICENSE).
