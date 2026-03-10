# CHEWY_BNF_Options_Trading

A Python/PySpark project for fetching, storing, and charting BankNifty (BNF) options data using the [Zerodha KiteConnect API](https://kite.trade/docs/connect/v3/).

---

## Project Structure

```
Project1/
├── 1_PROD_Get_All_BNF_Options_Instruments.ipynb   # Fetch all BNF options instruments
├── 2_PROD_Get_Auto_ATM_Strikes_Data.ipynb         # Auto-fetch ATM strike historical data
├── 2_DEV_Get_ATM_Strikes_Data.ipynb               # Dev version of ATM strikes data fetch
├── 3_Get_Calendar_Charts_BNF.ipynb                # Calendar spread charts for BNF
├── 3_Get_Charts_BNF.ipynb                         # BNF options charts
├── Temp.ipynb                                     # Scratch/temp notebook
├── Temp2.py                                       # Tkinter GUI for live trading
├── config.py                                      # Credentials & configuration (DO NOT COMMIT)
├── utils/
│   └── kite_auth.py                               # Reusable KiteConnect authentication
├── DataFiles/
│   └── HistoricalData/                            # Parquet files (1min, 3min, day intervals)
├── Chartsnapshots/                                # Saved chart images
├── spark-warehouse/                               # Spark metastore
├── .venv/                                         # Python virtual environment
├── requirements.txt                               # Python dependencies
├── .env.example                                   # Example environment variables
└── .gitignore                                     # Git ignore rules
```

---

## Notebook Descriptions

| Notebook | Purpose |
|---|---|
| `1_PROD_Get_All_BNF_Options_Instruments.ipynb` | Logs into Zerodha, fetches all BNF options instruments, filters by expiry/strike, saves to Parquet |
| `2_PROD_Get_Auto_ATM_Strikes_Data.ipynb` | Automatically identifies ATM strikes and downloads historical OHLCV data via KiteConnect |
| `2_DEV_Get_ATM_Strikes_Data.ipynb` | Development/testing version of the ATM data fetch pipeline |
| `3_Get_Calendar_Charts_BNF.ipynb` | Reads historical Parquet data and renders calendar spread charts for BNF options |
| `3_Get_Charts_BNF.ipynb` | Renders OHLCV and options charts for BNF instruments |
| `Temp2.py` | Tkinter-based GUI for live position monitoring and order placement |

---

## Setup

### 1. Create and activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate   # macOS/Linux
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure credentials

Copy `.env.example` to `.env` and fill in your Zerodha credentials:

```bash
cp .env.example .env
```

Or edit `config.py` directly (ensure it is listed in `.gitignore`).

### 4. Run notebooks

Launch Jupyter:

```bash
jupyter notebook
```

Run notebooks in order:
1. `1_PROD_Get_All_BNF_Options_Instruments.ipynb` — get instruments
2. `2_PROD_Get_Auto_ATM_Strikes_Data.ipynb` — fetch historical data
3. `3_Get_Calendar_Charts_BNF.ipynb` or `3_Get_Charts_BNF.ipynb` — visualise

---

## Authentication Flow

All notebooks use the same Zerodha login flow:

1. POST to `https://kite.zerodha.com/api/login` with user_id + password
2. POST to `https://kite.zerodha.com/api/twofa` with TOTP (via `pyotp`)
3. Generate KiteConnect session with `request_token` + `api_secret`
4. Set `access_token` on the `KiteConnect` instance

This flow is encapsulated in [`utils/kite_auth.py`](utils/kite_auth.py) for reuse across notebooks.

---

## Data Storage

Historical OHLCV data is stored as partitioned Parquet files under:

```
DataFiles/HistoricalData/<interval>/<fetch_date>/<instrument_token>/day=<trade_date>/
```

Intervals: `1Minute`, `3Minute`, `day`

---

## Dependencies

See [`requirements.txt`](requirements.txt) for the full list. Key packages:

- `kiteconnect` — Zerodha KiteConnect Python client
- `pyspark` — Distributed data processing
- `pandas`, `numpy` — Data manipulation
- `pyotp` — TOTP 2FA
- `plotly` / `mplfinance` — Charting
- `jupyter` — Notebook environment

---

## Security Notes

> ⚠️ **Never commit `config.py` or `.env` to version control.**  
> Credentials (user_id, password, TOTP key, API key/secret) must be kept private.  
> Use `.gitignore` to exclude sensitive files.
