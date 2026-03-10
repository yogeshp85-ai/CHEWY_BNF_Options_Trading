# CHEWY BankNifty Options Trading — PROD

> **Branch:** `feature-dev`  
> All production-grade, modularised code lives here. Original notebooks remain untouched in the root directory.

---

## 📌 Project Goal

Track **BankNifty options** price movement via Open Interest, volume, and OHLC candle data (3-minute intervals) to identify **Buy Call / Buy Put** signal opportunities in real time.

---

## 📂 Folder Structure

```
PROD/
│
├── utils/
│   ├── __init__.py          # Package marker + module overview
│   ├── kite_helpers.py      # KiteConnect login (TOTP), SparkSession factory
│   ├── strike_utils.py      # ATM rounding, strike level labels, Options DF, Parquet schemas
│   ├── data_fetcher.py      # Historical OHLC fetch, Parquet write, scheduled loop
│   └── chart_utils.py       # Analytics, Plotly chart rendering, auto-refresh loop
│
├── 1_Get_Instruments.ipynb  # Thin notebook — runs once/month to refresh instruments
├── 2_Fetch_Strikes_Data.ipynb # Thin notebook — continuous data pull during market hours
├── 3_Charts_BNF.ipynb       # Thin notebook — renders & auto-refreshes charts
│
└── README.md                # This file
```

Root-level DataFiles/ (written by notebooks, not committed):
```
DataFiles/
├── Instruments/
│   ├── Banknifty_Options/       ← Parquet
│   ├── Banknifty_Futures/       ← Parquet
│   ├── Nifty_Options/           ← Parquet
│   ├── Nifty_Futures/           ← Parquet
│   └── Nifty_Banknifty_Expiries/← Parquet
└── HistoricalData/
    └── 3minute/
        └── <expiry>/
            └── <instrument_token>/  ← Parquet, partitioned by day
```

---

## 🚀 Execution Flow

### Step 0 — Prerequisites (once per setup)

```bash
pip install -r requirements.txt
```

Create a `.env` file in the **repo root** (copied from `.env.example`):
```
ZERODHA_USER_ID=<your_user_id>
ZERODHA_PASSWORD=<your_password>
ZERODHA_TOTP_KEY=<your_totp_seed>
ZERODHA_API_KEY=<your_api_key>
ZERODHA_API_SECRET=<your_api_secret>
```

---

### Step 1 — Fetch All Instruments (once per month)

Open and run **`PROD/1_Get_Instruments.ipynb`** (Run All).

- Downloads all NFO instruments from Zerodha
- Filters BankNifty & Nifty options/futures
- Calculates weekly and monthly expiry dates
- Writes to `DataFiles/Instruments/`

Run again at the start of each new monthly expiry cycle.

---

### Step 2 — Fetch OHLC Data (every trading day, market hours)

Open and configure **`PROD/2_Fetch_Strikes_Data.ipynb`**.

Key CONFIG parameters:
| Parameter | Default | Notes |
|-----------|---------|-------|
| `BANKNIFTY` | `True` | Pull BankNifty options |
| `CUSTOM_STRIKE` | `0` | `0` = live LTP; set e.g. `56500` to override |
| `NUM_STRIKES` | `9` | ITM/OTM strikes each side |
| `LOOP_INTERVAL_MIN` | `1` | Pull frequency |

Run all cells. The last cell **blocks** and loops until interrupted.

- Data is written to `DataFiles/HistoricalData/3minute/<expiry>/<token>/`
- Partitioned by `day` for efficient reads

---

### Step 3 — View Charts (run alongside Step 2)

Open and configure **`PROD/3_Charts_BNF.ipynb`** in a **parallel kernel**.

Key CONFIG parameters:
| Parameter | Default | Notes |
|-----------|---------|-------|
| `STRIKE_LEVEL_NAME` | `'ATM'` | Which strike to chart |
| `CE_OR_PE` | `'E'` | `'CE'` / `'PE'` / `'E'` (straddle) |
| `IS_LATEST_DAY` | `True` | Show only today |
| `CUSTOM_STRIKE` | `0` | Override ATM |

Run all cells. The last cell **blocks** and auto-refreshes charts every minute.

---

## 📊 Chart Panels

| Figure | Contents |
|--------|----------|
| **Price & ROC** (2×2) | Straddle close, trailing SL, StopLoss signal, incremental avg, entry-allowed zone, ROC oscillator, CE/PE individual prices |
| **Open Interest** (1×2) | Combined OI + avg; CE/PE individual OI |
| **Candlestick** | OHLC 3-minute candles with date axis |

### Signal Logic
- **StopLoss line** — derived from max cumulative average, adjusted when CE or PE is at its session minimum
- **Entry Allowed** — marks candles where straddle close is below the trailing stop zone (green line = safe entry, None = avoid)
- **ROC_AVG** — rolling rate-of-change (20 periods) for detecting momentum shifts

---

## 🔧 Module Reference

### `utils/kite_helpers.py`
| Function | Description |
|----------|-------------|
| `kite_login()` | Automated TOTP login → returns `(kite, kws, access_token)` |
| `get_spark_session(app_name)` | Creates/gets SparkSession with G1GC and dynamic partition overwrite |

### `utils/strike_utils.py`
| Function | Description |
|----------|-------------|
| `round_to_hundred(num)` | Round to nearest 100 (BankNifty strike interval) |
| `round_to_fifty(num)` | Round to nearest 50 (Nifty strike interval) |
| `get_ATM_Strike(kite, token, rounding)` | Fetch live LTP and return ATM strike |
| `get_Options_DF(spark, df, atm, expiry, range, n)` | Filter + annotate options DF |
| `read_instruments(spark, path)` | Read all instrument Parquet files |
| `get_expiry_dates(expiries_df, name)` | Extract expiry dates for 'BANKNIFTY' or 'NIFTY' |

### `utils/data_fetcher.py`
| Function | Description |
|----------|-------------|
| `get_historical_data(kite, spark, token, from, to, interval)` | Fetch OHLC from API → Spark DF |
| `get_latest_data(kite, spark, ...)` | Pull + write one round of data for all configured strikes |
| `run_data_loop(kite, spark, ..., loop_interval_minutes)` | Continuous scheduled loop wrapper |

### `utils/chart_utils.py`
| Function | Description |
|----------|-------------|
| `get_Strike_OHLC_data(spark, opts_df, ...)` | Read + filter OHLC Parquet for one strike |
| `get_ROC_added(df, col_name, length)` | Add ROC_AVG column to Spark DF |
| `compute_analytics(spark, df, ce_or_pe, is_latest_day)` | Full analytics pipeline → pandas DF |
| `plot_Chart(df, strike, name, ce_or_pe)` | Render 3 Plotly figures |
| `get_chart(spark, opts_df, expiry, ...)` | Orchestrate read → analytics → plot |
| `run_chart_loop(spark, opts_df, ..., loop_interval_minutes)` | Auto-refresh loop |

---

## ⚠️ Known Caveats

- **Spark heartbeat errors** in output logs are benign — caused by long-running loops; data writes complete successfully.
- **SparkUI port conflicts** (`4040`, `4041`) are warnings only.
- `NativeCodeLoader` WARN on macOS is cosmetic — Hadoop native libs are Linux-only.
- The `.env` file must **never be committed**. It is listed in `.gitignore`.

---

## 📦 Dependencies

See `requirements.txt` in the repo root. Key packages:
- `kiteconnect` — Zerodha API client
- `pyspark` — distributed data processing
- `plotly` — interactive charts
- `pyotp` — TOTP 2FA
- `python-dotenv` — `.env` file loading
- `schedule` — Python job scheduler
