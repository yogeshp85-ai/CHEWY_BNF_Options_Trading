"""
config.py — Project configuration and credentials.

Credentials are loaded from environment variables (via a .env file).
Copy .env.example to .env and fill in your values.

DO NOT commit this file with real credentials to version control.
"""

import os
from dotenv import load_dotenv

# Load variables from .env file if present
load_dotenv()

# ---------------------------------------------------------------------------
# Zerodha / KiteConnect credentials
# ---------------------------------------------------------------------------
CREDS = {
    "user_id":    os.getenv("ZERODHA_USER_ID",    ""),
    "password":   os.getenv("ZERODHA_PASSWORD",   ""),
    "totp_key":   os.getenv("ZERODHA_TOTP_KEY",   ""),
    "api_key":    os.getenv("ZERODHA_API_KEY",    ""),
    "api_secret": os.getenv("ZERODHA_API_SECRET", ""),
}

# ---------------------------------------------------------------------------
# Zerodha API endpoints
# ---------------------------------------------------------------------------
BASE_URL        = "https://kite.zerodha.com"
LOGIN_URL       = "https://kite.zerodha.com/api/login"
TWOFA_URL       = "https://kite.zerodha.com/api/twofa"
INSTRUMENTS_URL = "https://api.kite.trade/instruments"

# ---------------------------------------------------------------------------
# Data storage paths
# ---------------------------------------------------------------------------
import pathlib

PROJECT_ROOT   = pathlib.Path(__file__).parent.resolve()
DATA_DIR       = PROJECT_ROOT / "DataFiles" / "HistoricalData"
CHARTS_DIR     = PROJECT_ROOT / "Chartsnapshots"

# ---------------------------------------------------------------------------
# PySpark configuration
# ---------------------------------------------------------------------------
SPARK_CONFIG = {
    "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Concurrent GC",
    "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors":   "G1 Concurrent GC",
    "spark.memory.offHeap.use": "true",
}

# ---------------------------------------------------------------------------
# Trading parameters
# ---------------------------------------------------------------------------
EXCHANGE        = "NFO"
UNDERLYING      = "BANKNIFTY"
STRIKE_STEP     = 100          # BankNifty strike interval
