"""
strike_utils.py
===============
Utilities for ATM strike calculation, instrument filtering,
and Spark schema definitions.

Includes:
  - Rounding helpers (round_to_hundred, round_to_fifty)
  - ATM strike lookup via KiteConnect LTP
  - Strike level labelling (ITM / OTM / ATM)
  - Options DataFrame filtering (with strike level columns)
  - Schema definitions for instruments and expiry DataFrames
  - Parquet read helpers for instruments and expiries
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import asc, lit
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Instrument token constants (hardcoded for BankNifty index & Nifty index)
# These are the NSE futures/index tokens used to fetch live LTP.
# ---------------------------------------------------------------------------
BANKNIFTY_INDEX_TOKEN = 260105
NIFTY_INDEX_TOKEN = 12014082

# Parquet data file paths (relative to repo root, not PROD/)
INSTRUMENTS_BASE_PATH = "DataFiles/Instruments"

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

INSTRUMENT_SCHEMA = StructType(
    [
        StructField("instrument_token", IntegerType(), nullable=False),
        StructField("exchange_token", StringType(), nullable=False),
        StructField("tradingsymbol", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("last_price", DoubleType(), nullable=False),
        StructField("expiry", DateType(), nullable=False),
        StructField("strike", DoubleType(), nullable=False),
        StructField("tick_size", DoubleType(), nullable=False),
        StructField("lot_size", IntegerType(), nullable=False),
        StructField("instrument_type", StringType(), nullable=False),
        StructField("segment", StringType(), nullable=False),
        StructField("exchange", StringType(), nullable=False),
    ]
)

EXPIRY_SCHEMA = StructType(
    [
        StructField("name", StringType(), nullable=False),
        StructField("current_week_expiry", DateType(), nullable=False),
        StructField("current_month_expiry", DateType(), nullable=False),
        StructField("next_week_expiry", DateType(), nullable=False),
        StructField("next_month_expiry", DateType(), nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Rounding helpers
# ---------------------------------------------------------------------------


def round_to_hundred(num: float) -> int:
    """Round a price to the nearest 100 (BankNifty strike interval).

    Parameters
    ----------
    num : float
        Raw price value.

    Returns
    -------
    int
        Price rounded to the nearest 100.
    """
    if num % 100 >= 50:
        return int(num + (100 - num % 100))
    return int(num - (num % 100))


def round_to_fifty(num: float) -> int:
    """Round a price to the nearest 50 (Nifty strike interval).

    Parameters
    ----------
    num : float
        Raw price value.

    Returns
    -------
    int
        Price rounded to the nearest 50.
    """
    if num % 50 >= 25:
        return int(num + (50 - num % 50))
    return int(num - (num % 50))


# ---------------------------------------------------------------------------
# ATM strike lookup
# ---------------------------------------------------------------------------


def get_ATM_Strike(kite, instrument_token: int, rounding_number: int) -> int:
    """Fetch the last traded price for an instrument and return the ATM strike.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated KiteConnect instance.
    instrument_token : int
        NSE instrument token (e.g. 260105 for BankNifty index).
    rounding_number : int
        Strike interval — 100 for BankNifty, 50 for Nifty.

    Returns
    -------
    int
        ATM strike price rounded to the nearest strike interval.
    """
    ltp = kite.ltp(instrument_token)[str(instrument_token)]["last_price"]
    logger.debug("LTP for token %s = %.2f", instrument_token, ltp)
    if rounding_number == 100:
        return round_to_hundred(ltp)
    if rounding_number == 50:
        return round_to_fifty(ltp)
    raise ValueError(f"Unsupported rounding_number={rounding_number}. Use 50 or 100.")


# ---------------------------------------------------------------------------
# Strike level classification UDFs
# ---------------------------------------------------------------------------


def calculate_Strike_level_type(col1: float) -> str:
    """Classify a strike level offset as ITM, OTM, or ATM.

    Parameters
    ----------
    col1 : float
        Strike level offset: (strike - ATM) / strike_interval.
        Negative = ITM (for CE), Positive = OTM (for CE).

    Returns
    -------
    str
        One of 'ITM', 'OTM', 'ATM'.
    """
    if col1 < 0:
        return "ITM"
    elif col1 > 0:
        return "OTM"
    return "ATM"


def calculate_Strike_level_Name(col1: float) -> str:
    """Generate a human-readable strike level name.

    Parameters
    ----------
    col1 : float
        Strike level offset.

    Returns
    -------
    str
        Examples: 'ATM-2', 'ATM', 'ATM+3'.
    """
    if col1 < 0:
        return "ATM" + str.replace(str(col1), ".0", "")
    elif col1 > 0:
        return "ATM+" + str.replace(str(col1), ".0", "")
    return "ATM"


# ---------------------------------------------------------------------------
# Options DataFrame builder
# ---------------------------------------------------------------------------


def get_Options_DF(
    spark: SparkSession,
    options_df_from_file: DataFrame,
    atm_strike: int,
    current_expiry: date,
    strike_range: int,
    num_strikes: int,
) -> DataFrame:
    """Filter and enrich options instruments around the ATM strike.

    Adds columns: ATM_Strike, Strike_level, Strike_level_type, Strike_level_Name.

    Parameters
    ----------
    spark : SparkSession
    options_df_from_file : DataFrame
        All options instruments loaded from parquet.
    atm_strike : int
        Current ATM strike price.
    current_expiry : date
        Expiry date to filter on.
    strike_range : int
        Strike interval (100 for BankNifty, 50 for Nifty).
    num_strikes : int
        Number of ITM/OTM strikes to include on each side of ATM.

    Returns
    -------
    DataFrame
        Filtered DataFrame with strike level columns.
        Columns: name, instrument_token, expiry, Strike_level_Name,
                 strike, Strike_level_type, lot_size, instrument_type, segment
    """
    from pyspark.sql import functions as fns

    udf_type = fns.udf(calculate_Strike_level_type, StringType())
    udf_name = fns.udf(calculate_Strike_level_Name, StringType())

    options_df = (
        options_df_from_file.filter(
            options_df_from_file["expiry"] == current_expiry
        )
        .filter(
            options_df_from_file["strike"]
            <= atm_strike + (strike_range * num_strikes)
        )
        .filter(
            options_df_from_file["strike"]
            >= atm_strike - (strike_range * num_strikes)
        )
        .orderBy(asc("strike"))
    )

    options_df = options_df.withColumn("ATM_Strike", lit(atm_strike))
    options_df = options_df.withColumn(
        "Strike_level",
        (options_df["strike"] - lit(atm_strike)) / strike_range,
    )
    options_df = options_df.withColumn(
        "Strike_level_type", udf_type(options_df["Strike_level"])
    )
    options_df = options_df.withColumn(
        "Strike_level_Name", udf_name(options_df["Strike_level"])
    )
    return options_df.select(
        "name",
        "instrument_token",
        "expiry",
        "Strike_level_Name",
        "strike",
        "Strike_level_type",
        "lot_size",
        "instrument_type",
        "segment",
    )


# ---------------------------------------------------------------------------
# Parquet readers
# ---------------------------------------------------------------------------


def read_instruments(spark: SparkSession, base_path: str = INSTRUMENTS_BASE_PATH):
    """Read all instruments DataFrames from parquet files.

    Parameters
    ----------
    spark : SparkSession
    base_path : str
        Root path containing Instruments/ subfolders.

    Returns
    -------
    tuple
        (banknifty_options, banknifty_futures, nifty_options, nifty_futures,
         expiries) — each a Spark DataFrame.
    """
    bnf_options = spark.read.parquet(
        f"{base_path}/Banknifty_Options", schema=INSTRUMENT_SCHEMA
    )
    bnf_futures = spark.read.parquet(
        f"{base_path}/Banknifty_Futures", schema=INSTRUMENT_SCHEMA
    )
    nifty_options = spark.read.parquet(
        f"{base_path}/Nifty_Options", schema=INSTRUMENT_SCHEMA
    )
    nifty_futures = spark.read.parquet(
        f"{base_path}/Nifty_Futures", schema=INSTRUMENT_SCHEMA
    )
    expiries = spark.read.parquet(
        f"{base_path}/Nifty_Banknifty_Expiries", schema=EXPIRY_SCHEMA
    )
    return bnf_options, bnf_futures, nifty_options, nifty_futures, expiries


def get_expiry_dates(expiries_df: DataFrame, name: str) -> dict:
    """Extract expiry dates for a given index (BANKNIFTY or NIFTY).

    Parameters
    ----------
    expiries_df : DataFrame
        Expiry DataFrame returned by read_instruments().
    name : str
        'BANKNIFTY' or 'NIFTY'.

    Returns
    -------
    dict
        Keys: current_week, current_month, next_week, next_month — all date objects.
    """
    row = expiries_df.filter(expiries_df["name"] == name).collect()[0]
    return {
        "current_week":  row["current_week_expiry"],
        "current_month": row["current_month_expiry"],
        "next_week":     row["next_week_expiry"],
        "next_month":    row["next_month_expiry"],
    }
