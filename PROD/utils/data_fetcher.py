"""
data_fetcher.py
===============
OHLC historical data fetch, Parquet write, and scheduled loop.

Responsibilities:
  - Fetch 3-minute historical candle data (OHLC + OI) from KiteConnect API
  - Write each instrument's data to partitioned Parquet files
  - Provide a scheduled loop that runs continuously every N minutes

Data path structure:
    DataFiles/HistoricalData/<interval>/<expiry>/<instrument_token>/

Usage:
    from utils.kite_helpers import kite_login, get_spark_session
    from utils.data_fetcher import run_data_loop

    kite, kws, access_token = kite_login()
    spark = get_spark_session()
    run_data_loop(kite, spark, banknifty=True, nifty=False,
                  custom_strike=56500, num_strikes=8, loop_interval_minutes=1)
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import schedule
from IPython.display import clear_output
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import asc, col
from pyspark.sql.types import (
    DateType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils1.strike_utils import (
    BANKNIFTY_INDEX_TOKEN,
    NIFTY_INDEX_TOKEN,
    get_ATM_Strike,
    get_Options_DF,
    get_expiry_dates,
    read_instruments,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Historical data path constant
# ---------------------------------------------------------------------------
HISTORICAL_DATA_BASE_PATH = "DataFiles/HistoricalData"
DEFAULT_INTERVAL = "3minute"

# ---------------------------------------------------------------------------
# Schema for historical OHLC candle data
# ---------------------------------------------------------------------------
OHLC_SCHEMA = StructType(
    [
        StructField("date",   TimestampType(), nullable=True),
        StructField("open",   StringType(),    nullable=True),
        StructField("high",   StringType(),    nullable=True),
        StructField("low",    StringType(),    nullable=True),
        StructField("close",  StringType(),    nullable=True),
        StructField("volume", StringType(),    nullable=True),
        StructField("oi",     StringType(),    nullable=True),
        StructField("day",    DateType(),      nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Core data fetch + write
# ---------------------------------------------------------------------------


def get_historical_data(
    kite,
    spark: SparkSession,
    instrument_token: int,
    from_date: datetime,
    to_date: datetime,
    interval: str = DEFAULT_INTERVAL,
) -> DataFrame:
    """Fetch historical OHLC + OI data from KiteConnect and return as a Spark DataFrame.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated KiteConnect instance.
    spark : SparkSession
    instrument_token : int
        Instrument token for the options strike.
    from_date : datetime
        Start datetime for data fetch.
    to_date : datetime
        End datetime for data fetch.
    interval : str
        Candle interval (e.g. '3minute', '5minute', 'day').

    Returns
    -------
    DataFrame
        Ordered by 'date' ascending, with 'day' column derived from 'date'.
    """
    raw_data = kite.historical_data(
        instrument_token, from_date, to_date, interval, oi=True
    )
    df = spark.createDataFrame(raw_data, schema=OHLC_SCHEMA)
    df = df.orderBy(asc("date")).withColumn("day", col("date").cast("date"))
    return df


def write_historical_data(
    df: DataFrame,
    expiry: object,
    instrument_token: int,
    to_date: datetime,
    interval: str = DEFAULT_INTERVAL,
    base_path: str = HISTORICAL_DATA_BASE_PATH,
) -> None:
    """Write OHLC DataFrame to a partitioned Parquet file.

    Writes in Delta-compatible overwrite mode, partitioned by 'day'.
    Only the partition matching to_date is overwritten (dynamic partition overwrite).

    Parameters
    ----------
    df : DataFrame
        OHLC data for one instrument.
    expiry : date
        Options expiry date (used in path).
    instrument_token : int
        Instrument token (used in path).
    to_date : datetime
        Current pull date used for replaceWhere.
    interval : str
        Candle interval string.
    base_path : str
        Root path for historical data.
    """
    path = f"{base_path}/{interval}/{expiry}/{instrument_token}"
    (
        df.coalesce(1)
        .write.format("Delta")
        .mode("overwrite")
        .partitionBy("day")
        .option("replaceWhere", f"day = '{str(to_date.date())}'")
        .parquet(path)
    )
    logger.debug("Written data to: %s", path)


# ---------------------------------------------------------------------------
# Main data pull orchestrator
# ---------------------------------------------------------------------------


def get_latest_data(
    kite,
    spark: SparkSession,
    banknifty: bool = True,
    nifty: bool = False,
    custom_strike: int = 0,
    num_strikes: int = 9,
    pull_next_expiry: bool = False,
    num_days_history: int = 1,
    interval: str = DEFAULT_INTERVAL,
    instruments_base_path: str = "PROD/DataFiles/Instruments",
) -> None:
    """Fetch and store the latest OHLC data for BankNifty and/or Nifty options.

    Reads instruments and expiry data from Parquet, determines ATM strikes,
    selects N strikes around ATM, fetches OHLC history, and writes to Parquet.

    Parameters
    ----------
    kite : KiteConnect
        Authenticated KiteConnect instance.
    spark : SparkSession
    banknifty : bool
        Whether to pull BankNifty options data.
    nifty : bool
        Whether to pull Nifty options data.
    custom_strike : int
        If > 0, use this as the ATM strike for the enabled index.
        If 0, fetch live LTP to determine ATM.
    num_strikes : int
        Number of ITM/OTM strikes to include on each side of ATM.
    pull_next_expiry : bool
        If True, also pull data for the next weekly expiry.
    num_days_history : int
        Number of calendar days of history to pull (from now).
    interval : str
        Candle interval (default '3minute').
    instruments_base_path : str
        Root path for instrument Parquet files.
    """
    (
        bnf_options, bnf_futures,
        nifty_options, nifty_futures,
        expiries_df,
    ) = read_instruments(spark, instruments_base_path)

    bnf_expiries   = get_expiry_dates(expiries_df, "BANKNIFTY")
    nifty_expiries = get_expiry_dates(expiries_df, "NIFTY")

    # Determine ATM strikes
    if custom_strike > 0:
        bnf_atm   = custom_strike if banknifty else get_ATM_Strike(kite, BANKNIFTY_INDEX_TOKEN, 100)
        nifty_atm = custom_strike if nifty     else get_ATM_Strike(kite, NIFTY_INDEX_TOKEN, 50)
    else:
        bnf_atm   = get_ATM_Strike(kite, BANKNIFTY_INDEX_TOKEN, 100)
        nifty_atm = get_ATM_Strike(kite, NIFTY_INDEX_TOKEN, 50)

    to_date   = datetime.now()
    from_date = to_date - timedelta(days=num_days_history)

    logger.info(
        "Data pull | BNF ATM=%s | NIFTY ATM=%s | from=%s | to=%s",
        bnf_atm, nifty_atm, from_date, to_date,
    )
    print(
        f"BankNifty ATM Strike = {bnf_atm} | Nifty ATM Strike = {nifty_atm} "
        f"| Data pulled at: {to_date}"
    )

    def _pull_expiry(bnf_expiry, nifty_expiry, label: str):
        """Pull data for a single expiry (current or next)."""
        bnf_opts = get_Options_DF(
            spark, bnf_options, bnf_atm, bnf_expiry, 100, num_strikes
        )
        nifty_opts = get_Options_DF(
            spark, nifty_options, nifty_atm, nifty_expiry, 50, num_strikes
        )

        bnf_tokens   = bnf_opts.select("instrument_token").orderBy("instrument_token").collect()
        nifty_tokens = nifty_opts.select("instrument_token").orderBy("instrument_token").collect()

        print(f"Pulling {label} Expiry ({bnf_expiry}) data…")

        for i, row in enumerate(bnf_tokens):
            if not banknifty:
                break
            token = row["instrument_token"]
            try:
                df = get_historical_data(kite, spark, token, from_date, to_date, interval)
                write_historical_data(df, bnf_expiry, token, to_date, interval)
            except Exception as exc:
                logger.warning("BNF token %s failed: %s", token, exc)

        for i, row in enumerate(nifty_tokens):
            if not nifty:
                break
            token = row["instrument_token"]
            try:
                df = get_historical_data(kite, spark, token, from_date, to_date, interval)
                write_historical_data(df, nifty_expiry, token, to_date, interval)
            except Exception as exc:
                logger.warning("Nifty token %s failed: %s", token, exc)

        print(f"{label} Expiry data complete.")

    _pull_expiry(
        bnf_expiries["current_week"],
        nifty_expiries["current_week"],
        "Current",
    )

    if pull_next_expiry:
        _pull_expiry(
            bnf_expiries["next_week"],
            nifty_expiries["next_week"],
            "Next",
        )


# ---------------------------------------------------------------------------
# Scheduled auto-loop
# ---------------------------------------------------------------------------


def run_data_loop(
    kite,
    spark: SparkSession,
    banknifty: bool = True,
    nifty: bool = False,
    custom_strike: int = 0,
    num_strikes: int = 9,
    pull_next_expiry: bool = False,
    num_days_history: int = 1,
    interval: str = DEFAULT_INTERVAL,
    loop_interval_minutes: int = 1,
    instruments_base_path: str = "DataFiles/Instruments",
    end_hour: Optional[int] = None,
    end_minute: Optional[int] = None,
) -> None:
    """Run the data pull in a continuous schedule loop.

    Calls get_latest_data() immediately, then schedules it to run every
    ``loop_interval_minutes`` minutes.

    The loop handles exceptions gracefully and continues running.

    Parameters
    ----------
    kite : KiteConnect
    spark : SparkSession
    banknifty : bool
    nifty : bool
    custom_strike : int
    num_strikes : int
    pull_next_expiry : bool
    num_days_history : int
    interval : str
    loop_interval_minutes : int
        How often (in minutes) to re-fetch data.
    instruments_base_path : str
    end_hour : int, optional
        24-hour clock hour at which the loop should stop (e.g. 15 for 3 PM).
        Both ``end_hour`` and ``end_minute`` must be provided to enable
        time-based exit; if either is None the loop runs indefinitely.
    end_minute : int, optional
        Minute at which the loop should stop (e.g. 30 for :30).
    """
    has_end_time = (end_hour is not None) and (end_minute is not None)

    def _past_end_time() -> bool:
        if not has_end_time:
            return False
        now = datetime.now()
        return (now.hour, now.minute) >= (end_hour, end_minute)

    def _end_time_str() -> str:
        return f"{end_hour:02d}:{end_minute:02d}"

    # If already past end time, run once and exit immediately
    if has_end_time and _past_end_time():
        now_str = datetime.now().strftime("%H:%M")
        print(f"⚠️  run_data_loop called after end time ({now_str}). Running once and exiting.")
        clear_output(wait=True)
        print(f"Running data pull at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            get_latest_data(
                kite=kite, spark=spark, banknifty=banknifty, nifty=nifty,
                custom_strike=custom_strike, num_strikes=num_strikes,
                pull_next_expiry=pull_next_expiry, num_days_history=num_days_history,
                interval=interval, instruments_base_path=instruments_base_path,
            )
            print("✅ Data pull completed.")
        except Exception as exc:
            logger.error("Data pull failed: %s", exc, exc_info=True)
            print(f"⚠️  Data pull error: {exc}")
        return

    def _job():
        # Time-based exit check at the start of each iteration
        if has_end_time and _past_end_time():
            now_str = datetime.now().strftime("%H:%M")
            print(f"⏹ run_data_loop stopped at {now_str} — end time {_end_time_str()} reached.")
            raise KeyboardInterrupt

        clear_output(wait=True)
        print(f"Running data pull at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            get_latest_data(
                kite=kite, spark=spark, banknifty=banknifty, nifty=nifty,
                custom_strike=custom_strike, num_strikes=num_strikes,
                pull_next_expiry=pull_next_expiry, num_days_history=num_days_history,
                interval=interval, instruments_base_path=instruments_base_path,
            )
            print("✅ Data pull completed.")
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.error("Data pull failed: %s", exc, exc_info=True)
            print(f"⚠️  Data pull error: {exc}")

    # Run immediately on first call
    _job()

    schedule.clear()
    schedule.every(loop_interval_minutes).minutes.do(_job)

    logger.info(
        "Scheduled data pull every %d minute(s). Press Ctrl+C to stop.",
        loop_interval_minutes,
    )

    while True:
        try:
            # Time check before running pending jobs
            if has_end_time and _past_end_time():
                now_str = datetime.now().strftime("%H:%M")
                print(f"⏹ run_data_loop stopped at {now_str} — end time {_end_time_str()} reached.")
                break
            schedule.run_pending()
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Data loop stopped.")
            break
        except Exception as exc:
            logger.error("Unexpected error in loop: %s", exc, exc_info=True)
            continue
