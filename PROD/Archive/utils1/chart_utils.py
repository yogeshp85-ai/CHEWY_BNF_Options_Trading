"""
chart_utils.py
==============
Chart data preparation and Plotly visualisation for BankNifty options.

Responsibilities:
  - Read Straddle / CE / PE OHLC data from Parquet for a given strike
  - Apply technical analytics: ROC, cumulative averages, stop-loss logic
  - Render multi-panel Plotly charts (price / ROC / OI)
  - Provide an auto-refresh loop via `schedule`

Usage:
    from utils.chart_utils import run_chart_loop

    run_chart_loop(
        spark, kite,
        banknifty_options_df=bnf_opts,
        expiry=expiry_date,
        strike_level_name='ATM',
        ce_or_pe='E',          # 'CE', 'PE', or 'E' for straddle
        num_days=2,
        loop_interval_minutes=1,
    )
"""

import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import schedule
from IPython.display import clear_output
from plotly.subplots import make_subplots
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import asc, col, lit, monotonically_increasing_id
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parquet read schema for OHLC data
# ---------------------------------------------------------------------------
OHLC_FLOAT_SCHEMA = StructType(
    [
        StructField("date",   TimestampType(), nullable=True),
        StructField("open",   FloatType(),     nullable=True),
        StructField("high",   FloatType(),     nullable=True),
        StructField("low",    FloatType(),     nullable=True),
        StructField("close",  FloatType(),     nullable=True),
        StructField("volume", IntegerType(),   nullable=True),
        StructField("oi",     FloatType(),     nullable=True),
        StructField("day",    DateType(),      nullable=True),
    ]
)

OHLC_STRADDLE_SCHEMA = StructType(
    [
        StructField("date",   TimestampType(), nullable=True),
        StructField("open",   FloatType(),     nullable=True),
        StructField("high",   FloatType(),     nullable=True),
        StructField("low",    FloatType(),     nullable=True),
        StructField("close",  FloatType(),     nullable=True),
        StructField("volume", FloatType(),     nullable=True),
        StructField("oi",     FloatType(),     nullable=True),
        StructField("day",    DateType(),      nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Strike data readers
# ---------------------------------------------------------------------------


def get_Strike_Token(
    options_df: DataFrame,
    name: str,
    expiry,
    strike_level_name: str,
    ce_or_pe: str,
) -> int:
    """Return the instrument token for a specific strike.

    Parameters
    ----------
    options_df : DataFrame
        Filtered options DataFrame (output of strike_utils.get_Options_DF).
    name : str
        Index name ('BANKNIFTY' or 'NIFTY').
    expiry : date
        Expiry date.
    strike_level_name : str
        e.g. 'ATM', 'ATM+1', 'ATM-2'.
    ce_or_pe : str
        'CE' or 'PE'.

    Returns
    -------
    int
        Instrument token.
    """
    return (
        options_df.filter(options_df["name"] == name)
        .filter(options_df["expiry"] == expiry)
        .filter(options_df["Strike_level_Name"] == strike_level_name)
        .filter(options_df["instrument_type"] == ce_or_pe)
        .select("instrument_token")
        .collect()[0]["instrument_token"]
    )


def get_Strike_OHLC_data(
    spark: SparkSession,
    options_df: DataFrame,
    name: str,
    expiry,
    strike_level_name: str,
    ce_or_pe: str,
    interval: str,
    num_days: int = 1,
    num_days_back: int = 0,
    historical_base_path: str = "DataFiles/HistoricalData",
) -> DataFrame:
    """Read OHLC data for a specific strike from Parquet.

    Supports single-leg (CE/PE) and straddle (combined CE+PE) modes.

    Parameters
    ----------
    spark : SparkSession
    options_df : DataFrame
        Options DataFrame with strike level columns.
    name : str
        'BANKNIFTY' or 'NIFTY'.
    expiry : date
    strike_level_name : str
        e.g. 'ATM', 'ATM+1', 'ATM-2'.
    ce_or_pe : str
        'CE', 'PE', or any other value for straddle (CE+PE combined).
    interval : str
        Candle interval string (e.g. '3minute').
    num_days : int
        Number of trading days of data to return.
    num_days_back : int
        Offset — 0 = latest, 1 = shift back one day, etc.
    historical_base_path : str
        Root path for HistoricalData/ parquet files.

    Returns
    -------
    DataFrame
        Enriched OHLC DataFrame ordered by date ascending.
        Adds: instrument_token, name, strike, expiry, ohlc4 columns.
        For straddle mode: also adds CE_close, PE_close, CE_oi, PE_oi.
    """
    from pyspark.sql.functions import round as spark_round

    def _read_parquet(token: int) -> DataFrame:
        path = f"{historical_base_path}/{interval}/{expiry}/{token}"
        return (
            spark.read.parquet(path, schema=OHLC_FLOAT_SCHEMA)
            .filter(col("volume") > 0)
            .distinct()
        )

    if ce_or_pe in ("CE", "PE"):
        # Single-leg mode
        leg_df = (
            options_df.filter(options_df["name"] == name)
            .filter(options_df["expiry"] == expiry)
            .filter(options_df["Strike_level_Name"] == strike_level_name)
            .filter(options_df["instrument_type"] == ce_or_pe)
        )
        token  = leg_df.collect()[0]["instrument_token"]
        strike = leg_df.collect()[0]["strike"]

        df = _read_parquet(token)
        df.createOrReplaceTempView("DF")
        df = spark.sql(
            f"""
            SELECT * FROM (
                SELECT date, open, high, low, close, oi, day,
                (( dense_rank() OVER (ORDER BY day DESC) ) - {num_days_back}) AS day_num
                FROM DF
                ORDER BY date DESC
            ) P
            WHERE day_num > 0 AND day_num <= {num_days}
            """
        )
        return (
            df.orderBy(asc("date"))
            .withColumn("instrument_token", lit(token))
            .withColumn("name", lit(name))
            .withColumn("strike", lit(strike))
            .withColumn("expiry", lit(expiry))
            .withColumn(
                "ohlc4",
                spark_round((col("open") + col("high") + col("low") + col("close")) / 4, 2),
            )
            .distinct()
        )

    else:
        # Straddle mode — combine CE + PE
        ce_opts = (
            options_df.filter(options_df["name"] == name)
            .filter(options_df["expiry"] == expiry)
            .filter(options_df["Strike_level_Name"] == strike_level_name)
            .filter(options_df["instrument_type"] == "CE")
        )
        pe_opts = (
            options_df.filter(options_df["name"] == name)
            .filter(options_df["expiry"] == expiry)
            .filter(options_df["Strike_level_Name"] == strike_level_name)
            .filter(options_df["instrument_type"] == "PE")
        )
        ce_token = ce_opts.collect()[0]["instrument_token"]
        pe_token = pe_opts.collect()[0]["instrument_token"]
        strike   = ce_opts.collect()[0]["strike"]

        ce_df = _read_parquet(ce_token).withColumn("instrument_type", lit("CE"))
        pe_df = _read_parquet(pe_token).withColumn("instrument_type", lit("PE"))
        combined = ce_df.union(pe_df)
        combined.createOrReplaceTempView("CE_PE_DF")

        combined = spark.sql(
            f"""
            SELECT * FROM (
                SELECT
                    date,
                    round(sum(open), 2)   AS open,
                    round(sum(high), 2)   AS high,
                    round(sum(low), 2)    AS low,
                    round(sum(close), 2)  AS close,
                    round(sum(CASE WHEN instrument_type='CE' THEN close ELSE NULL END), 2) AS CE_close,
                    round(sum(CASE WHEN instrument_type='PE' THEN close ELSE NULL END), 2) AS PE_close,
                    round(sum(oi), 2)     AS oi,
                    round(sum(CASE WHEN instrument_type='CE' THEN oi ELSE NULL END), 2) AS CE_oi,
                    round(sum(CASE WHEN instrument_type='PE' THEN oi ELSE NULL END), 2) AS PE_oi,
                    day,
                    (( dense_rank() OVER (ORDER BY day DESC) ) - {num_days_back}) AS day_num
                FROM CE_PE_DF
                GROUP BY day, date
                ORDER BY date DESC
            ) P
            WHERE day_num > 0 AND day_num <= {num_days}
            """
        )
        from pyspark.sql.functions import round as spark_round

        return (
            combined.orderBy(asc("date"))
            .withColumn("instrument_token", lit(""))
            .withColumn("name", lit(name))
            .withColumn("strike", lit(strike))
            .withColumn("expiry", lit(expiry))
            .withColumn(
                "ohlc4",
                spark_round((col("open") + col("high") + col("low") + col("close")) / 4, 2),
            )
            .distinct()
        )


# ---------------------------------------------------------------------------
# Technical analytics
# ---------------------------------------------------------------------------


def get_ROC_added(df: DataFrame, col_name: str = "close", length: int = 20) -> DataFrame:
    """Add a rolling Rate-of-Change (ROC) average column to the DataFrame.

    Computes the average percentage change over `length` periods.

    Parameters
    ----------
    df : DataFrame
        Must contain 'rownum' and 'strike' columns plus `col_name`.
    col_name : str
        Column to compute ROC on (e.g. 'close', 'CE_close', 'PE_close').
    length : int
        Number of lookback periods.

    Returns
    -------
    DataFrame
        Input DataFrame with a new 'ROC_AVG_<col_name>' column added.
    """
    from pyspark.sql.functions import lag

    w = Window.partitionBy("strike").orderBy("rownum")
    df = df.withColumn(f"ROC_AVG_{col_name}", lit(0))
    roc_cols = []
    for i in range(1, length + 1):
        roc_col = f"_ROC_{i}"
        df = df.withColumn(
            roc_col,
            (col(col_name) - lag(col(col_name), i).over(w))
            / lag(col(col_name), i).over(w),
        )
        df = df.withColumn(
            f"ROC_AVG_{col_name}",
            col(f"ROC_AVG_{col_name}") + col(roc_col),
        )
        roc_cols.append(roc_col)
    df = df.withColumn(f"ROC_AVG_{col_name}", col(f"ROC_AVG_{col_name}") / 2)
    df = df.drop(*roc_cols)
    return df


def compute_analytics(
    spark: SparkSession,
    df: DataFrame,
    ce_or_pe: str,
    is_latest_day: bool = True,
) -> pd.DataFrame:
    """Apply full analytics pipeline and return a pandas DataFrame ready for plotting.

    Analytics added:
      - ROC averages for close, CE_close, PE_close
      - Cumulative/incremental averages and minimums
      - Stop-loss signals
      - Entry-allowed flags
      - Open_Max_Close (for straddle)

    Parameters
    ----------
    spark : SparkSession
    df : DataFrame
        Raw OHLC DataFrame from get_Strike_OHLC_data().
    ce_or_pe : str
        'CE', 'PE', or straddle ('E' or any other value).
    is_latest_day : bool
        If True, filter to show only the latest trading day.

    Returns
    -------
    pd.DataFrame
        Analytics-enriched pandas DataFrame sorted by rownum ascending.
    """
    is_straddle = ce_or_pe not in ("CE", "PE")

    df = df.filter(df["date"].isNotNull())
    if is_straddle:
        df = df.filter(df.CE_close.isNotNull() & df.PE_close.isNotNull())

    df = df.withColumn("rownum", monotonically_increasing_id())
    df = get_ROC_added(df, col_name="close", length=20)
    if is_straddle:
        df = get_ROC_added(df, col_name="CE_close", length=20)
        df = get_ROC_added(df, col_name="PE_close", length=20)

    df.createOrReplaceTempView("df_raw")
    df = spark.sql(
        """
        SELECT *,
            (CASE WHEN day_num = (SELECT MIN(day_num) FROM df_raw) THEN 1 ELSE 0 END) AS IS_LATEST_DAY
        FROM df_raw
        """
    )

    if is_latest_day:
        df = df.filter(df["IS_LATEST_DAY"] == True)  # noqa: E712
        df = df.withColumn("rownum", monotonically_increasing_id())

    if is_straddle:
        df = df.withColumn(
            "ROC_AVG_CEPE_AVG",
            (df["ROC_AVG_CE_close"] + df["ROC_AVG_PE_close"]) / 2,
        )

    w_cum = Window.partitionBy("strike").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    w_day = Window.partitionBy("strike", "day_num").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

    df = (df
        .withColumn("cumulative_sum_close",     F.sum("close").over(w_cum))
        .withColumn("cumulative_min_close",      F.min("close").over(w_cum))
        .withColumn("cumulative_count",          F.count("close").over(w_cum))
        .withColumn("cumulative_sum_oi",         F.sum("oi").over(w_cum))
        .withColumn("incremental_close_avg",     col("cumulative_sum_close") / col("cumulative_count"))
        .withColumn("incremental_oi_avg",        col("cumulative_sum_oi") / col("cumulative_count"))
        .withColumn("incremental_min_close",     col("cumulative_min_close"))
        .withColumn("MAX_incremental_close_avg", F.max("incremental_close_avg").over(w_cum))
        .withColumn("MidPoint", lit(0))
    )

    if is_straddle:
        df = (df
            .withColumn("cumulative_sum_CE_close",  F.sum("CE_close").over(w_cum))
            .withColumn("cumulative_sum_PE_close",  F.sum("PE_close").over(w_cum))
            .withColumn("incremental_CE_close_avg", col("cumulative_sum_CE_close") / col("cumulative_count"))
            .withColumn("incremental_PE_close_avg", col("cumulative_sum_PE_close") / col("cumulative_count"))
            .withColumn("cumulative_MAX_CE_close",  F.min("CE_close").over(w_cum))
            .withColumn("cumulative_MAX_PE_close",  F.min("PE_close").over(w_cum))
        )
        # Stop-loss logic
        df = df.withColumn(
            "StopLoss",
            F.when(
                (df.CE_close > df.incremental_CE_close_avg)
                & (df.CE_close > df.incremental_PE_close_avg)
                & (df.ROC_AVG_close > 0),
                df.high,
            )
            .when(
                (df.PE_close > df.incremental_CE_close_avg)
                & (df.PE_close > df.incremental_PE_close_avg)
                & (df.ROC_AVG_close > 0),
                df.high,
            )
            .otherwise(df.MAX_incremental_close_avg),
        )
        df = df.withColumn(
            "StopLoss",
            F.when(
                (df.CE_close == df.cumulative_MAX_CE_close)
                & (df.CE_close > df.PE_close)
                & (df.CE_oi < df.PE_oi),
                df.high,
            )
            .when(
                (df.PE_close == df.cumulative_MAX_PE_close)
                & (df.PE_close > df.CE_close)
                & (df.CE_oi > df.PE_oi),
                df.high,
            )
            .otherwise(df.StopLoss),
        )
        df = df.withColumn(
            "StopLoss",
            F.when(df.StopLoss > df.MAX_incremental_close_avg, df.MAX_incremental_close_avg).otherwise(df.StopLoss),
        )
        df = df.withColumn(
            "StopLoss",
            F.when(
                df.StopLoss
                < ((df.incremental_min_close + df.incremental_close_avg) / 2),
                (df.incremental_min_close + df.incremental_close_avg + df.StopLoss) / 3,
            ).otherwise(df.StopLoss),
        )
        df = df.withColumn("StopLoss", F.min("StopLoss").over(w_day))

        # Entry-allowed logic
        df = (df
            .withColumn("CE_close_On_Open", F.first("CE_close").over(w_day))
            .withColumn("PE_close_On_Open", F.first("PE_close").over(w_day))
            .withColumn(
                "Open_Max_Close",
                F.when(df.CE_close_On_Open >= df.PE_close_On_Open, df.CE_close_On_Open).otherwise(df.PE_close_On_Open),
            )
            .withColumn(
                "EntryAllowed",
                F.when(
                    ((df.CE_close > df.Open_Max_Close) | (df.PE_close > df.Open_Max_Close))
                    & (df.close > ((df.incremental_min_close + df.incremental_close_avg) / 2)),
                    lit(None),
                ).otherwise(df.close),
            )
        )

    # Convert to pandas
    df_pd = df.repartition(1).toPandas()
    for num_col in ["rownum", "open", "high", "low", "close", "oi"]:
        if num_col in df_pd.columns:
            df_pd[num_col] = pd.to_numeric(df_pd[num_col])
    df_pd["date"] = pd.to_datetime(df_pd["date"])

    if is_straddle:
        df_pd["CE_close"] = pd.to_numeric(df_pd["CE_close"])
        df_pd["PE_close"] = pd.to_numeric(df_pd["PE_close"])

    df_pd.sort_values("rownum", ascending=True, inplace=True)
    return df_pd


# ---------------------------------------------------------------------------
# Plotly chart rendering
# ---------------------------------------------------------------------------


def plot_Chart(df_pandas: pd.DataFrame, strike: float, strike_level_name: str, ce_or_pe: str) -> None:
    """Render multi-panel Plotly charts for a BankNifty options strike.

    Panel layout:
      Figure 1 (2×2 or 2×1):
        - Row 1 Col 1: Straddle close + trailing SL + StopLoss + AVG + EntryAllowed
        - Row 1 Col 2: ROC_AVG (rate of change)
        - Row 2 Col 1: CE close vs PE close (straddle only)
        - Row 2 Col 2: CE/PE ROC (straddle only)
      Figure 2 (OI):
        - Combined OI + CE OI / PE OI
      Figure 3 (Candlestick):
        - OHLC candlestick chart

    Parameters
    ----------
    df_pandas : pd.DataFrame
        Analytics-enriched DataFrame from compute_analytics().
    strike : float
        Strike price (for chart title).
    strike_level_name : str
        e.g. 'ATM', 'ATM+1'.
    ce_or_pe : str
        'CE', 'PE', or 'E' for straddle.
    """
    is_straddle = ce_or_pe not in ("CE", "PE")
    now_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    title_suffix = f"Strike: ({strike_level_name}) / {strike}  —  Pulled at {now_str}"

    # ── Figure 1: Price + ROC ────────────────────────────────────────────────
    if is_straddle:
        fig = make_subplots(rows=2, cols=2)
    else:
        fig = make_subplots(rows=2, cols=1)

    fig.add_trace(
        go.Scatter(x=df_pandas["rownum"], y=df_pandas["close"], mode="lines",
                   name="Straddle Close", line=dict(color="black", width=2)), row=1, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=df_pandas["rownum"],
            y=(df_pandas["incremental_min_close"] + df_pandas["incremental_close_avg"]) / 2,
            mode="lines", name="Trailing SL", line=dict(color="red", width=2),
        ), row=1, col=1,
    )
    if "StopLoss" in df_pandas.columns:
        fig.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["StopLoss"], mode="lines",
                       name="StopLoss", line=dict(color="blue", width=2)), row=1, col=1
        )
    fig.add_trace(
        go.Scatter(x=df_pandas["rownum"], y=df_pandas["incremental_close_avg"], mode="lines",
                   name="Straddle AVG", line=dict(color="gray", width=2)), row=1, col=1
    )
    if "EntryAllowed" in df_pandas.columns:
        fig.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["EntryAllowed"], mode="lines",
                       name="Entry Allowed", line=dict(color="green", width=3)), row=1, col=1
        )

    # ROC panel
    fig.add_trace(
        go.Scatter(x=df_pandas["rownum"], y=df_pandas["MidPoint"], mode="lines", name="MidLine"),
        row=1, col=2 if is_straddle else 1,
    )
    fig.add_trace(
        go.Scatter(x=df_pandas["rownum"], y=df_pandas["ROC_AVG_close"], mode="lines",
                   name="ROC AVG", line=dict(color="black", width=2)),
        row=1, col=2 if is_straddle else 1,
    )

    # Straddle-only CE/PE panels
    if is_straddle:
        for col_name, label, c in [("CE_close", "CALL", "red"), ("PE_close", "PUT", "green")]:
            fig.add_trace(
                go.Scatter(x=df_pandas["rownum"], y=df_pandas[col_name], mode="lines",
                           name=label, line=dict(color=c, width=2)), row=2, col=1
            )
        fig.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["incremental_CE_close_avg"], mode="lines",
                       name="CE AVG", line=dict(color="red", width=1)), row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["incremental_PE_close_avg"], mode="lines",
                       name="PE AVG", line=dict(color="green", width=1)), row=2, col=1
        )
        if "Open_Max_Close" in df_pandas.columns:
            fig.add_trace(
                go.Scatter(x=df_pandas["rownum"], y=df_pandas["Open_Max_Close"], mode="lines",
                           name="Open Max Close", line=dict(color="black", width=2)), row=2, col=1
            )
        fig.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["MidPoint"], mode="lines", name="MidLine"),
            row=2, col=2,
        )
        for col_name, label, c in [("ROC_AVG_CE_close", "CE ROC AVG", "red"), ("ROC_AVG_PE_close", "PE ROC AVG", "green")]:
            fig.add_trace(
                go.Scatter(x=df_pandas["rownum"], y=df_pandas[col_name], mode="lines",
                           name=label, line=dict(color=c, width=2)), row=2, col=2
            )

    fig.update_layout(
        height=600, width=1200,
        title_text=f"Price & ROC — {title_suffix}",
        paper_bgcolor="gray",
        margin=dict(l=50, r=50, b=50, t=50),
    )
    fig.show()

    # ── Figure 2: OI ─────────────────────────────────────────────────────────
    if is_straddle:
        fig2 = make_subplots(rows=1, cols=2)
    else:
        fig2 = make_subplots(rows=1, cols=1)

    fig2.add_trace(
        go.Scatter(x=df_pandas["rownum"], y=df_pandas["oi"], mode="lines", name="Combined OI"),
        row=1, col=1,
    )
    fig2.add_trace(
        go.Scatter(x=df_pandas["rownum"], y=df_pandas["incremental_oi_avg"], mode="lines",
                   name="OI AVG", line=dict(color="gray", width=2)), row=1, col=1
    )
    if is_straddle:
        fig2.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["CE_oi"], mode="lines",
                       name="CALL OI", line=dict(color="red", width=2)), row=1, col=2
        )
        fig2.add_trace(
            go.Scatter(x=df_pandas["rownum"], y=df_pandas["PE_oi"], mode="lines",
                       name="PUT OI", line=dict(color="green", width=2)), row=1, col=2
        )

    fig2.update_layout(
        height=400, width=1200,
        title_text=f"Open Interest — {title_suffix}",
        paper_bgcolor="gray",
        margin=dict(l=50, r=50, b=50, t=50),
    )
    fig2.show()

    # ── Figure 3: Candlestick ────────────────────────────────────────────────
    fig3 = go.Figure(
        data=[
            go.Candlestick(
                x=df_pandas["date"],
                open=df_pandas["open"],
                high=df_pandas["high"],
                low=df_pandas["low"],
                close=df_pandas["close"],
            )
        ]
    )
    fig3.update_layout(
        height=600, width=1200,
        title_text=f"OHLC Candlestick — {title_suffix}",
        xaxis_rangeslider_visible=False,
    )
    fig3.show()


# ---------------------------------------------------------------------------
# High-level chart orchestrator
# ---------------------------------------------------------------------------


def get_chart(
    spark: SparkSession,
    options_df: DataFrame,
    expiry,
    strike_level_name: str,
    ce_or_pe: str,
    interval: str,
    num_days: int = 2,
    num_days_back: int = 0,
    is_latest_day: bool = True,
    historical_base_path: str = "DataFiles/HistoricalData",
) -> None:
    """Fetch, compute analytics, and render all charts for one strike.

    Parameters
    ----------
    spark : SparkSession
    options_df : DataFrame
        Filtered options DataFrame for the desired expiry.
    expiry : date
    strike_level_name : str
    ce_or_pe : str
        'CE', 'PE', or 'E' for straddle.
    interval : str
    num_days : int
    num_days_back : int
    is_latest_day : bool
    historical_base_path : str
    """
    cols_single  = ["date", "open", "high", "low", "close", "oi", "strike", "day_num"]
    cols_straddle = ["date", "open", "high", "low", "close",
                     "CE_close", "PE_close", "oi", "CE_oi", "PE_oi", "strike", "day_num"]

    is_straddle = ce_or_pe not in ("CE", "PE")
    select_cols = cols_straddle if is_straddle else cols_single

    raw_df = get_Strike_OHLC_data(
        spark=spark,
        options_df=options_df,
        name="BANKNIFTY",
        expiry=expiry,
        strike_level_name=strike_level_name,
        ce_or_pe=ce_or_pe,
        interval=interval,
        num_days=num_days,
        num_days_back=num_days_back,
        historical_base_path=historical_base_path,
    ).select(select_cols)

    df_pd = compute_analytics(spark, raw_df, ce_or_pe, is_latest_day)
    strike = df_pd.iloc[0]["strike"]

    clear_output(wait=True)
    plot_Chart(df_pd, strike, strike_level_name, ce_or_pe)


# ---------------------------------------------------------------------------
# Scheduled auto-refresh loop
# ---------------------------------------------------------------------------


def run_chart_loop(
    spark: SparkSession,
    options_df: DataFrame,
    expiry,
    strike_level_name: str = "ATM",
    ce_or_pe: str = "E",
    interval: str = "3minute",
    num_days: int = 2,
    is_latest_day: bool = True,
    loop_interval_minutes: int = 1,
    historical_base_path: str = "DataFiles/HistoricalData",
) -> None:
    """Render charts and auto-refresh them on a schedule.

    Parameters
    ----------
    spark : SparkSession
    options_df : DataFrame
    expiry : date
    strike_level_name : str
    ce_or_pe : str
    interval : str
    num_days : int
    is_latest_day : bool
    loop_interval_minutes : int
    historical_base_path : str
    """
    def _job():
        try:
            get_chart(
                spark=spark,
                options_df=options_df,
                expiry=expiry,
                strike_level_name=strike_level_name,
                ce_or_pe=ce_or_pe,
                interval=interval,
                num_days=num_days,
                is_latest_day=is_latest_day,
                historical_base_path=historical_base_path,
            )
        except Exception as exc:
            logger.error("Chart refresh failed: %s", exc, exc_info=True)
            print(f"⚠️  Chart refresh error: {exc}")

    _job()
    schedule.clear()
    schedule.every(loop_interval_minutes).minutes.do(_job)

    while True:
        try:
            schedule.run_pending()
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Chart loop stopped by user.")
            break
        except Exception as exc:
            logger.error("Unexpected error in chart loop: %s", exc, exc_info=True)
            continue
