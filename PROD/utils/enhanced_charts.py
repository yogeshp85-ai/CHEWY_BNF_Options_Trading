"""
enhanced_charts.py
==================
Enhanced Plotly visualisation for BankNifty options with time-based x-axis.

This module provides an alternative charting experience compared to the
original ``chart_utils.plot_Chart``.  Key differences:

  - **Time-based x-axis** — candle timestamps (HH:MM) instead of row numbers
  - **Dark-themed Plotly** layout with gridlines and hover crosshairs
  - **Additional computed columns**: Straddle Premium Decay %, CE/PE Ratio,
    OI Put-Call Ratio, and Bollinger-style bands on the straddle close
  - **Shared x-axis zoom** across all sub-plots for synchronised analysis
  - **Candlestick chart** with volume overlay

The module re-uses ``compute_analytics`` and ``get_Strike_OHLC_data`` from
``chart_utils`` so the underlying data pipeline is unchanged.

Usage:
    from utils.enhanced_charts import run_enhanced_chart_loop

    run_enhanced_chart_loop(
        spark, options_df,
        expiry=expiry_date,
        strike_level_name='ATM',
        ce_or_pe='E',
        num_days=2,
        loop_interval_minutes=1,
    )
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import schedule
from IPython.display import clear_output
from plotly.subplots import make_subplots
from pyspark.sql import DataFrame, SparkSession

from utils.chart_utils import compute_analytics, get_Strike_OHLC_data

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Colour palette (dark theme)
# ---------------------------------------------------------------------------
COLORS = {
    "bg":              "#1e1e2f",
    "paper":           "#1e1e2f",
    "grid":            "#2e2e44",
    "text":            "#d4d4dc",
    "straddle_close":  "#00e5ff",
    "trailing_sl":     "#ff5252",
    "stoploss":        "#7c4dff",
    "avg":             "#ffd740",
    "entry_allowed":   "#69f0ae",
    "roc_avg":         "#e0e0e0",
    "midline":         "rgba(255,255,255,0.25)",
    "ce_close":        "#ff6e6e",
    "pe_close":        "#4caf50",
    "ce_avg":          "#ff8a80",
    "pe_avg":          "#a5d6a7",
    "open_max_close":  "#ffffff",
    "ce_roc":          "#ff8a80",
    "pe_roc":          "#a5d6a7",
    "oi_combined":     "#448aff",
    "oi_avg":          "#90a4ae",
    "ce_oi":           "#ff6e6e",
    "pe_oi":           "#4caf50",
    "candle_up":       "#26a69a",
    "candle_down":     "#ef5350",
    "bb_fill":         "rgba(0,229,255,0.08)",
    "bb_line":         "rgba(0,229,255,0.35)",
    "decay_line":      "#ce93d8",
    "ratio_line":      "#ffab40",
    "pcr_line":        "#80deea",
}


# ---------------------------------------------------------------------------
# Helper: convert rownum → IST time string
# ---------------------------------------------------------------------------

def _add_time_column(df_pd: pd.DataFrame) -> pd.DataFrame:
    """Add a ``time_str`` column mapping each row to its IST market time.

    If the DataFrame already has a ``date`` column with valid timestamps,
    we use those directly.  Otherwise we synthesise times starting at
    09:15 with 3-minute intervals.

    Returns the DataFrame with ``time_str`` (str) and ``time_dt`` (datetime)
    columns added.
    """
    df = df_pd.copy()

    if "date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["time_dt"] = df["date"]
        df["time_str"] = df["time_dt"].dt.strftime("%H:%M")
    else:
        # Fallback: synthesise from rownum
        base = datetime(2026, 1, 1, 9, 15)
        df["time_dt"] = df["rownum"].apply(lambda r: base + timedelta(minutes=int(r) * 3))
        df["time_str"] = df["time_dt"].dt.strftime("%H:%M")

    return df


def _add_extra_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute additional analytics columns on the pandas DataFrame.

    New columns:
      - ``premium_decay_pct``: % change of straddle close from the open
      - ``ce_pe_ratio``: CE_close / PE_close (straddle only)
      - ``oi_pcr``: PE_oi / CE_oi (straddle only)
      - ``bb_upper`` / ``bb_lower``: 20-period Bollinger bands on close
    """
    df = df.copy()

    # Premium decay %
    first_close = df["close"].iloc[0] if len(df) > 0 else 1
    df["premium_decay_pct"] = ((df["close"] - first_close) / first_close) * 100

    # Bollinger bands (20-period, 2 std)
    df["bb_mid"] = df["close"].rolling(window=20, min_periods=1).mean()
    bb_std = df["close"].rolling(window=20, min_periods=1).std().fillna(0)
    df["bb_upper"] = df["bb_mid"] + 2 * bb_std
    df["bb_lower"] = df["bb_mid"] - 2 * bb_std

    # Straddle-specific
    if "CE_close" in df.columns and "PE_close" in df.columns:
        df["ce_pe_ratio"] = (df["CE_close"] / df["PE_close"]).replace([np.inf, -np.inf], np.nan)
    if "CE_oi" in df.columns and "PE_oi" in df.columns:
        df["oi_pcr"] = (df["PE_oi"] / df["CE_oi"]).replace([np.inf, -np.inf], np.nan)

    return df


# ---------------------------------------------------------------------------
# Shared layout helper
# ---------------------------------------------------------------------------

def _apply_dark_layout(fig: go.Figure, title: str, height: int = 700, width: int = 1400) -> None:
    """Apply a consistent dark-themed layout to a Plotly figure."""
    fig.update_layout(
        height=height,
        width=width,
        title=dict(text=title, font=dict(size=16, color=COLORS["text"]), x=0.5),
        paper_bgcolor=COLORS["paper"],
        plot_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"], size=11),
        legend=dict(
            bgcolor="rgba(30,30,47,0.85)",
            bordercolor="#444",
            borderwidth=1,
            font=dict(size=10, color=COLORS["text"]),
        ),
        hovermode="x unified",
        margin=dict(l=60, r=30, t=50, b=50),
    )
    fig.update_xaxes(
        showgrid=True, gridcolor=COLORS["grid"], gridwidth=0.5,
        tickfont=dict(size=10), tickangle=-45,
    )
    fig.update_yaxes(
        showgrid=True, gridcolor=COLORS["grid"], gridwidth=0.5,
        tickfont=dict(size=10),
    )


# ---------------------------------------------------------------------------
# Main plotting function
# ---------------------------------------------------------------------------

def plot_enhanced_chart(
    df_pandas: pd.DataFrame,
    strike: float,
    strike_level_name: str,
    ce_or_pe: str,
) -> None:
    """Render enhanced multi-panel Plotly charts with time-based x-axis.

    Figures produced:
      1. **Price & Signals** (2×2 for straddle, 2×1 for single leg)
         - Straddle close + Bollinger bands + SL + AVG + Entry
         - ROC AVG + Premium Decay %
         - CE vs PE close + averages (straddle)
         - CE/PE ROC + CE/PE Ratio (straddle)
      2. **Open Interest** (1×2 for straddle, 1×1 for single)
         - Combined OI + OI AVG
         - CE OI vs PE OI + OI PCR (straddle)
      3. **Candlestick** with volume colour-coded bars

    Parameters
    ----------
    df_pandas : pd.DataFrame
        Analytics-enriched DataFrame from ``compute_analytics()``.
    strike : float
    strike_level_name : str
    ce_or_pe : str
    """
    is_straddle = ce_or_pe not in ("CE", "PE")
    now_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    title_base = f"Strike: ({strike_level_name}) / {strike}  —  {now_str}"

    # Enrich data
    df = _add_time_column(df_pandas)
    df = _add_extra_analytics(df)
    x = df["time_str"]

    # ── Figure 1: Price & Signals ─────────────────────────────────────────
    n_rows, n_cols = (2, 2) if is_straddle else (2, 1)
    row_heights = [0.55, 0.45]
    subplot_titles = (
        ["Straddle Price & Signals", "ROC & Premium Decay",
         "CE vs PE Close", "CE / PE ROC & Ratio"]
        if is_straddle
        else ["Price & Signals", "ROC & Premium Decay"]
    )

    fig = make_subplots(
        rows=n_rows, cols=n_cols,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
        vertical_spacing=0.10,
        horizontal_spacing=0.06,
    )

    # --- Row 1, Col 1: Straddle close + bands + SL + AVG + Entry ----------
    # Bollinger band fill
    fig.add_trace(go.Scatter(
        x=x, y=df["bb_upper"], mode="lines", line=dict(width=0),
        showlegend=False, hoverinfo="skip",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=x, y=df["bb_lower"], mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor=COLORS["bb_fill"],
        showlegend=False, hoverinfo="skip",
    ), row=1, col=1)
    # BB lines
    fig.add_trace(go.Scatter(
        x=x, y=df["bb_upper"], mode="lines", name="BB Upper",
        line=dict(color=COLORS["bb_line"], width=1, dash="dot"),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=x, y=df["bb_lower"], mode="lines", name="BB Lower",
        line=dict(color=COLORS["bb_line"], width=1, dash="dot"),
    ), row=1, col=1)

    # Straddle close
    fig.add_trace(go.Scatter(
        x=x, y=df["close"], mode="lines", name="Straddle Close",
        line=dict(color=COLORS["straddle_close"], width=2.5),
    ), row=1, col=1)

    # Trailing SL
    trailing_sl = (df["incremental_min_close"] + df["incremental_close_avg"]) / 2
    fig.add_trace(go.Scatter(
        x=x, y=trailing_sl, mode="lines", name="Trailing SL",
        line=dict(color=COLORS["trailing_sl"], width=2, dash="dash"),
    ), row=1, col=1)

    # StopLoss
    if "StopLoss" in df.columns:
        fig.add_trace(go.Scatter(
            x=x, y=df["StopLoss"], mode="lines", name="StopLoss",
            line=dict(color=COLORS["stoploss"], width=2),
        ), row=1, col=1)

    # Straddle AVG
    fig.add_trace(go.Scatter(
        x=x, y=df["incremental_close_avg"], mode="lines", name="Straddle AVG",
        line=dict(color=COLORS["avg"], width=1.5, dash="dot"),
    ), row=1, col=1)

    # Entry Allowed
    if "EntryAllowed" in df.columns:
        fig.add_trace(go.Scatter(
            x=x, y=df["EntryAllowed"], mode="markers+lines", name="Entry Allowed",
            line=dict(color=COLORS["entry_allowed"], width=2),
            marker=dict(size=4, color=COLORS["entry_allowed"]),
        ), row=1, col=1)

    # --- Row 1, Col 2 (or Row 2, Col 1 for single): ROC + Decay ----------
    roc_col = 2 if is_straddle else 1
    roc_row = 1 if is_straddle else 2

    fig.add_trace(go.Scatter(
        x=x, y=[0] * len(df), mode="lines", name="Zero Line",
        line=dict(color=COLORS["midline"], width=1),
        showlegend=False,
    ), row=roc_row, col=roc_col)

    fig.add_trace(go.Scatter(
        x=x, y=df["ROC_AVG_close"], mode="lines", name="ROC AVG",
        line=dict(color=COLORS["roc_avg"], width=2),
    ), row=roc_row, col=roc_col)

    fig.add_trace(go.Scatter(
        x=x, y=df["premium_decay_pct"], mode="lines", name="Premium Decay %",
        line=dict(color=COLORS["decay_line"], width=1.5, dash="dash"),
    ), row=roc_row, col=roc_col)

    # --- Straddle-only panels (Row 2) -------------------------------------
    if is_straddle:
        # Row 2, Col 1: CE vs PE close
        fig.add_trace(go.Scatter(
            x=x, y=df["CE_close"], mode="lines", name="CALL",
            line=dict(color=COLORS["ce_close"], width=2),
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=x, y=df["PE_close"], mode="lines", name="PUT",
            line=dict(color=COLORS["pe_close"], width=2),
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=x, y=df["incremental_CE_close_avg"], mode="lines", name="CE AVG",
            line=dict(color=COLORS["ce_avg"], width=1, dash="dot"),
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=x, y=df["incremental_PE_close_avg"], mode="lines", name="PE AVG",
            line=dict(color=COLORS["pe_avg"], width=1, dash="dot"),
        ), row=2, col=1)
        if "Open_Max_Close" in df.columns:
            fig.add_trace(go.Scatter(
                x=x, y=df["Open_Max_Close"], mode="lines", name="Open Max Close",
                line=dict(color=COLORS["open_max_close"], width=2, dash="dashdot"),
            ), row=2, col=1)

        # Row 2, Col 2: CE/PE ROC + Ratio
        fig.add_trace(go.Scatter(
            x=x, y=[0] * len(df), mode="lines",
            line=dict(color=COLORS["midline"], width=1),
            showlegend=False,
        ), row=2, col=2)
        fig.add_trace(go.Scatter(
            x=x, y=df["ROC_AVG_CE_close"], mode="lines", name="CE ROC AVG",
            line=dict(color=COLORS["ce_roc"], width=2),
        ), row=2, col=2)
        fig.add_trace(go.Scatter(
            x=x, y=df["ROC_AVG_PE_close"], mode="lines", name="PE ROC AVG",
            line=dict(color=COLORS["pe_roc"], width=2),
        ), row=2, col=2)
        if "ce_pe_ratio" in df.columns:
            fig.add_trace(go.Scatter(
                x=x, y=df["ce_pe_ratio"], mode="lines", name="CE/PE Ratio",
                line=dict(color=COLORS["ratio_line"], width=1.5, dash="dash"),
                yaxis="y",  # shares same y-axis; values are small
            ), row=2, col=2)

    _apply_dark_layout(fig, f"Price & Signals — {title_base}", height=700, width=1400)
    fig.show()

    # ── Figure 2: Open Interest ───────────────────────────────────────────
    oi_cols = 2 if is_straddle else 1
    oi_titles = (
        ["Combined OI", "CE / PE OI & PCR"]
        if is_straddle
        else ["Open Interest"]
    )
    fig2 = make_subplots(rows=1, cols=oi_cols, subplot_titles=oi_titles)

    fig2.add_trace(go.Scatter(
        x=x, y=df["oi"], mode="lines", name="Combined OI",
        line=dict(color=COLORS["oi_combined"], width=2),
    ), row=1, col=1)
    fig2.add_trace(go.Scatter(
        x=x, y=df["incremental_oi_avg"], mode="lines", name="OI AVG",
        line=dict(color=COLORS["oi_avg"], width=1.5, dash="dot"),
    ), row=1, col=1)

    if is_straddle:
        fig2.add_trace(go.Scatter(
            x=x, y=df["CE_oi"], mode="lines", name="CALL OI",
            line=dict(color=COLORS["ce_oi"], width=2),
        ), row=1, col=2)
        fig2.add_trace(go.Scatter(
            x=x, y=df["PE_oi"], mode="lines", name="PUT OI",
            line=dict(color=COLORS["pe_oi"], width=2),
        ), row=1, col=2)
        if "oi_pcr" in df.columns:
            fig2.add_trace(go.Scatter(
                x=x, y=df["oi_pcr"], mode="lines", name="OI PCR (PE/CE)",
                line=dict(color=COLORS["pcr_line"], width=1.5, dash="dash"),
            ), row=1, col=2)

    _apply_dark_layout(fig2, f"Open Interest — {title_base}", height=400, width=1400)
    fig2.show()

    # ── Figure 3: Candlestick with volume bars ────────────────────────────
    fig3 = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
        subplot_titles=["OHLC Candlestick", "Volume"],
    )

    fig3.add_trace(go.Candlestick(
        x=x,
        open=df["open"], high=df["high"], low=df["low"], close=df["close"],
        increasing_line_color=COLORS["candle_up"],
        decreasing_line_color=COLORS["candle_down"],
        name="OHLC",
    ), row=1, col=1)

    # Volume bars (colour by direction)
    if "volume" in df.columns:
        vol_colors = [
            COLORS["candle_up"] if c >= o else COLORS["candle_down"]
            for c, o in zip(df["close"], df["open"])
        ]
        fig3.add_trace(go.Bar(
            x=x, y=df["volume"], name="Volume",
            marker_color=vol_colors, opacity=0.6,
        ), row=2, col=1)

    _apply_dark_layout(fig3, f"OHLC Candlestick — {title_base}", height=600, width=1400)
    fig3.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig3.show()


# ---------------------------------------------------------------------------
# High-level chart orchestrator (mirrors get_chart from chart_utils)
# ---------------------------------------------------------------------------

def get_enhanced_chart(
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
    """Fetch data, compute analytics, and render enhanced charts.

    Parameters mirror ``chart_utils.get_chart`` exactly so this is a
    drop-in replacement.
    """
    cols_single = ["date", "open", "high", "low", "close", "oi", "strike", "day_num"]
    cols_straddle = [
        "date", "open", "high", "low", "close",
        "CE_close", "PE_close", "oi", "CE_oi", "PE_oi", "strike", "day_num",
    ]

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
    plot_enhanced_chart(df_pd, strike, strike_level_name, ce_or_pe)


# ---------------------------------------------------------------------------
# Scheduled auto-refresh loop
# ---------------------------------------------------------------------------

def run_enhanced_chart_loop(
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
    """Render enhanced charts and auto-refresh on a schedule.

    Parameters mirror ``chart_utils.run_chart_loop`` exactly so this is a
    drop-in replacement.
    """
    def _job():
        try:
            get_enhanced_chart(
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
            logger.error("Enhanced chart refresh failed: %s", exc, exc_info=True)
            print(f"⚠️  Enhanced chart refresh error: {exc}")

    _job()
    schedule.clear()
    schedule.every(loop_interval_minutes).minutes.do(_job)

    while True:
        try:
            schedule.run_pending()
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Enhanced chart loop stopped by user.")
            break
        except Exception as exc:
            logger.error("Unexpected error in enhanced chart loop: %s", exc, exc_info=True)
            continue
