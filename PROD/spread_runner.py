"""
spread_runner.py
================
Chart worker for Bull Call Spread and Bear Put Spread tabs.

Reads two option legs (Long + Short) from the same data pipeline used by
the Straddle tab (Step 2), computes spread OHLC, and renders 8 interactive
Plotly charts per tab.
"""

import logging
import math
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from PIL import Image
from plotly.subplots import make_subplots
from pyspark.sql import functions as F
from pyspark.sql.functions import asc, col, lit, monotonically_increasing_id
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window
from PyQt5.QtCore import QThread, pyqtSignal

import utils.enhanced_charts_v3 as ec3
from utils.chart_utils import OHLC_FLOAT_SCHEMA, get_ROC_added
from utils.strike_utils import BANKNIFTY_INDEX_TOKEN, get_ATM_Strike, get_Options_DF

logger = logging.getLogger(__name__)


class SpreadChartWorker(QThread):
    """Step 3 chart worker for Bull Call / Bear Put Spread tabs."""

    log_signal = pyqtSignal(str)
    chart_ready_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)

    def __init__(self, kite, spark, options_df, expiries, config, spread_type):
        """
        Parameters
        ----------
        spread_type : str
            "bull_call" or "bear_put"
        """
        super().__init__()
        self.kite = kite
        self.spark = spark
        self.options_df = options_df
        self.expiries = expiries
        self.config = config
        self.spread_type = spread_type
        self._is_running = True
        self._force_snapshot = False

        # Derived labels
        self.leg_type = "CE" if spread_type == "bull_call" else "PE"
        self.spread_name = "Bull Call Spread" if spread_type == "bull_call" else "Bear Put Spread"
        self.long_label = f"Long {self.leg_type}"
        self.short_label = f"Short {self.leg_type}"

    def trigger_snapshot(self):
        self._force_snapshot = True

    # ------------------------------------------------------------------
    # Parquet / token helpers
    # ------------------------------------------------------------------

    def _read_parquet(self, token, expiry):
        path = f"DataFiles/HistoricalData/3minute/{expiry}/{token}"
        try:
            self.spark.catalog.refreshByPath(path)
        except Exception:
            pass
        return (
            self.spark.read.parquet(path, schema=OHLC_FLOAT_SCHEMA)
            .filter(col("volume") > 0)
            .distinct()
        )

    def _get_token_for_strike(self, strike_value, expiry):
        rows = (
            self.options_df
            .filter(col("name") == "BANKNIFTY")
            .filter(col("expiry") == expiry)
            .filter(col("strike") == float(strike_value))
            .filter(col("instrument_type") == self.leg_type)
            .collect()
        )
        return rows[0]["instrument_token"] if rows else None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        self.log_signal.emit(f"📈 {self.spread_name} Chart Loop started.")
        while self._is_running:
            try:
                if self._past_end_time():
                    self.log_signal.emit(f"⏹ {self.spread_name} chart loop auto-stopped.")
                    break

                self.log_signal.emit(f"Rendering {self.spread_name} at {datetime.now().strftime('%H:%M:%S')}...")

                # 1. ATM strike
                bnf_atm = (self.config.CUSTOM_STRIKE
                           if self.config.CUSTOM_STRIKE > 0
                           else get_ATM_Strike(self.kite, BANKNIFTY_INDEX_TOKEN, 100))
                long_strike = bnf_atm
                expiry = self.expiries["current_week"]

                # 2. Long leg token
                long_token = self._get_token_for_strike(long_strike, expiry)
                if not long_token:
                    self.log_signal.emit(f"⚠️ {self.long_label} not found for {long_strike}. Retrying…")
                    time.sleep(5); continue

                # 3. Hedge strike
                hedge_strike = getattr(self.config, "HEDGE_STRIKE", 0)
                if hedge_strike == 0:
                    try:
                        ltp = self.kite.ltp(long_token)[str(long_token)]["last_price"]
                        offset = math.ceil(ltp / 100) * 100
                        hedge_strike = (bnf_atm + offset) if self.spread_type == "bull_call" else (bnf_atm - offset)
                    except Exception as e:
                        self.error_signal.emit(f"Hedge calc failed: {e}"); time.sleep(5); continue

                short_token = self._get_token_for_strike(hedge_strike, expiry)
                if not short_token:
                    self.log_signal.emit(f"⚠️ {self.short_label} not found for {hedge_strike}. Retrying…")
                    time.sleep(5); continue

                # 4. Read both legs
                long_df = self._read_parquet(long_token, expiry)
                short_df = self._read_parquet(short_token, expiry)

                # 5. Compute spread
                df_pd = self._compute_spread_data(long_df, short_df, long_strike, hedge_strike)
                if df_pd is None or df_pd.empty:
                    self.log_signal.emit(f"⚠️ {self.spread_name} data empty. Waiting…")
                else:
                    html_path = self._generate_charts(df_pd, long_strike, hedge_strike)
                    self.chart_ready_signal.emit(html_path)
                    self.log_signal.emit(f"✅ {self.spread_name} charts updated.")

                for _ in range(self.config.LOOP_INTERVAL_MIN * 1):
                    if not self._is_running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.error_signal.emit(f"{self.spread_name} Error: {e}")
                time.sleep(5)

    # ------------------------------------------------------------------
    # Spread data computation
    # ------------------------------------------------------------------

    def _compute_spread_data(self, long_spark, short_spark, long_strike, short_strike):
        long_r = long_spark.select(
            col("date"), col("open").alias("long_open"), col("high").alias("long_high"),
            col("low").alias("long_low"), col("close").alias("long_close"),
            col("volume").alias("long_volume"), col("oi").alias("long_oi"), col("day"),
        )
        short_r = short_spark.select(
            col("date").alias("s_date"), col("open").alias("short_open"),
            col("high").alias("short_high"), col("low").alias("short_low"),
            col("close").alias("short_close"), col("volume").alias("short_volume"),
            col("oi").alias("short_oi"), col("day").alias("s_day"),
        )

        joined = long_r.join(short_r, long_r["date"] == short_r["s_date"], "inner").drop("s_date", "s_day")

        joined = (joined
            .withColumn("open",   spark_round(col("long_open")  - col("short_open"),  2))
            .withColumn("high",   spark_round(col("long_high")  - col("short_high"),  2))
            .withColumn("low",    spark_round(col("long_low")   - col("short_low"),   2))
            .withColumn("close",  spark_round(col("long_close") - col("short_close"), 2))
            .withColumn("oi",     col("long_oi") + col("short_oi"))
            .withColumn("volume", col("long_volume"))
            .withColumn("strike", lit(float(long_strike)))
        )

        view_name = f"spread_{self.spread_type}"
        joined.createOrReplaceTempView(view_name)
        joined = self.spark.sql(f"""
            SELECT * FROM (
                SELECT *, (dense_rank() OVER (ORDER BY day DESC)) AS day_num
                FROM {view_name}
            ) P WHERE day_num > 0 AND day_num <= {self.config.NUM_DAYS}
        """)

        joined = joined.orderBy(asc("date"))
        joined = joined.withColumn("rownum", monotonically_increasing_id())
        cached = joined.cache()

        df = get_ROC_added(cached, col_name="close", length=20)
        df = get_ROC_added(df, col_name="long_close", length=20)
        df = get_ROC_added(df, col_name="short_close", length=20)

        raw_view = f"spread_raw_{self.spread_type}"
        df.createOrReplaceTempView(raw_view)
        df = self.spark.sql(f"""
            SELECT *,
                (CASE WHEN day_num = (SELECT MIN(day_num) FROM {raw_view}) THEN 1 ELSE 0 END) AS IS_LATEST_DAY
            FROM {raw_view}
        """)
        if self.config.IS_LATEST_DAY:
            df = df.filter(df["IS_LATEST_DAY"] == True)  # noqa: E712
            df = df.orderBy(asc("date"))
            df = df.withColumn("rownum", monotonically_increasing_id())

        w_cum = Window.partitionBy("strike").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        w_day = Window.partitionBy("strike", "day_num").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

        df = (df
            .withColumn("cumulative_sum_close",  F.sum("close").over(w_cum))
            .withColumn("cumulative_min_close",  F.min("close").over(w_cum))
            .withColumn("cumulative_count",      F.count("close").over(w_cum))
            .withColumn("cumulative_sum_oi",     F.sum("oi").over(w_cum))
            .withColumn("incremental_close_avg", col("cumulative_sum_close") / col("cumulative_count"))
            .withColumn("incremental_oi_avg",    col("cumulative_sum_oi")   / col("cumulative_count"))
            .withColumn("incremental_min_close", col("cumulative_min_close"))
            .withColumn("cumulative_sum_long",   F.sum("long_close").over(w_cum))
            .withColumn("cumulative_sum_short",  F.sum("short_close").over(w_cum))
            .withColumn("incremental_long_avg",  col("cumulative_sum_long")  / col("cumulative_count"))
            .withColumn("incremental_short_avg", col("cumulative_sum_short") / col("cumulative_count"))
            .withColumn("long_open_of_day",      F.first("long_close").over(w_day))
        )

        df_pd = df.repartition(1).toPandas()
        try: cached.unpersist()
        except Exception: pass
        for v in (view_name, raw_view):
            try: self.spark.catalog.dropTempView(v)
            except Exception: pass

        num_cols = ["rownum","open","high","low","close","oi","long_close","short_close",
                    "long_oi","short_oi","long_open","short_open","long_high","short_high",
                    "long_low","short_low","volume"]
        for c in num_cols:
            if c in df_pd.columns:
                df_pd[c] = pd.to_numeric(df_pd[c], errors="coerce")
        df_pd["date"] = pd.to_datetime(df_pd["date"])
        df_pd.sort_values("date", ascending=True, inplace=True)
        df_pd["rownum"] = range(len(df_pd))
        return df_pd

    # ------------------------------------------------------------------
    # Chart generation  (8 charts)
    # ------------------------------------------------------------------

    def _generate_charts(self, df_pd, long_strike, short_strike):
        cur_dt = datetime.now()
        now_str = cur_dt.strftime("%Y-%m-%d %I:%M %p")
        today_str = cur_dt.strftime("%Y-%m-%d")

        title_base = f"Long {long_strike} / Short {short_strike} — {now_str}"
        df = ec3._add_extra_analytics(df_pd)

        for c in df.columns:
            if df[c].dtype == "object" and c not in ("date","x_label","_dt","_date_only"):
                try: df[c] = pd.to_numeric(df[c], errors="ignore")
                except Exception: pass

        x, boundary_x = ec3._build_x_axis(df)
        roc_avg_ma = df["ROC_AVG_close"].expanding().mean()

        html_snippets = []
        saved_paths = []
        snap_dir = os.path.join("ChartsSnapshots", today_str)
        os.makedirs(snap_dir, exist_ok=True)
        tag = self.spread_name.replace(" ", "_")

        def process_fig(fig, name):
            html_snippets.append(fig.to_html(include_plotlyjs=(len(html_snippets)==0), full_html=False))
            if getattr(self.config, "SAVE_SNAPSHOT", False) or self._force_snapshot:
                fp = os.path.join(snap_dir, f"{name}_{today_str}.png")
                try: fig.write_image(fp, scale=1.5); saved_paths.append(fp)
                except Exception as e: self.error_signal.emit(f"Img save fail: {e}")

        # ── Chart 1+2: Spread Price & Signals + ROC ────────────────────
        fig1 = make_subplots(rows=1, cols=2, subplot_titles=[f"{self.spread_name}", "ROC AVG"], horizontal_spacing=0.06)
        fig1.add_trace(go.Scatter(x=x,y=df["bb_upper"],mode="lines",line=dict(width=0),showlegend=False,hoverinfo="skip"), row=1,col=1)
        fig1.add_trace(go.Scatter(x=x,y=df["bb_lower"],mode="lines",line=dict(width=0),fill="tonexty",fillcolor=ec3.COLORS["bb_fill"],showlegend=False,hoverinfo="skip"), row=1,col=1)
        fig1.add_trace(go.Scatter(x=x,y=df["bb_upper"],mode="lines",name="BB Upper",line=dict(color=ec3.COLORS["bb_line"],width=1,dash="dot")), row=1,col=1)
        fig1.add_trace(go.Scatter(x=x,y=df["bb_lower"],mode="lines",name="BB Lower",line=dict(color=ec3.COLORS["bb_line"],width=1,dash="dot")), row=1,col=1)
        fig1.add_trace(go.Scatter(x=x,y=df["close"],mode="lines",name=self.spread_name,line=dict(color=ec3.COLORS["straddle_close"],width=2.5)), row=1,col=1)
        trailing_sl = (df["incremental_min_close"] + df["incremental_close_avg"]) / 2
        fig1.add_trace(go.Scatter(x=x,y=trailing_sl,mode="lines",name="Trailing SL",line=dict(color=ec3.COLORS["trailing_sl"],width=2,dash="dash")), row=1,col=1)
        fig1.add_trace(go.Scatter(x=x,y=df["incremental_close_avg"],mode="lines",name=f"{self.spread_name} AVG",line=dict(color=ec3.COLORS["avg"],width=1.5,dash="dot")), row=1,col=1)

        fig1.add_trace(go.Scatter(x=x,y=[0]*len(df),mode="lines",line=dict(color=ec3.COLORS["midline"],width=1),showlegend=False), row=1,col=2)
        fig1.add_trace(go.Scatter(x=x,y=df["ROC_AVG_close"],mode="lines",name="ROC AVG",line=dict(color=ec3.COLORS["roc_avg"],width=2)), row=1,col=2)
        fig1.add_trace(go.Scatter(x=x,y=roc_avg_ma,mode="lines",name="ROC MA",line=dict(color=ec3.COLORS["avg"],width=1.5,dash="dot")), row=1,col=2)

        ec3._add_day_boundaries(fig1, boundary_x, [1,1], [1,2])
        ec3._apply_dark_layout(fig1, f"{self.spread_name} — {title_base}", height=500, width=1400)
        ec3._add_last_value_annotations(fig1, [(x,df["close"],ec3.COLORS["straddle_close"],"")], row=1,col=1)
        ec3._add_last_value_annotations(fig1, [(x,df["ROC_AVG_close"],ec3.COLORS["roc_avg"],"")], row=1,col=2)
        process_fig(fig1, f"{tag}_Price_Signal")

        # ── Chart 3+4: Long vs Short + ROC ────────────────────────────
        fig2 = make_subplots(rows=1, cols=2, subplot_titles=[f"{self.long_label} VS {self.short_label}", f"{self.long_label} / {self.short_label} ROC"], horizontal_spacing=0.06)
        fig2.add_trace(go.Scatter(x=x,y=df["long_close"],mode="lines",name=self.long_label,line=dict(color=ec3.COLORS["pe_close"],width=2)), row=1,col=1)
        fig2.add_trace(go.Scatter(x=x,y=df["short_close"],mode="lines",name=self.short_label,line=dict(color=ec3.COLORS["ce_close"],width=2)), row=1,col=1)
        if "incremental_long_avg" in df.columns:
            fig2.add_trace(go.Scatter(x=x,y=df["incremental_long_avg"],mode="lines",name=f"{self.long_label} AVG",line=dict(color=ec3.COLORS["pe_avg"],width=1,dash="dot")), row=1,col=1)
        if "incremental_short_avg" in df.columns:
            fig2.add_trace(go.Scatter(x=x,y=df["incremental_short_avg"],mode="lines",name=f"{self.short_label} AVG",line=dict(color=ec3.COLORS["ce_avg"],width=1,dash="dot")), row=1,col=1)
        if "long_open_of_day" in df.columns:
            fig2.add_trace(go.Scatter(x=x,y=df["long_open_of_day"],mode="lines",name="Open MAX Close",line=dict(color=ec3.COLORS["open_max_close"],width=2,dash="dashdot")), row=1,col=1)

        fig2.add_trace(go.Scatter(x=x,y=[0]*len(df),mode="lines",line=dict(color=ec3.COLORS["midline"],width=1),showlegend=False), row=1,col=2)
        if "ROC_AVG_long_close" in df.columns:
            fig2.add_trace(go.Scatter(x=x,y=df["ROC_AVG_long_close"],mode="lines",name=f"{self.long_label} ROC",line=dict(color=ec3.COLORS["pe_roc"],width=2)), row=1,col=2)
        if "ROC_AVG_short_close" in df.columns:
            fig2.add_trace(go.Scatter(x=x,y=df["ROC_AVG_short_close"],mode="lines",name=f"{self.short_label} ROC",line=dict(color=ec3.COLORS["ce_roc"],width=2)), row=1,col=2)

        ec3._add_day_boundaries(fig2, boundary_x, [1,1], [1,2])
        ec3._apply_dark_layout(fig2, f"{self.long_label} / {self.short_label} — {title_base}", height=450, width=1400)
        ec3._add_last_value_annotations(fig2, [(x,df["long_close"],ec3.COLORS["pe_close"],""),(x,df["short_close"],ec3.COLORS["ce_close"],"")], row=1,col=1)
        process_fig(fig2, f"{tag}_Legs")

        # ── Chart 5+6: Combined OI + Leg OI ───────────────────────────
        fig3 = make_subplots(rows=1, cols=2, subplot_titles=["Combined OI", f"{self.long_label} AND {self.short_label} OI"], horizontal_spacing=0.06)
        fig3.add_trace(go.Scatter(x=x,y=df["oi"],mode="lines",name="Combined OI",line=dict(color=ec3.COLORS["oi_combined"],width=2)), row=1,col=1)
        fig3.add_trace(go.Scatter(x=x,y=df["incremental_oi_avg"],mode="lines",name="OI AVG",line=dict(color=ec3.COLORS["oi_avg"],width=1.5,dash="dot")), row=1,col=1)
        fig3.add_trace(go.Scatter(x=x,y=df["long_oi"],mode="lines",name=f"{self.long_label} OI",line=dict(color=ec3.COLORS["pe_oi"],width=2)), row=1,col=2)
        fig3.add_trace(go.Scatter(x=x,y=df["short_oi"],mode="lines",name=f"{self.short_label} OI",line=dict(color=ec3.COLORS["ce_oi"],width=2)), row=1,col=2)

        ec3._add_day_boundaries(fig3, boundary_x, [1,1], [1,2])
        ec3._apply_dark_layout(fig3, f"Open Interest — {title_base}", height=400, width=1400)
        ec3._add_last_value_annotations(fig3, [(x,df["oi"],ec3.COLORS["oi_combined"],"")], row=1,col=1)
        ec3._add_last_value_annotations(fig3, [(x,df["long_oi"],ec3.COLORS["pe_oi"],""),(x,df["short_oi"],ec3.COLORS["ce_oi"],"")], row=1,col=2)
        process_fig(fig3, f"{tag}_OI")

        # ── Chart 7: VWAP Analysis ────────────────────────────────────
        fig4 = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.70,0.30], vertical_spacing=0.04,
                             subplot_titles=[f"{self.spread_name} · VWAP · OIWAP", f"{self.short_label} OI − {self.long_label} OI"])
        fig4.add_trace(go.Scatter(x=x,y=df["close"],mode="lines",name=self.spread_name,line=dict(color=ec3.COLORS["straddle_close"],width=2)), row=1,col=1)
        fig4.add_trace(go.Scatter(x=x,y=df["incremental_close_avg"],mode="lines",name=f"{self.spread_name} AVG",line=dict(color=ec3.COLORS["avg"],width=1.5,dash="dot")), row=1,col=1)
        if "vwap" in df.columns and df["vwap"].notna().any():
            fig4.add_trace(go.Scatter(x=x,y=df["vwap"],mode="lines",name="VWAP",line=dict(color=ec3.COLORS["vwap"],width=2,dash="dash")), row=1,col=1)
        if "oiwap" in df.columns and df["oiwap"].notna().any():
            fig4.add_trace(go.Scatter(x=x,y=df["oiwap"],mode="lines",name="OIWAP",line=dict(color=ec3.COLORS["oiwap"],width=2,dash="dot")), row=1,col=1)

        oi_diff = df["short_oi"] - df["long_oi"]
        oi_diff_avg = oi_diff.expanding().mean()
        fig4.add_trace(go.Scatter(x=x,y=oi_diff,mode="lines",name=f"{self.short_label}−{self.long_label} OI",line=dict(color=ec3.COLORS["oi_diff"],width=1.5)), row=2,col=1)
        fig4.add_trace(go.Scatter(x=x,y=oi_diff_avg,mode="lines",name="OI Diff AVG",line=dict(color=ec3.COLORS["oi_avg"],width=1.5,dash="dot")), row=2,col=1)
        fig4.add_hline(y=0, line=dict(color=ec3.COLORS["midline"],width=1,dash="dash"), row=2,col=1)

        ec3._add_day_boundaries(fig4, boundary_x, [1,2], [1,1])
        ec3._apply_dark_layout(fig4, f"{self.spread_name} VWAP — {title_base}", height=500, width=1400)
        ec3._add_last_value_annotations(fig4, [(x,oi_diff,ec3.COLORS["oi_diff"],""),(x,oi_diff_avg,ec3.COLORS["oi_avg"],"")], row=2,col=1)
        process_fig(fig4, f"{tag}_VWAP")

        # ── Chart 8: OHLC + Volume + Cumulative Candles ───────────────
        fig5 = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.60,0.20,0.20], vertical_spacing=0.03,
                             subplot_titles=[f"{self.spread_name} OHLC", "Volume", "Cumulative Candles"])
        fig5.add_trace(go.Candlestick(x=x, open=df["open"], high=df["high"], low=df["low"], close=df["close"],
                                       increasing_line_color=ec3.COLORS["candle_up"], decreasing_line_color=ec3.COLORS["candle_down"], name="OHLC"), row=1,col=1)
        if "volume" in df.columns:
            vc = [ec3.COLORS["candle_up"] if c>=o else ec3.COLORS["candle_down"] for c,o in zip(df["close"],df["open"])]
            fig5.add_trace(go.Bar(x=x,y=df["volume"],name="Volume",marker_color=vc,opacity=0.6), row=2,col=1)

        is_green = (df["close"]>=df["open"]).astype(int)
        is_red   = (df["close"]<df["open"]).astype(int)
        x_dates  = pd.to_datetime(x).date
        fig5.add_trace(go.Scatter(x=x,y=is_green.groupby(x_dates).cumsum(),mode="lines",name="Green Cum",line=dict(color=ec3.COLORS["candle_up"],width=2)), row=3,col=1)
        fig5.add_trace(go.Scatter(x=x,y=is_red.groupby(x_dates).cumsum(),mode="lines",name="Red Cum",line=dict(color=ec3.COLORS["candle_down"],width=2)), row=3,col=1)

        ec3._add_day_boundaries(fig5, boundary_x, [1,2,3], [1,1,1])
        ec3._apply_dark_layout(fig5, f"{self.spread_name} OHLC — {title_base}", height=750, width=1400)
        fig5.update_xaxes(rangeslider_visible=False, row=1, col=1)
        g = int(is_green.sum()); r = len(df)-g
        fig5.add_annotation(text=f"Total: {len(df)}<br><span style='color:{ec3.COLORS['candle_up']}'>Green: {g}</span><br><span style='color:{ec3.COLORS['candle_down']}'>Red: {r}</span>",
                            xref="paper",yref="paper",x=0.01,y=0.98,xanchor="left",yanchor="top",showarrow=False,
                            font=dict(size=11,color=ec3.COLORS["text"],family="monospace"),bgcolor="rgba(30,30,47,0.80)",bordercolor="#444",borderwidth=1,borderpad=6,align="left")
        process_fig(fig5, f"{tag}_OHLC")

        # ── Stitch snapshot ───────────────────────────────────────────
        if saved_paths:
            try:
                imgs = [Image.open(p) for p in saved_paths]
                w_max = max(i.size[0] for i in imgs)
                h_sum = sum(i.size[1] for i in imgs)
                combined = Image.new("RGB", (w_max, h_sum))
                y = 0
                for im in imgs:
                    combined.paste(im, (0, y)); y += im.size[1]
                out = os.path.join(snap_dir, f"BNF-{self.spread_name}-{today_str}.png")
                combined.save(out)
                for p in saved_paths:
                    try: os.remove(p)
                    except OSError: pass
                self.log_signal.emit(f"📸 {self.spread_name} snapshot → {out}")
                self._force_snapshot = False
            except Exception as e:
                self.error_signal.emit(f"Stitch fail: {e}")

        # ── Build HTML ────────────────────────────────────────────────
        divs = "".join(f'<div class="chart-container">{s}</div>' for s in html_snippets)
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
        <style>body{{background:#1e1e2f;color:#d4d4dc;font-family:sans-serif;margin:0;padding:10px}}
        .chart-container{{margin-bottom:20px}}</style></head><body>{divs}</body></html>"""

        tmp = os.path.join(os.getcwd(), "DataFiles", f".tmp_{self.spread_type}_charts.html")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "w") as f:
            f.write(html)
        return tmp

    # ------------------------------------------------------------------
    def _past_end_time(self):
        if self.config.END_HOUR is None or self.config.END_MINUTE is None:
            return False
        now = datetime.now()
        return (now.hour, now.minute) >= (self.config.END_HOUR, self.config.END_MINUTE)

    def stop(self):
        self._is_running = False
