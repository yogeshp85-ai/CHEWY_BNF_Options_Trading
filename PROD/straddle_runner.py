import logging
import time
import os
import PyQt6
from datetime import datetime
from PyQt6.QtCore import QThread, pyqtSignal


# Import existing pipeline methods
from utils.kite_helpers import kite_login, get_spark_session
from utils.data_fetcher import get_latest_data
from utils.strike_utils import read_instruments, get_expiry_dates, get_ATM_Strike, BANKNIFTY_INDEX_TOKEN, NIFTY_INDEX_TOKEN, get_Options_DF
from utils.enhanced_charts_v3 import get_enhanced_chart_v3

logger = logging.getLogger(__name__)

class InstrumentsWorker(QThread):
    """Step 1: Fetch instruments, start Spark, get expiries."""
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(object, object, object, object) # kite, spark, bnf_options, bnf_expiries
    error_signal = pyqtSignal(str)

    def run(self):
        try:
            self.log_signal.emit("Step 1: Logging in to KiteConnect...")
            kite, kws, access_token = kite_login()
            self.log_signal.emit("✅ Login successful")

            self.log_signal.emit("Starting SparkSession...")
            spark = get_spark_session(app_name="Straddle_App")
            self.log_signal.emit("✅ Spark session ready")

            # We don't need to re-fetch *all* instruments every minute, 
            # we just read the latest parquets (assuming Notebook 1 ran recently)
            # OR we can run the Notebook 1 logic here.
            # Per prompt: "Run once at startup -> Pull all required trading instruments"
            # It's better to run the logic from 1_Get_Instruments.ipynb here:
            self.log_signal.emit("Fetching instruments from Kite...")
            all_instruments = kite.instruments()
            self.log_signal.emit(f"Fetched {len(all_instruments)} instruments")
            
            bnf_options  = [i for i in all_instruments if i['exchange']=='NFO' and i['segment']=='NFO-OPT' and i['name']=='BANKNIFTY']
            nifty_options= [i for i in all_instruments if i['exchange']=='NFO' and i['segment']=='NFO-OPT' and i['name']=='NIFTY']
            bnf_futures  = [i for i in all_instruments if i['exchange']=='NFO' and i['segment']=='NFO-FUT' and i['name']=='BANKNIFTY']
            nifty_futures= [i for i in all_instruments if i['exchange']=='NFO' and i['segment']=='NFO-FUT' and i['name']=='NIFTY']
            
            from utils.strike_utils import INSTRUMENT_SCHEMA
            from pyspark.sql.functions import asc, min as spark_min
            
            BNF_Options_DF     = spark.createDataFrame(bnf_options,   schema=INSTRUMENT_SCHEMA).orderBy(asc('expiry'))
            Nifty_Options_DF   = spark.createDataFrame(nifty_options, schema=INSTRUMENT_SCHEMA).orderBy(asc('expiry'))
            BNF_Futures_DF     = spark.createDataFrame(bnf_futures,   schema=INSTRUMENT_SCHEMA).orderBy(asc('expiry'))
            Nifty_Futures_DF   = spark.createDataFrame(nifty_futures, schema=INSTRUMENT_SCHEMA).orderBy(asc('expiry'))
            
            self.log_signal.emit(f"BankNifty Options: {BNF_Options_DF.count()} rows")
            
            OUTPUT_BASE = 'DataFiles/Instruments'
            os.makedirs(OUTPUT_BASE, exist_ok=True)
            BNF_Options_DF.coalesce(1).write.format('Delta').mode('Overwrite').parquet(f'{OUTPUT_BASE}/Banknifty_Options')
            Nifty_Options_DF.coalesce(1).write.format('Delta').mode('Overwrite').parquet(f'{OUTPUT_BASE}/Nifty_Options')
            BNF_Futures_DF.coalesce(1).write.format('Delta').mode('Overwrite').parquet(f'{OUTPUT_BASE}/Banknifty_Futures')
            Nifty_Futures_DF.coalesce(1).write.format('Delta').mode('Overwrite').parquet(f'{OUTPUT_BASE}/Nifty_Futures')
            
            def _min_expiry(df):     return df.agg(spark_min('expiry')).collect()[0][0]
            def _next_expiry(df, e): return df.filter(df['expiry'] > e).agg(spark_min('expiry')).collect()[0][0]
            
            bnf_curr_week   = _min_expiry(BNF_Options_DF)
            bnf_curr_month  = _min_expiry(BNF_Futures_DF)
            bnf_next_week   = _next_expiry(BNF_Options_DF, bnf_curr_week)
            bnf_next_month  = _next_expiry(BNF_Futures_DF, bnf_curr_month)
            
            nifty_curr_week  = _min_expiry(Nifty_Options_DF)
            nifty_curr_month = _min_expiry(Nifty_Futures_DF)
            nifty_next_week  = _next_expiry(Nifty_Options_DF, nifty_curr_week)
            nifty_next_month = _next_expiry(Nifty_Futures_DF, nifty_curr_month)
            
            expiry_data = [
                ('BANKNIFTY', bnf_curr_week,   bnf_curr_month,   bnf_next_week,   bnf_next_month),
                ('NIFTY',     nifty_curr_week, nifty_curr_month, nifty_next_week, nifty_next_month),
            ]
            from utils.strike_utils import EXPIRY_SCHEMA
            spark.createDataFrame(expiry_data, schema=EXPIRY_SCHEMA)\
                 .coalesce(1).write.format('Delta').mode('Overwrite')\
                 .parquet(f'{OUTPUT_BASE}/Nifty_Banknifty_Expiries')
                 
            self.log_signal.emit(f"✅ Instruments written to parquet. BNF Expiry: {bnf_curr_week}")
            
            # Re-read to pass properly formatted DF to the next steps
            bnf_opts, _, _, _, expiries_df = read_instruments(spark, OUTPUT_BASE)
            bnf_expiries = get_expiry_dates(expiries_df, 'BANKNIFTY')
            
            self.finished_signal.emit(kite, spark, bnf_opts, bnf_expiries)

        except Exception as e:
            self.error_signal.emit(f"Instruments Worker Error: {str(e)}")


class DataFetchWorker(QThread):
    """Step 2: Continuous loop fetching historical data."""
    log_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)

    def __init__(self, kite, spark, config):
        super().__init__()
        self.kite = kite
        self.spark = spark
        self.config = config
        self._is_running = True

    def run(self):
        self.log_signal.emit("📊 Data Fetch Loop started.")
        while self._is_running:
            try:
                if self._past_end_time():
                    self.log_signal.emit(f"⏹ Data fetch loop auto-stopped (end time reached: {self.config.END_HOUR}:{self.config.END_MINUTE}).")
                    break

                self.log_signal.emit(f"Fetching data at {datetime.now().strftime('%H:%M:%S')}...")
                
                # Dynamically apply weekend logic
                # Saturday (+1), Sunday (+2)
                weekday = datetime.now().weekday()
                # 5 = Saturday, 6 = Sunday
                days_history = self.config.NUM_DAYS_HISTORY
                if weekday == 5:
                    days_history += 1
                elif weekday == 6:
                    days_history += 2

                get_latest_data(
                    kite=self.kite,
                    spark=self.spark,
                    banknifty=self.config.BANKNIFTY,
                    nifty=self.config.NIFTY,
                    custom_strike=self.config.CUSTOM_STRIKE,
                    num_strikes=self.config.NUM_STRIKES,
                    pull_next_expiry=self.config.PULL_NEXT_EXPIRY,
                    num_days_history=days_history,
                    interval="3minute",
                    instruments_base_path="DataFiles/Instruments"
                )
                
                self.log_signal.emit(f"✅ Data fetch completed. Waiting {self.config.LOOP_INTERVAL_MIN} min...")
                
                # Sleep in small increments to allow responsive stopping
                # for _ in range(self.config.LOOP_INTERVAL_MIN * 60):
                for _ in range(self.config.LOOP_INTERVAL_MIN * 1):
                    if not self._is_running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.error_signal.emit(f"Data Fetch Error: {str(e)}")
                time.sleep(5) # wait before retry

    def _past_end_time(self) -> bool:
        if self.config.END_HOUR is None or self.config.END_MINUTE is None:
            return False
        now = datetime.now()
        return (now.hour, now.minute) >= (self.config.END_HOUR, self.config.END_MINUTE)

    def stop(self):
        self._is_running = False


class ChartWorker(QThread):
    """Step 3: Continuous loop rendering charts."""
    log_signal = pyqtSignal(str)
    chart_ready_signal = pyqtSignal(str) # Emits the path to the HTML file
    error_signal = pyqtSignal(str)

    def __init__(self, kite, spark, options_df, expiries, config):
        super().__init__()
        self.kite = kite
        self.spark = spark
        self.options_df = options_df
        self.expiries = expiries
        self.config = config
        self._is_running = True
        self._force_snapshot = False

    def trigger_snapshot(self):
        self._force_snapshot = True

    def run(self):
        self.log_signal.emit("📈 Chart Render Loop started.")
        
        # Monkey patch plot_enhanced_chart_v3 to NOT call fig.show() and instead generate HTML
        import utils.enhanced_charts_v3 as ec3
        import utils.chart_utils as cu
        
        # We need to intercept the plotting logic. The easiest way is to patch the module.
        # However, since get_enhanced_chart_v3 calls plot_enhanced_chart_v3 directly,
        # we'll build our own wrapper that pulls the dataframe and builds the HTML.
        
        while self._is_running:
            try:
                if self._past_end_time():
                    self.log_signal.emit(f"⏹ Chart loop auto-stopped (end time reached).")
                    break
                
                self.log_signal.emit(f"Rendering charts at {datetime.now().strftime('%H:%M:%S')}...")
                
                # Fetch Strike OHLC data directly
                if self.config.CUSTOM_STRIKE > 0:
                    bnf_atm = self.config.CUSTOM_STRIKE
                else:
                    bnf_atm = get_ATM_Strike(self.kite, BANKNIFTY_INDEX_TOKEN, rounding_number=100)
                    
                Banknifty_Options_DF = get_Options_DF(
                    spark=self.spark,
                    options_df_from_file=self.options_df,
                    atm_strike=bnf_atm,
                    current_expiry=self.expiries['current_week'],
                    strike_range=100,
                    num_strikes=self.config.NUM_STRIKES,
                )
                
                # Using the existing logic to get df_pd
                cols_single = ["date", "open", "high", "low", "close", "oi", "strike", "day_num"]
                cols_straddle = ["date", "open", "high", "low", "close", "CE_close", "PE_close", "oi", "CE_oi", "PE_oi", "strike", "day_num"]

                is_straddle = self.config.CE_OR_PE not in ("CE", "PE")
                select_cols = cols_straddle if is_straddle else cols_single

                try:
                    raw_df = cu.get_Strike_OHLC_data(
                        spark=self.spark,
                        options_df=Banknifty_Options_DF,
                        name="BANKNIFTY",
                        expiry=self.expiries['current_week'],
                        strike_level_name=self.config.STRIKE_LEVEL_NAME,
                        ce_or_pe=self.config.CE_OR_PE,
                        interval="3minute",
                        num_days=self.config.NUM_DAYS,
                        num_days_back=0,
                        historical_base_path="DataFiles/HistoricalData"
                    ).select(select_cols)
                except IndexError:
                    self.log_signal.emit(f"⚠️ Strike {self.config.STRIKE_LEVEL_NAME} not found in Parquet data yet. Retrying...")
                    time.sleep(5)
                    continue
                
                df_pd = ec3.compute_analytics(self.spark, raw_df, self.config.CE_OR_PE, self.config.IS_LATEST_DAY)
                
                if df_pd.empty:
                    self.log_signal.emit("⚠️ Chart data is empty. Waiting...")
                else:
                    # Generate the custom HTML/PNG here using a custom function
                    html_path = self.generate_interactive_charts(df_pd, bnf_atm)
                    self.chart_ready_signal.emit(html_path)
                    self.log_signal.emit("✅ Charts updated.")
                
                # Sleep in increments
                # for _ in range(self.config.LOOP_INTERVAL_MIN * 60):
                for _ in range(self.config.LOOP_INTERVAL_MIN * 1):
                    if not self._is_running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.error_signal.emit(f"Chart Render Error: {str(e)}")
                time.sleep(5)
                
    def generate_interactive_charts(self, df_pd, strike):
        """Builds Plotly figures, saves PNGs, stiches them, and returns an HTML string of interactive charts."""
        import utils.enhanced_charts_v3 as ec3
        import utils.chart_utils as cu
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        from PIL import Image
        import numpy as np
        import pandas as pd
        
        is_straddle = self.config.CE_OR_PE not in ("CE", "PE")
        cur_dt = datetime.now()
        now_str = cur_dt.strftime("%Y-%m-%d %I:%M %p")
        today_str = cur_dt.strftime("%Y-%m-%d")
        
        title_base = f"Strike: ({self.config.STRIKE_LEVEL_NAME}) / {strike}  —  {now_str}"
        
        df = ec3._add_extra_analytics(df_pd)
        
        # PREVENT CATEGORICAL Y-AXIS BUG: Convert Spark Decimals -> Floats
        for col in df.columns:
            if df[col].dtype == 'object' and col not in ["date", "x_label", "_dt", "_date_only"]:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except Exception:
                    pass
        
        x, boundary_x = ec3._build_x_axis(df)
        roc_avg_series = df["ROC_AVG_close"].expanding().mean()
        
        # We need to collect all HTML snippets
        html_snippets = []
        saved_images_paths = []
        snapshot_dir = os.path.join("ChartsSnapshots", today_str)
        os.makedirs(snapshot_dir, exist_ok=True)
        
        def process_fig(fig, base_name):
            # Embed the massive Plotly.js source completely offline in the first snippet 
            # to bypass the QtWebEngine CORS restriction on file:// URIs.
            inc_js = True if len(html_snippets) == 0 else False
            html_snippets.append(fig.to_html(include_plotlyjs=inc_js, full_html=False))
            
            # Conditionally save snapshots based on config OR manual trigger
            if getattr(self.config, 'SAVE_SNAPSHOT', False) or self._force_snapshot:
                filepath = os.path.join(snapshot_dir, f"{base_name}_{today_str}.png")
                try:
                    fig.write_image(filepath, scale=1.5)
                    saved_images_paths.append(filepath)
                except Exception as e:
                    self.error_signal.emit(f"Failed to save {filepath}: {e}")

        # === Fig 1 ===
        fig1 = make_subplots(rows=1, cols=2, subplot_titles=["Straddle Price & Signals", "ROC AVG"], horizontal_spacing=0.06)
        # Col 1
        fig1.add_trace(go.Scatter(x=x, y=df["bb_upper"], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"), row=1, col=1)
        fig1.add_trace(go.Scatter(x=x, y=df["bb_lower"], mode="lines", line=dict(width=0), fill="tonexty", fillcolor=ec3.COLORS["bb_fill"], showlegend=False, hoverinfo="skip"), row=1, col=1)
        fig1.add_trace(go.Scatter(x=x, y=df["bb_upper"], mode="lines", name="BB Upper", line=dict(color=ec3.COLORS["bb_line"], width=1, dash="dot")), row=1, col=1)
        fig1.add_trace(go.Scatter(x=x, y=df["bb_lower"], mode="lines", name="BB Lower", line=dict(color=ec3.COLORS["bb_line"], width=1, dash="dot")), row=1, col=1)
        fig1.add_trace(go.Scatter(x=x, y=df["close"], mode="lines", name="Straddle Close", line=dict(color=ec3.COLORS["straddle_close"], width=2.5)), row=1, col=1)
        trailing_sl = (df["incremental_min_close"] + df["incremental_close_avg"]) / 2
        fig1.add_trace(go.Scatter(x=x, y=trailing_sl, mode="lines", name="Trailing SL", line=dict(color=ec3.COLORS["trailing_sl"], width=2, dash="dash")), row=1, col=1)
        if "StopLoss" in df.columns:
            fig1.add_trace(go.Scatter(x=x, y=df["StopLoss"], mode="lines", name="StopLoss", line=dict(color=ec3.COLORS["stoploss"], width=2)), row=1, col=1)
        fig1.add_trace(go.Scatter(x=x, y=df["incremental_close_avg"], mode="lines", name="Straddle AVG", line=dict(color=ec3.COLORS["avg"], width=1.5, dash="dot")), row=1, col=1)
        if "EntryAllowed" in df.columns:
            fig1.add_trace(go.Scatter(x=x, y=df["EntryAllowed"], mode="markers+lines", name="Entry Allowed", line=dict(color=ec3.COLORS["entry_allowed"], width=2), marker=dict(size=4, color=ec3.COLORS["entry_allowed"])), row=1, col=1)
        
        # Col 2
        fig1.add_trace(go.Scatter(x=x, y=[0]*len(df), mode="lines", line=dict(color=ec3.COLORS["midline"], width=1), showlegend=False), row=1, col=2)
        fig1.add_trace(go.Scatter(x=x, y=df["ROC_AVG_close"], mode="lines", name="ROC AVG", line=dict(color=ec3.COLORS["roc_avg"], width=2)), row=1, col=2)
        fig1.add_trace(go.Scatter(x=x, y=roc_avg_series, mode="lines", name="ROC AVG Line", line=dict(color=ec3.COLORS["avg"], width=1.5, dash="dot")), row=1, col=2)
        
        ec3._add_day_boundaries(fig1, boundary_x, [1,1], [1,2])
        ec3._apply_dark_layout(fig1, f"Price & Signals — {title_base}", height=500, width=1400)
        ec3._add_last_value_annotations(fig1, [(x, df["close"], ec3.COLORS["straddle_close"], "")], row=1, col=1)
        ec3._add_last_value_annotations(fig1, [(x, df["ROC_AVG_close"], ec3.COLORS["roc_avg"], "")], row=1, col=2)
        process_fig(fig1, "Straddle_Price_Signal")

        # === Fig 2a ===
        if is_straddle:
            fig2a = make_subplots(rows=1, cols=2, subplot_titles=["CE vs PE Close", "CE / PE ROC & Ratio"], horizontal_spacing=0.06)
            fig2a.add_trace(go.Scatter(x=x, y=df["CE_close"], mode="lines", name="CALL", line=dict(color=ec3.COLORS["ce_close"], width=2)), row=1, col=1)
            fig2a.add_trace(go.Scatter(x=x, y=df["PE_close"], mode="lines", name="PUT", line=dict(color=ec3.COLORS["pe_close"], width=2)), row=1, col=1)
            fig2a.add_trace(go.Scatter(x=x, y=df["incremental_CE_close_avg"], mode="lines", name="CE AVG", line=dict(color=ec3.COLORS["ce_avg"], width=1, dash="dot")), row=1, col=1)
            fig2a.add_trace(go.Scatter(x=x, y=df["incremental_PE_close_avg"], mode="lines", name="PE AVG", line=dict(color=ec3.COLORS["pe_avg"], width=1, dash="dot")), row=1, col=1)
            if "Open_Max_Close" in df.columns:
                fig2a.add_trace(go.Scatter(x=x, y=df["Open_Max_Close"], mode="lines", name="Open Max Close", line=dict(color=ec3.COLORS["open_max_close"], width=2, dash="dashdot")), row=1, col=1)
                
            fig2a.add_trace(go.Scatter(x=x, y=[0]*len(df), mode="lines", line=dict(color=ec3.COLORS["midline"], width=1), showlegend=False), row=1, col=2)
            fig2a.add_trace(go.Scatter(x=x, y=df["ROC_AVG_CE_close"], mode="lines", name="CE ROC AVG", line=dict(color=ec3.COLORS["ce_roc"], width=2)), row=1, col=2)
            fig2a.add_trace(go.Scatter(x=x, y=df["ROC_AVG_PE_close"], mode="lines", name="PE ROC AVG", line=dict(color=ec3.COLORS["pe_roc"], width=2)), row=1, col=2)
            # fig2a.add_trace(go.Scatter(x=x, y=df["ROC_AVG_CEPE_AVG"], mode="lines", name="CE_PE ROC AVG", line=dict(color=ec3.COLORS["roc_avg"], width=2)), row=1, col=2)
            fig2a.add_trace(go.Scatter(x=x, y=df["ROC_AVG_CE_close"]+df["ROC_AVG_PE_close"], mode="lines", name="CE_PE ROC AVG", line=dict(color=ec3.COLORS["roc_avg"], width=2)), row=1, col=2)
            if "ce_pe_ratio" in df.columns:
                fig2a.add_trace(go.Scatter(x=x, y=df["ce_pe_ratio"], mode="lines", name="CE/PE Ratio", line=dict(color=ec3.COLORS["ratio_line"], width=1.5, dash="dash")), row=1, col=2)
                
            ec3._add_day_boundaries(fig2a, boundary_x, [1,1], [1,2])
            ec3._apply_dark_layout(fig2a, f"CE / PE — {title_base}", height=450, width=1400)
            ec3._add_last_value_annotations(fig2a, [(x, df["CE_close"], ec3.COLORS["ce_close"], ""), (x, df["PE_close"], ec3.COLORS["pe_close"], "")], row=1, col=1)
            process_fig(fig2a, "CE_vs_PE_Close")

        # === Fig 2 ===
        oi_cols = 2 if is_straddle else 1
        oi_titles = ["Combined OI", "CE / PE OI  (right axis = PUT−CALL OI)"] if is_straddle else ["Open Interest"]
        oi_specs = [[{"secondary_y": False}, {"secondary_y": True}]] if is_straddle else [[{"secondary_y": False}]]
        fig2 = make_subplots(rows=1, cols=oi_cols, subplot_titles=oi_titles, specs=oi_specs, horizontal_spacing=0.06, column_widths=[0.5, 0.5] if is_straddle else [1.0])
        fig2.add_trace(go.Scatter(x=x, y=df["oi"], mode="lines", name="Combined OI", line=dict(color=ec3.COLORS["oi_combined"], width=2)), row=1, col=1)
        fig2.add_trace(go.Scatter(x=x, y=df["incremental_oi_avg"], mode="lines", name="OI AVG", line=dict(color=ec3.COLORS["oi_avg"], width=1.5, dash="dot")), row=1, col=1)
        if is_straddle:
            fig2.add_trace(go.Scatter(x=x, y=df["CE_oi"], mode="lines", name="CALL OI", line=dict(color=ec3.COLORS["ce_oi"], width=2)), row=1, col=2, secondary_y=False)
            fig2.add_trace(go.Scatter(x=x, y=df["PE_oi"], mode="lines", name="PUT OI", line=dict(color=ec3.COLORS["pe_oi"], width=2)), row=1, col=2, secondary_y=False)
            if "oi_diff" in df.columns:
                fig2.add_trace(go.Scatter(x=x, y=df["oi_diff"], mode="lines", name="PUT OI − CALL OI", line=dict(color=ec3.COLORS["oi_diff"], width=1.5, dash="dash")), row=1, col=2, secondary_y=True)
                fig2.add_trace(go.Scatter(x=x, y=df["oi_diff_avg"], mode="lines", name="OI Diff AVG", line=dict(color=ec3.COLORS["oi_avg"], width=1.5, dash="dot")), row=1, col=2, secondary_y=True)
                fig2.update_yaxes(title_text="PUT OI − CALL OI", secondary_y=True, row=1, col=2, showgrid=False, rangemode="normal", tickfont=dict(size=9, color=ec3.COLORS["oi_diff"]))
        
        ec3._add_day_boundaries(fig2, boundary_x, [1]*oi_cols, list(range(1, oi_cols+1)))
        ec3._apply_dark_layout(fig2, f"Open Interest — {title_base}", height=400, width=1400)
        ec3._add_last_value_annotations(fig2, [(x, df["oi"], ec3.COLORS["oi_combined"], "")], row=1, col=1)
        if is_straddle:
            ec3._add_last_value_annotations(fig2, [(x, df["CE_oi"], ec3.COLORS["ce_oi"], ""), (x, df["PE_oi"], ec3.COLORS["pe_oi"], "")], row=1, col=2)
            if "oi_diff" in df.columns:
                ec3._add_last_value_annotations(fig2, [(x, df["oi_diff"], ec3.COLORS["oi_diff"], ""), (x, df["oi_diff_avg"], ec3.COLORS["oi_avg"], "")], row=1, col=2, secondary_y=True)
        process_fig(fig2, "Open_Interest")

        # === Fig 3 ===
        if is_straddle and "oi_pcr" in df.columns:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=x, y=df["oi_pcr"], mode="lines", name="OI PCR (PE/CE)", line=dict(color=ec3.COLORS["oi_pcr"], width=2)))
            fig3.add_hline(y=1.0, line=dict(color=ec3.COLORS["midline"], width=1.5, dash="dash"), annotation_text="PCR = 1", annotation_position="top right", annotation_font=dict(color=ec3.COLORS["text"], size=10))
            fig3.update_layout(height=300, width=1400, title=dict(text=f"OI Put-Call Ratio (PCR) — {title_base}", font=dict(size=16, color=ec3.COLORS["text"]), x=0.5), paper_bgcolor=ec3.COLORS["paper"], plot_bgcolor=ec3.COLORS["bg"], font=dict(color=ec3.COLORS["text"], size=11), legend=dict(bgcolor="rgba(30,30,47,0.85)", bordercolor="#444", borderwidth=1, font=dict(size=10, color=ec3.COLORS["text"])), hovermode="x unified", margin=dict(l=60, r=30, t=55, b=60), yaxis=dict(title="PCR", showgrid=True, gridcolor=ec3.COLORS["grid"]), xaxis=dict(showgrid=True, gridcolor=ec3.COLORS["grid"], tickfont=dict(size=9), tickangle=-45, nticks=20))
            for bx in boundary_x:
                fig3.add_vline(x=bx, line=dict(color=ec3.COLORS["day_boundary"], width=1.5, dash="dash"))
            ec3._add_last_value_annotations(fig3, [(x, df["oi_pcr"], ec3.COLORS["oi_pcr"], "")])
            process_fig(fig3, "PCR")

        # === Fig 4 ===
        fig4 = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.70, 0.30], vertical_spacing=0.04, subplot_titles=["Straddle Price · VWAP · OIWAP", "PUT OI − CALL OI"])
        fig4.add_trace(go.Scatter(x=x, y=df["close"], mode="lines", name="Straddle Price", line=dict(color=ec3.COLORS["straddle_close"], width=2)), row=1, col=1)
        if "vwap" in df.columns and df["vwap"].notna().any():
            fig4.add_trace(go.Scatter(x=x, y=df["vwap"], mode="lines", name="VWAP", line=dict(color=ec3.COLORS["vwap"], width=2, dash="dash")), row=1, col=1)
        if "oiwap" in df.columns and df["oiwap"].notna().any():
            fig4.add_trace(go.Scatter(x=x, y=df["oiwap"], mode="lines", name="OIWAP", line=dict(color=ec3.COLORS["oiwap"], width=2, dash="dot")), row=1, col=1)
        if is_straddle and "oi_diff" in df.columns:
            fig4.add_trace(go.Scatter(x=x, y=df["oi_diff"], mode="lines", name="PUT OI − CALL OI", line=dict(color=ec3.COLORS["oi_diff"], width=1.5)), row=2, col=1)
            fig4.add_trace(go.Scatter(x=x, y=df["oi_diff_avg"], mode="lines", name="OI Diff AVG", line=dict(color=ec3.COLORS["oi_avg"], width=1.5, dash="dot")), row=2, col=1)
            fig4.add_hline(y=0, line=dict(color=ec3.COLORS["midline"], width=1, dash="dash"), row=2, col=1)
        
        ec3._add_day_boundaries(fig4, boundary_x, [1,2], [1,1])
        ec3._apply_dark_layout(fig4, f"Straddle Price / VWAP / OIWAP — {title_base}", height=500, width=1400)
        fig4.update_xaxes(tickangle=-45, nticks=20, row=2, col=1)
        if is_straddle and "oi_diff" in df.columns:
            diff_vals = pd.concat([df["oi_diff"].dropna(), df["oi_diff_avg"].dropna()])
            if len(diff_vals)>0:
                d_min, d_max = float(diff_vals.min()), float(diff_vals.max())
                pad = (d_max - d_min)*0.05 if d_max!=d_min else 1
                fig4.update_yaxes(range=[d_min-pad, d_max+pad], row=2, col=1)
            ec3._add_last_value_annotations(fig4, [(x, df["oi_diff"], ec3.COLORS["oi_diff"], ""), (x, df["oi_diff_avg"], ec3.COLORS["oi_avg"], "")], row=2, col=1)
        process_fig(fig4, "Straddle_Price_VWAP_OIWAP")

        # === Fig 5 ===
        fig5 = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.60, 0.20, 0.20], vertical_spacing=0.03, subplot_titles=["OHLC Candlestick", "Volume", "Cumulative Candles"])
        fig5.add_trace(go.Candlestick(x=x, open=df["open"], high=df["high"], low=df["low"], close=df["close"], increasing_line_color=ec3.COLORS["candle_up"], decreasing_line_color=ec3.COLORS["candle_down"], name="OHLC"), row=1, col=1)
        if "volume" in df.columns:
            vol_colors = [ec3.COLORS["candle_up"] if c>=o else ec3.COLORS["candle_down"] for c,o in zip(df["close"], df["open"])]
            fig5.add_trace(go.Bar(x=x, y=df["volume"], name="Volume", marker_color=vol_colors, opacity=0.6), row=2, col=1)
            
        # Add Green/Red Candle cumulative count lines in the new 3rd row
        is_green = (df["close"] >= df["open"]).astype(int)
        is_red = (df["close"] < df["open"]).astype(int)
        
        # Safely extract the date component regardless of whether x is a DatetimeIndex or a list of strings
        x_dates = pd.to_datetime(x).date
        
        green_cum = is_green.groupby(x_dates).cumsum()
        red_cum = is_red.groupby(x_dates).cumsum()
            
        fig5.add_trace(go.Scatter(x=x, y=green_cum, mode="lines", name="Green Cumulative", line=dict(color=ec3.COLORS["candle_up"], width=2)), row=3, col=1)
        fig5.add_trace(go.Scatter(x=x, y=red_cum, mode="lines", name="Red Cumulative", line=dict(color=ec3.COLORS["candle_down"], width=2)), row=3, col=1)

        ec3._add_day_boundaries(fig5, boundary_x, [1,2,3], [1,1,1])
        ec3._apply_dark_layout(fig5, f"OHLC Candlestick — {title_base}", height=750, width=1400)
        fig5.update_xaxes(rangeslider_visible=False, row=1, col=1)
        
        total_candles = len(df)
        green_candles = int((df["close"]>=df["open"]).sum())
        red_candles = total_candles - green_candles
        summary_text = f"Total : {total_candles}<br><span style='color:{ec3.COLORS['candle_up']}'>Green : {green_candles}</span><br><span style='color:{ec3.COLORS['candle_down']}'>Red   : {red_candles}</span>"
        fig5.add_annotation(text=summary_text, xref="paper", yref="paper", x=0.01, y=0.98, xanchor="left", yanchor="top", showarrow=False, font=dict(size=11, color=ec3.COLORS["text"], family="monospace"), bgcolor="rgba(30,30,47,0.80)", bordercolor="#444", borderwidth=1, borderpad=6, align="left")
        process_fig(fig5, "OHLC_Candlestick")

        # === Stitch Images ===
        if saved_images_paths:
            try:
                images = [Image.open(p) for p in saved_images_paths]
                widths, heights = zip(*(i.size for i in images))
                total_width = max(widths)
                total_height = sum(heights)
                combined_img = Image.new('RGB', (total_width, total_height))
                y_offset = 0
                for im in images:
                    combined_img.paste(im, (0, y_offset))
                    y_offset += im.size[1]
                out_path = os.path.join(snapshot_dir, f"BNF-Staddle-{today_str}.png")
                combined_img.save(out_path)
                for p in saved_images_paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
                self.log_signal.emit(f"📸 Snapshot saved to {out_path}")
                
                # Reset the override flag if manually requested
                self._force_snapshot = False
                
            except Exception as e:
                self.error_signal.emit(f"Failed to stitch images: {e}")

        # Construct final HTML
        divs = ''.join([f'<div class="chart-container">{snip}</div>' for snip in html_snippets])
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ background-color: #1e1e2f; color: #d4d4dc; font-family: sans-serif; margin: 0; padding: 10px; }}
                .chart-container {{ margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            {divs}
        </body>
        </html>
        """
        
        # Save to a temp file that the web engine can load
        tmp_html_path = os.path.join(os.getcwd(), "DataFiles", ".tmp_charts.html")
        os.makedirs(os.path.dirname(tmp_html_path), exist_ok=True)
        with open(tmp_html_path, "w") as f:
            f.write(html_content)
            
        return tmp_html_path

    def _past_end_time(self) -> bool:
        if self.config.END_HOUR is None or self.config.END_MINUTE is None:
            return False
        now = datetime.now()
        return (now.hour, now.minute) >= (self.config.END_HOUR, self.config.END_MINUTE)

    def stop(self):
        self._is_running = False

