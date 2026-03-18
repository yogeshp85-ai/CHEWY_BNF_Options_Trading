import sys
import os

# Fix for Qt Platform Plugin error on macOS when running from bash scripts
venv_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
qt_plugin_path = os.path.join(venv_base, '.venv', 'lib', 'python3.12', 'site-packages', 'PySide6', 'Qt', 'plugins', 'platforms')
os.environ['QT_QPA_PLATFORM_PLUGIN_PATH'] = qt_plugin_path

import logging
from dataclasses import dataclass
from datetime import datetime, date

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QLineEdit, QCheckBox, QPushButton, QTextEdit, QGridLayout,
    QGroupBox, QSplitter, QMessageBox, QTabWidget, QComboBox, QToolButton, QScrollArea, QSizePolicy
)
from PySide6.QtCore import Qt, QUrl, QTimer
from PySide6.QtGui import QIcon
from PySide6.QtWebEngineWidgets import QWebEngineView
from dotenv import load_dotenv

# Load variables from .env file if present
load_dotenv()

# Import the worker threads
from straddle_runner import InstrumentsWorker, DataFetchWorker, ChartWorker
from spread_runner import SpreadChartWorker

# Configure root logger to not spam the console if we don't want to
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class PipelineConfig:
    BANKNIFTY: bool = True
    NIFTY: bool = False
    CUSTOM_STRIKE: int = 0
    HEDGE_STRIKE: int = 0
    NUM_STRIKES: int = 9
    PULL_NEXT_EXPIRY: bool = False
    NUM_DAYS_HISTORY: int = 1
    LOOP_INTERVAL_MIN: int = 1
    
    STRIKE_LEVEL_NAME: str = 'ATM'
    CE_OR_PE: str = 'E'
    NUM_DAYS: int = 2
    IS_LATEST_DAY: bool = True
    
    END_HOUR: int = 15
    END_MINUTE: int = 30
    SAVE_SNAPSHOT: bool = True


def create_info_btn(tooltip: str):
    btn = QToolButton()
    btn.setText("ℹ")
    btn.setToolTip(tooltip)
    btn.setStyleSheet("border: none; color: #888888; font-weight: bold;")
    return btn

def create_combo(start=0, end=20, default=0):
    cb = QComboBox()
    for i in range(start, end+1):
        cb.addItem(str(i))
    cb.setCurrentText(str(default))
    return cb

class StraddleApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("TEWY - Options Charts")
        self.resize(1600, 1000)
        
        # App icon
        icon_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Logo", "Tewy.jpg")
        if os.path.exists(icon_path):
            app_icon = QIcon(icon_path)
            self.setWindowIcon(app_icon)
            QApplication.instance().setWindowIcon(app_icon)
        
        self.kite = None
        self.spark = None
        self.options_df = None
        self.expiries = None
        
        self.instruments_worker = None
        self.data_worker = None
        self.chart_worker = None
        self.bull_call_worker = None
        self.bear_put_worker = None
        
        self.strike_watcher_timer = QTimer(self)
        self.strike_watcher_timer.setSingleShot(True)
        self.strike_watcher_timer.timeout.connect(self.handle_strike_changed)
        
        self.init_ui()
        
    def init_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)
        
        # --- TOP LEVEL SECTION ---
        top_bar = QHBoxLayout()
        # App Updated Date
        lbl_date = QLabel(f"<b>App Updated Date:</b> {date.today().strftime('%Y-%m-%d')}")
        lbl_date.setStyleSheet("color: #aaaaaa; font-family: monospace;")
        top_bar.addWidget(lbl_date)
        top_bar.addStretch()
        
        # Toggles and Extra Buttons
        self.btn_save_snap = QPushButton("📸 Save Charts Snapshot")
        self.btn_save_snap.setStyleSheet("background-color: #2196F3; color: white; font-weight: bold;")
        self.btn_save_snap.clicked.connect(self.trigger_manual_snapshot)
        self.btn_save_snap.setFixedSize(180, 30)
        
        self.btn_toggle_config = QPushButton("Hide Configuration")
        self.btn_toggle_config.setCheckable(True)
        self.btn_toggle_config.clicked.connect(self.update_splitter_visibility)
        self.btn_toggle_config.setFixedSize(150, 30)
        
        self.btn_toggle_status = QPushButton("Hide Status Window")
        self.btn_toggle_status.setCheckable(True)
        self.btn_toggle_status.clicked.connect(self.update_splitter_visibility)
        self.btn_toggle_status.setFixedSize(150, 30)
        
        top_bar.addWidget(self.btn_save_snap)
        top_bar.addWidget(self.btn_toggle_config)
        top_bar.addWidget(self.btn_toggle_status)
        main_layout.addLayout(top_bar)
        
        # --- HORIZONTAL SPLITTER: Left (Config+Status) | Right (Tabs) ---
        self.h_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(self.h_splitter)
        
        # ---- LEFT PANEL (Config + Status) — shared across all tabs ----
        self.left_panel = QWidget()
        left_panel_layout = QVBoxLayout(self.left_panel)
        left_panel_layout.setContentsMargins(0, 0, 0, 0)
        
        self.config_wrapper = QWidget()
        config_wrapper_layout = QVBoxLayout(self.config_wrapper)
        config_wrapper_layout.setContentsMargins(0, 0, 0, 0)
        
        self.config_group = QGroupBox("Configuration Parameters")
        grid = QGridLayout()
        self.config_group.setLayout(grid)
        
        # Checkboxes row 0-1
        self.chk_bnf = QCheckBox("BANKNIFTY"); self.chk_bnf.setChecked(True)
        self.chk_nifty = QCheckBox("NIFTY")
        self.chk_next_expiry = QCheckBox("PULL_NEXT_EXPIRY")
        self.chk_latest_day = QCheckBox("IS_LATEST_DAY"); self.chk_latest_day.setChecked(True)
        self.chk_save_snapshot = QCheckBox("SAVE_SNAPSHOT"); self.chk_save_snapshot.setChecked(True)
        
        h_checks = QHBoxLayout()
        h_checks.addWidget(self.chk_bnf); h_checks.addWidget(self.chk_nifty)
        h_checks.addWidget(self.chk_next_expiry); h_checks.addStretch()
        grid.addLayout(h_checks, 0, 0, 1, 3)
        
        h_checks_2 = QHBoxLayout()
        h_checks_2.addWidget(self.chk_latest_day); h_checks_2.addWidget(self.chk_save_snapshot)
        h_checks_2.addStretch()
        grid.addLayout(h_checks_2, 1, 0, 1, 3)
        
        # Config rows
        grid.addWidget(QLabel("Custom Strike (0=LTP):"), 2, 0)
        self.txt_custom_strike = QLineEdit("0")
        self.txt_custom_strike.textChanged.connect(self.on_strike_text_changed)
        grid.addWidget(self.txt_custom_strike, 2, 1)
        grid.addWidget(create_info_btn("0 = current ATM. Triggers auto-restart."), 2, 2)
        
        grid.addWidget(QLabel("Hedge Strike (0=Auto):"), 3, 0)
        self.txt_hedge_strike = QLineEdit("0")
        grid.addWidget(self.txt_hedge_strike, 3, 1)
        grid.addWidget(create_info_btn("0 = auto-calc from premium. For spread tabs."), 3, 2)
        
        grid.addWidget(QLabel("Strike Range (+/-):"), 4, 0)
        self.cmb_num_strikes = create_combo(0, 20, 9)
        grid.addWidget(self.cmb_num_strikes, 4, 1)
        grid.addWidget(create_info_btn("Strikes above/below ATM."), 4, 2)
        
        grid.addWidget(QLabel("Data History (Days):"), 5, 0)
        self.cmb_history_days = create_combo(0, 20, 1)
        grid.addWidget(self.cmb_history_days, 5, 1)
        grid.addWidget(create_info_btn("Auto-adjusts (+1 Sat, +2 Sun)."), 5, 2)
        
        grid.addWidget(QLabel("Fetch Interval (Sec):"), 6, 0)
        self.cmb_loop_interval = create_combo(0, 20, 10)
        grid.addWidget(self.cmb_loop_interval, 6, 1)
        grid.addWidget(create_info_btn("Seconds between fetches."), 6, 2)
        
        grid.addWidget(QLabel("Target Strike Level:"), 7, 0)
        self.txt_strike_name = QLineEdit("ATM")
        grid.addWidget(self.txt_strike_name, 7, 1)
        grid.addWidget(create_info_btn("Target strike level (ATM, ATM+1)."), 7, 2)
        
        grid.addWidget(QLabel("Strategy Mode:"), 8, 0)
        self.cmb_ce_pe = QComboBox()
        self.cmb_ce_pe.addItems(["E", "CE", "PE"]); self.cmb_ce_pe.setCurrentText("E")
        grid.addWidget(self.cmb_ce_pe, 8, 1)
        grid.addWidget(create_info_btn("'E' = Straddle. 'CE'/'PE' = Single leg."), 8, 2)
        
        grid.addWidget(QLabel("Chart History (Days):"), 9, 0)
        self.cmb_num_days_chart = create_combo(0, 20, 2)
        grid.addWidget(self.cmb_num_days_chart, 9, 1)
        grid.addWidget(create_info_btn("Days of data to render."), 9, 2)
        
        grid.addWidget(QLabel("Auto-Stop Time:"), 10, 0)
        h_time = QHBoxLayout()
        self.txt_end_hour = QLineEdit("15"); self.txt_end_hour.setMaximumWidth(40)
        h_time.addWidget(self.txt_end_hour); h_time.addWidget(QLabel("MIN:"))
        self.txt_end_minute = QLineEdit("30"); self.txt_end_minute.setMaximumWidth(40)
        h_time.addWidget(self.txt_end_minute)
        grid.addLayout(h_time, 10, 1)
        grid.addWidget(create_info_btn("Auto stop (24h)."), 10, 2)
        
        # Max widths
        for w in [self.txt_custom_strike, self.txt_hedge_strike, self.cmb_num_strikes,
                  self.cmb_history_days, self.cmb_loop_interval, self.txt_strike_name,
                  self.cmb_ce_pe, self.cmb_num_days_chart]:
            w.setMaximumWidth(120)
        grid.setColumnStretch(3, 1)
        
        config_wrapper_layout.addWidget(self.config_group)
        
        # Pipeline controls
        h_ctrl = QHBoxLayout()
        self.btn_start = QPushButton("▶ START PIPELINE")
        self.btn_start.setStyleSheet("background-color: #4CAF50; color: white; padding: 6px; font-weight: bold;")
        self.btn_start.clicked.connect(self.start_pipeline)
        self.btn_stop = QPushButton("⏹ STOP PIPELINE")
        self.btn_stop.setStyleSheet("background-color: #f44336; color: white; padding: 6px; font-weight: bold;")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_pipeline)
        h_ctrl.addWidget(self.btn_start); h_ctrl.addWidget(self.btn_stop)
        config_wrapper_layout.addLayout(h_ctrl)
        
        left_panel_layout.addWidget(self.config_wrapper)
        
        # Status group
        self.status_group = QGroupBox("Status Window")
        status_layout = QVBoxLayout(); self.status_group.setLayout(status_layout)
        self.lbl_status = QLabel("Status: Idle")
        self.lbl_status.setStyleSheet("font-weight: bold; color: #4CAF50;")
        status_layout.addWidget(self.lbl_status)
        self.text_log = QTextEdit(); self.text_log.setReadOnly(True)
        self.text_log.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: monospace;")
        status_layout.addWidget(self.text_log)
        left_panel_layout.addWidget(self.status_group)
        
        self.h_splitter.addWidget(self.left_panel)
        
        # ---- RIGHT PANEL: Tab Widget with 3 tabs ----
        self.tabs = QTabWidget()
        
        placeholder_html = "<html><body style='background-color:#1e1e2f;color:white;display:flex;justify-content:center;align-items:center;height:100vh;'><h2>Charts will load here...</h2></body></html>"
        
        self.webview = QWebEngineView()
        self.webview.setHtml(placeholder_html)
        self.webview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.tabs.addTab(self.webview, "BNF - Straddle")
        
        self.webview_bull = QWebEngineView()
        self.webview_bull.setHtml(placeholder_html)
        self.webview_bull.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.tabs.addTab(self.webview_bull, "BNF - Bull Call Spread")
        
        self.webview_bear = QWebEngineView()
        self.webview_bear.setHtml(placeholder_html)
        self.webview_bear.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.tabs.addTab(self.webview_bear, "BNF - Bear Put Spread")
        
        self.h_splitter.addWidget(self.tabs)
        self.h_splitter.setSizes([300, 1700])
        
        self.setCentralWidget(main_widget)

    def trigger_manual_snapshot(self):
        triggered = False
        for w in [self.chart_worker, self.bull_call_worker, self.bear_put_worker]:
            if w and w.isRunning():
                w.trigger_snapshot()
                triggered = True
        if triggered:
            self.log_msg("📸 Manual snapshot requested for all active tabs.")
        else:
            QMessageBox.information(self, "Not Running", "Cannot take snapshot. Charts are not actively generating.")

    def update_splitter_visibility(self):
        # Update the button text
        config_hidden = self.btn_toggle_config.isChecked()
        status_hidden = self.btn_toggle_status.isChecked()
        
        self.btn_toggle_config.setText("Show Configuration" if config_hidden else "Hide Configuration")
        self.btn_toggle_status.setText("Show Status Window" if status_hidden else "Hide Status Window")
        
        # Hide internal elements
        self.config_wrapper.setVisible(not config_hidden)
        self.status_group.setVisible(not status_hidden)
        
        # If BOTH are hidden, collapse the ENTIRE left panel so charts take 100% width
        if config_hidden and status_hidden:
            self.left_panel.hide()
        else:
            self.left_panel.show()
            self.h_splitter.setSizes([300, 1700])

    def log_msg(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.text_log.append(f"[{timestamp}] {msg}")
        logger.info(msg)

    def get_config(self) -> PipelineConfig:
        try:
            return PipelineConfig(
                BANKNIFTY=self.chk_bnf.isChecked(),
                NIFTY=self.chk_nifty.isChecked(),
                CUSTOM_STRIKE=int(self.txt_custom_strike.text()),
                HEDGE_STRIKE=int(self.txt_hedge_strike.text()) if self.txt_hedge_strike.text() else 0,
                NUM_STRIKES=int(self.cmb_num_strikes.currentText()),
                PULL_NEXT_EXPIRY=self.chk_next_expiry.isChecked(),
                NUM_DAYS_HISTORY=int(self.cmb_history_days.currentText()),
                LOOP_INTERVAL_MIN=int(self.cmb_loop_interval.currentText()),
                STRIKE_LEVEL_NAME=self.txt_strike_name.text(),
                CE_OR_PE=self.cmb_ce_pe.currentText(),
                NUM_DAYS=int(self.cmb_num_days_chart.currentText()),
                IS_LATEST_DAY=self.chk_latest_day.isChecked(),
                END_HOUR=int(self.txt_end_hour.text()) if self.txt_end_hour.text() else None,
                END_MINUTE=int(self.txt_end_minute.text()) if self.txt_end_minute.text() else None,
                SAVE_SNAPSHOT=self.chk_save_snapshot.isChecked()
            )
        except ValueError as e:
            QMessageBox.critical(self, "Config Error", f"Invalid integer parameter: {e}")
            return None

    def start_pipeline(self):
        config = self.get_config()
        if not config: return
        
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.lbl_status.setText("Status: Step 1 (Instruments & Login)")
        self.lbl_status.setStyleSheet("font-weight: bold; color: orange;")
        self.text_log.clear()
        
        self.log_msg("=== Starting Pipeline ===")
        
        # Start Step 1
        self.instruments_worker = InstrumentsWorker()
        self.instruments_worker.log_signal.connect(self.log_msg)
        self.instruments_worker.error_signal.connect(lambda e: self.log_msg(f"❌ ERROR: {e}"))
        self.instruments_worker.finished_signal.connect(lambda k, s, o, e: self.on_instruments_done(k, s, o, e, config))
        self.instruments_worker.start()

    def on_instruments_done(self, kite, spark, options_df, expiries, config):
        self.kite = kite
        self.spark = spark
        self.options_df = options_df
        self.expiries = expiries
        
        self.start_loops(config)

    def start_loops(self, config):
        self.lbl_status.setText("Status: Loops Running Concurrently ●")
        self.lbl_status.setStyleSheet("font-weight: bold; color: green;")
        
        # Start Step 2
        self.data_worker = DataFetchWorker(self.kite, self.spark, config)
        self.data_worker.log_signal.connect(self.log_msg)
        self.data_worker.error_signal.connect(lambda e: self.log_msg(f"❌ DATA ERROR: {e}"))
        self.data_worker.start()
        
        # Start Step 3 — Straddle
        self.chart_worker = ChartWorker(self.kite, self.spark, self.options_df, self.expiries, config)
        self.chart_worker.log_signal.connect(self.log_msg)
        self.chart_worker.error_signal.connect(lambda e: self.log_msg(f"❌ CHART ERROR: {e}"))
        self.chart_worker.chart_ready_signal.connect(self.update_charts)
        self.chart_worker.start()
        
        # Start Step 3 — Bull Call Spread
        self.bull_call_worker = SpreadChartWorker(self.kite, self.spark, self.options_df, self.expiries, config, "bull_call")
        self.bull_call_worker.log_signal.connect(self.log_msg)
        self.bull_call_worker.error_signal.connect(lambda e: self.log_msg(f"❌ BULL CALL ERROR: {e}"))
        self.bull_call_worker.chart_ready_signal.connect(self.update_bull_charts)
        self.bull_call_worker.start()
        
        # Start Step 3 — Bear Put Spread
        self.bear_put_worker = SpreadChartWorker(self.kite, self.spark, self.options_df, self.expiries, config, "bear_put")
        self.bear_put_worker.log_signal.connect(self.log_msg)
        self.bear_put_worker.error_signal.connect(lambda e: self.log_msg(f"❌ BEAR PUT ERROR: {e}"))
        self.bear_put_worker.chart_ready_signal.connect(self.update_bear_charts)
        self.bear_put_worker.start()

    def stop_loops(self, wait=True):
        for w in [self.data_worker, self.chart_worker, self.bull_call_worker, self.bear_put_worker]:
            if w:
                w.stop()
                if wait: w.wait()

    def on_strike_text_changed(self):
        # Debounce to prevent restarting on every keystroke
        self.strike_watcher_timer.stop()
        self.strike_watcher_timer.start(1000) # 1 sec debounce

    def handle_strike_changed(self):
        # Only restart if loops are currently active (instruments done)
        if not self.kite or not self.spark or not self.data_worker:
            return
            
        config = self.get_config()
        if not config: return
        
        self.log_msg(f"🔄 Custom Strike changed to {config.CUSTOM_STRIKE}. Restarting execution loops...")
        self.lbl_status.setText("Status: Restarting Loops...")
        self.lbl_status.setStyleSheet("font-weight: bold; color: orange;")
        
        self.stop_loops(wait=True)
        self.start_loops(config)

    def update_charts(self, html_file_path):
        url = QUrl.fromLocalFile(os.path.abspath(html_file_path))
        self.webview.load(url)
    
    def update_bull_charts(self, html_file_path):
        url = QUrl.fromLocalFile(os.path.abspath(html_file_path))
        self.webview_bull.load(url)
    
    def update_bear_charts(self, html_file_path):
        url = QUrl.fromLocalFile(os.path.abspath(html_file_path))
        self.webview_bear.load(url)

    def stop_pipeline(self):
        self.log_msg("Initiating graceful shutdown...")
        self.lbl_status.setText("Status: Stopping...")
        self.lbl_status.setStyleSheet("font-weight: bold; color: red;")
        self.btn_stop.setEnabled(False)
        
        self.stop_loops(wait=False)
            
        self.log_msg("Pipeline stopped.")
        self.btn_start.setEnabled(True)
        self.lbl_status.setText("Status: Idle")
        self.lbl_status.setStyleSheet("font-weight: bold; color: blue;")
        
        self.kite = None
        self.spark = None

    def closeEvent(self, event):
        self.stop_pipeline()
        self.stop_loops(wait=True)
        if self.instruments_worker: self.instruments_worker.wait()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    window = StraddleApp()
    window.show()
    sys.exit(app.exec())
