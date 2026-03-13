import sys
import os
import logging
from dataclasses import dataclass
from datetime import datetime

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QLineEdit, QCheckBox, QPushButton, QTextEdit, QGridLayout,
    QGroupBox, QSplitter, QScrollArea, QMessageBox
)
from PyQt5.QtCore import Qt, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView
from dotenv import load_dotenv

# Load variables from .env file if present
load_dotenv()

# Import the worker threads
from straddle_runner import InstrumentsWorker, DataFetchWorker, ChartWorker

# Configure root logger to not spam the console if we don't want to
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class PipelineConfig:
    BANKNIFTY: bool = True
    NIFTY: bool = False
    CUSTOM_STRIKE: int = 0
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


class StraddleApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BankNifty Straddle Pipeline")
        self.resize(1600, 1000)
        
        self.kite = None
        self.spark = None
        self.options_df = None
        self.expiries = None
        
        self.instruments_worker = None
        self.data_worker = None
        self.chart_worker = None
        
        self.init_ui()
        
    def init_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        
        # Splitter to allow resizing config/logs vs charts
        h_splitter = QSplitter(Qt.Horizontal)
        
        # Left Panel: Config & Logs
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # 1. Config Group
        config_group = QGroupBox("Configuration Parameters")
        grid = QGridLayout()
        config_group.setLayout(grid)
        
        self.chk_bnf = QCheckBox("BANKNIFTY")
        self.chk_bnf.setChecked(True)
        self.chk_nifty = QCheckBox("NIFTY")
        
        self.txt_custom_strike = QLineEdit("0")
        self.txt_num_strikes = QLineEdit("9")
        self.chk_next_expiry = QCheckBox("PULL_NEXT_EXPIRY")
        self.txt_history_days = QLineEdit("1")
        self.txt_loop_interval = QLineEdit("1")
        
        self.txt_strike_name = QLineEdit("ATM")
        self.txt_ce_pe = QLineEdit("E")
        self.txt_num_days_chart = QLineEdit("2")
        self.chk_latest_day = QCheckBox("IS_LATEST_DAY")
        self.chk_latest_day.setChecked(True)
        
        self.txt_end_hour = QLineEdit("15")
        self.txt_end_minute = QLineEdit("30")
        
        # Layout the grid
        row = 0
        grid.addWidget(self.chk_bnf, row, 0)
        grid.addWidget(self.chk_nifty, row, 1)
        row += 1
        grid.addWidget(QLabel("CUSTOM_STRIKE (0=LTP):"), row, 0)
        grid.addWidget(self.txt_custom_strike, row, 1)
        row += 1
        grid.addWidget(QLabel("NUM_STRIKES (+/- ATM):"), row, 0)
        grid.addWidget(self.txt_num_strikes, row, 1)
        row += 1
        grid.addWidget(self.chk_next_expiry, row, 0, 1, 2)
        row += 1
        grid.addWidget(QLabel("NUM_DAYS_HISTORY:"), row, 0)
        grid.addWidget(self.txt_history_days, row, 1)
        row += 1
        grid.addWidget(QLabel("LOOP_INTERVAL_MIN:"), row, 0)
        grid.addWidget(self.txt_loop_interval, row, 1)
        row += 1
        grid.addWidget(QLabel("STRIKE_LEVEL_NAME:"), row, 0)
        grid.addWidget(self.txt_strike_name, row, 1)
        row += 1
        grid.addWidget(QLabel("CE_OR_PE ('E'=straddle):"), row, 0)
        grid.addWidget(self.txt_ce_pe, row, 1)
        row += 1
        grid.addWidget(QLabel("NUM_DAYS (Charts):"), row, 0)
        grid.addWidget(self.txt_num_days_chart, row, 1)
        row += 1
        grid.addWidget(self.chk_latest_day, row, 0, 1, 2)
        row += 1
        grid.addWidget(QLabel("END_HOUR (24h):"), row, 0)
        grid.addWidget(self.txt_end_hour, row, 1)
        row += 1
        grid.addWidget(QLabel("END_MINUTE:"), row, 0)
        grid.addWidget(self.txt_end_minute, row, 1)
        
        left_layout.addWidget(config_group)
        
        # 2. Controls
        h_ctrl = QHBoxLayout()
        self.btn_start = QPushButton("▶ START PIPELINE")
        self.btn_start.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px; font-weight: bold;")
        self.btn_start.clicked.connect(self.start_pipeline)
        
        self.btn_stop = QPushButton("⏹ STOP PIPELINE")
        self.btn_stop.setStyleSheet("background-color: #f44336; color: white; padding: 10px; font-weight: bold;")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_pipeline)
        
        h_ctrl.addWidget(self.btn_start)
        h_ctrl.addWidget(self.btn_stop)
        left_layout.addLayout(h_ctrl)
        
        # 3. Status
        self.lbl_status = QLabel("Status: Idle")
        self.lbl_status.setStyleSheet("font-weight: bold; color: blue;")
        left_layout.addWidget(self.lbl_status)
        
        # 4. Logs
        self.text_log = QTextEdit()
        self.text_log.setReadOnly(True)
        self.text_log.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: monospace;")
        left_layout.addWidget(self.text_log)
        
        # Right Panel: Interactive Charts
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        self.webview = QWebEngineView()
        # Set a dark background HTML as default
        self.webview.setHtml("<html><body style='background-color: #1e1e2f; color: white; display:flex; justify-content:center; align-items:center; height:100vh;'><h3>Interactive charts will load here...</h3></body></html>")
        right_layout.addWidget(self.webview)
        
        # Add to splitter
        h_splitter.addWidget(left_panel)
        h_splitter.addWidget(right_panel)
        h_splitter.setSizes([400, 1200]) # initial sizes
        
        main_layout.addWidget(h_splitter)
        self.setCentralWidget(main_widget)

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
                NUM_STRIKES=int(self.txt_num_strikes.text()),
                PULL_NEXT_EXPIRY=self.chk_next_expiry.isChecked(),
                NUM_DAYS_HISTORY=int(self.txt_history_days.text()),
                LOOP_INTERVAL_MIN=int(self.txt_loop_interval.text()),
                STRIKE_LEVEL_NAME=self.txt_strike_name.text(),
                CE_OR_PE=self.txt_ce_pe.text(),
                NUM_DAYS=int(self.txt_num_days_chart.text()),
                IS_LATEST_DAY=self.chk_latest_day.isChecked(),
                END_HOUR=int(self.txt_end_hour.text()) if self.txt_end_hour.text() else None,
                END_MINUTE=int(self.txt_end_minute.text()) if self.txt_end_minute.text() else None,
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
        
        self.lbl_status.setText("Status: Step 2 & 3 Running Concurrently ●")
        self.lbl_status.setStyleSheet("font-weight: bold; color: green;")
        
        # Start Step 2
        self.data_worker = DataFetchWorker(self.kite, self.spark, config)
        self.data_worker.log_signal.connect(self.log_msg)
        self.data_worker.error_signal.connect(lambda e: self.log_msg(f"❌ DATA ERROR: {e}"))
        self.data_worker.start()
        
        # Start Step 3
        self.chart_worker = ChartWorker(self.kite, self.spark, self.options_df, self.expiries, config)
        self.chart_worker.log_signal.connect(self.log_msg)
        self.chart_worker.error_signal.connect(lambda e: self.log_msg(f"❌ CHART ERROR: {e}"))
        self.chart_worker.chart_ready_signal.connect(self.update_charts)
        self.chart_worker.start()

    def update_charts(self, html_file_path):
        """Loads the interactive Plotly HTML into the QWebEngineView."""
        url = QUrl.fromLocalFile(os.path.abspath(html_file_path))
        self.webview.load(url)

    def stop_pipeline(self):
        self.log_msg("Initiating graceful shutdown...")
        self.lbl_status.setText("Status: Stopping...")
        self.lbl_status.setStyleSheet("font-weight: bold; color: red;")
        self.btn_stop.setEnabled(False)
        
        if self.data_worker:
            self.data_worker.stop()
        if self.chart_worker:
            self.chart_worker.stop()
            
        self.log_msg("Pipeline stopped.")
        self.btn_start.setEnabled(True)
        self.lbl_status.setText("Status: Idle")
        self.lbl_status.setStyleSheet("font-weight: bold; color: blue;")

    def closeEvent(self, event):
        self.stop_pipeline()
        # Ensure threads finish before exiting
        if self.data_worker: self.data_worker.wait()
        if self.chart_worker: self.chart_worker.wait()
        if self.instruments_worker: self.instruments_worker.wait()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    # Change to root dir so DataFiles are accessible
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    window = StraddleApp()
    window.show()
    sys.exit(app.exec_())

