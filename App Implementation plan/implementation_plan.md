# Unified BankNifty Straddle — Desktop GUI Application

Build a standalone PyQt5 desktop application that orchestrates all 3 steps of the BankNifty Straddle pipeline in a single execution, with configurable parameters, live status logging, and **interactive Plotly charts**.

## Proposed Changes

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PyQt5 GUI Window                          │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Config Panel (all parameters from screenshot)         │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │  [▶ Start]  [⏹ Stop]               Status: Running ● │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │  Live Log (scrollable, timestamped)                    │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │  Interactive Charts (QWebEngineView — full Plotly)     │  │
│  │  ┌─────────────────────────────────────────────────┐  │  │
│  │  │ Fig1: Price & Signals | ROC AVG                 │  │  │
│  │  ├─────────────────────────────────────────────────┤  │  │
│  │  │ Fig2: CE vs PE Close | CE/PE ROC & Ratio        │  │  │
│  │  ├─────────────────────────────────────────────────┤  │  │
│  │  │ Fig3: Open Interest | CE/PE OI + PUT-CALL OI    │  │  │
│  │  ├─────────────────────────────────────────────────┤  │  │
│  │  │ Fig4: OI PCR                                    │  │  │
│  │  ├─────────────────────────────────────────────────┤  │  │
│  │  │ Fig5: Straddle Price/VWAP/OIWAP + PUT-CALL OI  │  │  │
│  │  ├─────────────────────────────────────────────────┤  │  │
│  │  │ Fig6: OHLC Candlestick + Volume                 │  │  │
│  │  └─────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Interactive charts approach:**
- Use **`QWebEngineView`** (Qt's Chromium-based browser widget) to render Plotly charts as interactive HTML
- All 6 chart figures are combined into a single HTML page with Plotly.js CDN
- Charts support full interactivity: **zoom, pan, hover tooltips, legend toggle, crosshair**
- **Simultaneously**, save a combined PNG snapshot (all charts stitched vertically) to `ChartsSnapshots/<today>/` — exactly like the attached screenshot

**Threading model:**
- **Main thread** → PyQt5 event loop (GUI always responsive)
- **Worker thread 1** → Step 1 (instruments) → Step 2 (data fetch loop)
- **Worker thread 2** → Step 3 (chart loop), started after Step 1 completes
- Chart worker generates Plotly figures → converts to HTML string → emits signal to GUI → GUI loads HTML in QWebEngineView
- Chart worker also saves combined PNG snapshot at each refresh

---

### Application Module

#### [NEW] [straddle_app.py](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/PROD/straddle_app.py)

Single-file PyQt5 desktop application. Key components:

1. **`PipelineConfig` dataclass** — all configurable parameters from the screenshot

2. **`InstrumentsWorker(QThread)`** — Step 1:
   - [kite_login()](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/PROD/utils/kite_helpers.py#69-151) → [get_spark_session()](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/PROD/utils/kite_helpers.py#153-188) → fetch/filter instruments → write Parquet → calc expiries
   - Emits: `log_signal`, `finished_signal(kite, spark, bnf_options, bnf_expiries)`

3. **`DataFetchWorker(QThread)`** — Step 2 loop:
   - Calls [get_latest_data()](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/PROD/utils/data_fetcher.py#164-332) in a loop with `time.sleep()`
   - Graceful stop via `self._stop_event`

4. **`ChartWorker(QThread)`** — Step 3 loop:
   - Calls chart data functions → builds Plotly figures → emits `chart_html_signal(html_str)` + saves combined PNG
   - Does NOT call `fig.show()` — instead calls `fig.to_html(include_plotlyjs=False)` for each figure, wraps in a single HTML page with Plotly.js CDN
   - Uses `fig.write_image()` to save individual PNGs, then stitches them vertically into `Combined_Dashboard_<date>.png`

5. **`StraddleApp(QMainWindow)`** — main window:
   - Config panel, Start/Stop, log panel, and scrollable `QWebEngineView` for interactive charts
   - On chart_html_signal → load HTML into QWebEngineView (instant interactive rendering)

**Key design decisions:**
- Reuses all existing utility functions from `utils/` **without modification**
- Charts are fully interactive in the GUI AND saved as combined PNG snapshots
- No `fig.show()` browser pop-ups — charts render inside the app window
- No `IPython.display.clear_output()` — all output goes to GUI log panel

---

### Dependencies

#### [MODIFY] [requirements.txt](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/requirements.txt)

Add: `PyQt5>=5.15.0`, `PyQtWebEngine>=5.15.0`, `Pillow>=10.0.0`, `kaleido>=0.2.1`

---

### Launcher

#### [NEW] [run_straddle_app.sh](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/run_straddle_app.sh)

Shell script: `cd PROD && python straddle_app.py`

---

## User Review Required

> [!IMPORTANT]
> Charts will be **fully interactive** (zoom, pan, hover, crosshair) inside the desktop app using an embedded Chromium widget. A combined PNG snapshot (like the attached screenshot) will also be saved to `ChartsSnapshots/<today>/` at each refresh.

> [!IMPORTANT]
> Existing notebooks and utility modules will **NOT be modified**. The app imports and calls existing functions directly.

---

## Verification Plan

### Manual Verification
1. **GUI launch**: Verify window opens with all config fields, defaults populated
2. **Interactive charts**: Verify charts are zoomable, pannable, show hover tooltips
3. **PNG snapshots**: Verify `ChartsSnapshots/<today>/Combined_Dashboard_<date>.png` is saved
4. **Pipeline flow**: Start → Step 1 completes → Step 2 + Step 3 run concurrently → auto-stop at END_HOUR:END_MINUTE
5. **Stop button**: Verify graceful shutdown within seconds
