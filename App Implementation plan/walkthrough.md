# Unified BankNifty Straddle Application — Walkthrough

I have built the unified executable application that orchestrates all 3 steps of the BankNifty Straddle pipeline, complete with a desktop GUI and fully interactive charts.

## What was built

1. **[PROD/straddle_app.py](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/PROD/straddle_app.py)** — The main PyQt5 dashboard application. It provides:
   - A configuration panel on the left with all the inputs from your screenshot.
   - A Start/Stop pipeline control system.
   - A scrollable live log panel for real-time status output.
   - A large interactive chart viewer on the right powered by Chromium (`QWebEngineView`).

2. **[PROD/straddle_runner.py](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/PROD/straddle_runner.py)** — The multi-threaded pipeline logic.
   - **`InstrumentsWorker`**: Runs Step 1 (logs in, fetches instruments, calculates expiries).
   - **`DataFetchWorker`**: Runs Step 2 in a continuous background loop (downloads OHLC and saves to Parquet).
   - **`ChartWorker`**: Runs Step 3 concurrently. It renders all 6 Plotly charts. Instead of opening a browser pop-up `fig.show()`, it converts the figures to interactive HTML and displays them directly in the app. It also stitches all 6 charts into a single `Combined_Dashboard_<date>.png` and saves it to `ChartsSnapshots/`.

3. **`run_straddle_app.sh`** — A convenient launcher script.

4. **Updated [requirements.txt](file:///Users/yogeshpatil/Documents/Trading/Antigravety/CHEWY_BNF_Options_Trading/requirements.txt)** — Added `PyQt5`, `PyQtWebEngine`, `kaleido` (for static image export), and `Pillow` (for image stitching).

## How to Run & Verify

Before launching, please install the new UI dependencies from your terminal:

```bash
pip install -r requirements.txt
```

Then, you can launch the application by running the launcher script from the root folder:

```bash
./run_straddle_app.sh
```

Or run Python directly:
```bash
python PROD/straddle_app.py
```

### What to check:
- **GUI Features**: You should see all configurable parameters natively editable right in the app.
- **Interactive Charts**: Once you click Start and data populates, the charts pane on the right will load. These are **fully interactive Plotly charts** — you can hover, zoom, pan, and click the legend items.
- **Shared Snapshot**: After each chart refresh, look in `ChartsSnapshots/YYYY-MM-DD/`. You will find a single tall PNG file named `Combined_Dashboard_<date>.png` containing all the charts, exactly like the screenshot you provided.
- **Graceful Stop**: Clicking the Stop button will safely interrupt the continuous loops and gracefully shut down the threads.
