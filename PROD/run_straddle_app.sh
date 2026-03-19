#!/bin/bash
# BankNifty Straddle - Unified GUI Application Launcher

# Navigate to the project directory
cd "$(dirname "$0")/.."

# Export Java Home so Spark can pick it up
export JAVA_HOME=$(/usr/libexec/java_home)

# Ensure PySpark uses the correct Python executable
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python3"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python3"

# Run the app 
cd PROD
export PYTHONPATH="$(pwd)"
# Force python3 (resolves to 3.12 internally) for PySpark compatibility
../.venv/bin/python3 straddle_app.py
