"""
kite_helpers.py
===============
KiteConnect authentication helpers.

Handles:
  - Loading credentials from .env
  - Performing automated TOTP-based login via the Zerodha web session
  - Generating a KiteConnect session (access token)
  - Creating a KiteTicker websocket instance

Usage:
    from utils.kite_helpers import kite_login

    kite, kws, access_token = kite_login()
"""

import os
import json
import logging

import pyotp
import requests
from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL constants
# ---------------------------------------------------------------------------
BASE_URL = "https://kite.zerodha.com"
LOGIN_URL = f"{BASE_URL}/api/login"
TWOFA_URL = f"{BASE_URL}/api/twofa"


def _load_credentials() -> dict:
    """Load Zerodha API credentials from the .env file.

    Returns
    -------
    dict
        Keys: user_id, password, totp_key, api_key, api_secret
    """
    load_dotenv()
    creds = {
        "user_id":    os.getenv("ZERODHA_USER_ID"),
        "password":   os.getenv("ZERODHA_PASSWORD"),
        "totp_key":   os.getenv("ZERODHA_TOTP_KEY"),
        "api_key":    os.getenv("ZERODHA_API_KEY"),
        "api_secret": os.getenv("ZERODHA_API_SECRET"),
    }
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            "Ensure .env file is present and populated."
        )
    return creds


def kite_login() -> tuple:
    """Perform automated TOTP-based login to Zerodha KiteConnect.

    The login flow:
        1. POST username/password to Zerodha login endpoint.
        2. POST TOTP OTP to the 2FA endpoint.
        3. Use the redirect URL (containing request_token) to generate a session.
        4. Set the access token on the KiteConnect instance.

    Returns
    -------
    tuple
        (kite: KiteConnect, kws: KiteTicker, access_token: str)

    Raises
    ------
    ValueError
        If the request_token cannot be extracted from the redirect URL.
    requests.exceptions.RequestException
        If any HTTP call fails.
    """
    creds = _load_credentials()

    session = requests.Session()

    # Step 1: Username + password login
    logger.info("Logging in as user: %s", creds["user_id"])
    response = session.post(
        LOGIN_URL,
        data={"user_id": creds["user_id"], "password": creds["password"]},
        timeout=15,
    )
    response.raise_for_status()
    request_id = json.loads(response.text)["data"]["request_id"]

    # Step 2: TOTP 2FA
    twofa_pin = pyotp.TOTP(creds["totp_key"]).now()
    logger.info("Generated TOTP pin for 2FA")
    response_2fa = session.post(
        TWOFA_URL,
        data={
            "user_id":     creds["user_id"],
            "request_id":  request_id,
            "twofa_value": twofa_pin,
            "twofa_type":  "totp",
        },
        timeout=15,
    )
    response_2fa.raise_for_status()

    # Step 3: Extract request_token from redirect
    kite = KiteConnect(api_key=creds["api_key"])
    kite_url = kite.login_url()
    request_token = None
    try:
        session.get(kite_url, timeout=15)
    except Exception as exc:
        e_msg = str(exc)
        if "request_token=" not in e_msg:
            raise ValueError(
                f"Could not extract request_token from redirect. "
                f"Exception message: {e_msg}"
            ) from exc
        request_token = (
            e_msg.split("request_token=")[1].split(" ")[0].split("&action")[0]
        )

    if not request_token:
        raise ValueError("request_token is empty after login redirect.")

    logger.info("Login successful — request_token: %s", request_token)

    # Step 4: Generate session
    data = kite.generate_session(request_token, creds["api_secret"])
    access_token = data["access_token"]
    kite.set_access_token(access_token)

    # Step 5: Create ticker
    kws = KiteTicker(creds["api_key"], access_token)

    logger.info("KiteConnect session established for user: %s", creds["user_id"])
    return kite, kws, access_token


def get_spark_session(app_name: str = "BNF_Options_Trading"):
    """Create or retrieve a SparkSession configured for local use with G1GC.

    Parameters
    ----------
    app_name : str
        Spark application name shown in the UI.

    Returns
    -------
    SparkSession
    """
    import os
    import pyspark  # noqa: F401
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        .config(
            "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors",
            "G1 Concurrent GC",
        )
        .config(
            "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors",
            "G1 Concurrent GC",
        )
        .config("spark.memory.offHeap.use", True)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    return spark
