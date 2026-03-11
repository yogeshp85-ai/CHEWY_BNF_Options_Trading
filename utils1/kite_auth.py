"""
utils/kite_auth.py — Reusable Zerodha KiteConnect authentication helper.

Usage in a notebook or script:
    from utils.kite_auth import get_kite_session

    kite, kws, access_token = get_kite_session()
"""

import json
import requests
import pyotp
from kiteconnect import KiteConnect, KiteTicker

# Import credentials from config.py (which reads from .env)
from config import CREDS, LOGIN_URL, TWOFA_URL


def get_kite_session(creds: dict = None) -> tuple[KiteConnect, KiteTicker, str]:
    """
    Perform the full Zerodha login flow and return an authenticated
    KiteConnect instance, a KiteTicker instance, and the access token.

    Parameters
    ----------
    creds : dict, optional
        Dictionary with keys: user_id, password, totp_key, api_key, api_secret.
        Defaults to the CREDS dict loaded from config.py / .env.

    Returns
    -------
    kite : KiteConnect
        Authenticated KiteConnect REST client.
    kws : KiteTicker
        KiteTicker WebSocket client (not yet connected).
    access_token : str
        The session access token.

    Raises
    ------
    ValueError
        If credentials are missing or login fails.
    """
    if creds is None:
        creds = CREDS

    _validate_creds(creds)

    session = requests.Session()

    # Step 1: Password login
    response = session.post(
        LOGIN_URL,
        data={"user_id": creds["user_id"], "password": creds["password"]},
    )
    response.raise_for_status()
    login_data = json.loads(response.text)
    request_id = login_data["data"]["request_id"]

    # Step 2: TOTP 2FA
    twofa_pin = pyotp.TOTP(creds["totp_key"]).now()
    response_2fa = session.post(
        TWOFA_URL,
        data={
            "user_id":     creds["user_id"],
            "request_id":  request_id,
            "twofa_value": twofa_pin,
            "twofa_type":  "totp",
        },
    )
    response_2fa.raise_for_status()

    # Step 3: Extract request_token from redirect URL
    kite = KiteConnect(api_key=creds["api_key"])
    kite_url = kite.login_url()
    try:
        session.get(kite_url)
    except Exception as e:
        e_msg = str(e)

    request_token = e_msg.split("request_token=")[1].split(" ")[0].split("&action")[0]
    print(f"Successful Login with Request Token: {request_token}")

    # Step 4: Generate session
    data = kite.generate_session(request_token, creds["api_secret"])
    access_token = data["access_token"]
    kite.set_access_token(access_token)

    # Step 5: KiteTicker
    kws = KiteTicker(creds["api_key"], access_token)

    return kite, kws, access_token


def _validate_creds(creds: dict) -> None:
    """Raise ValueError if any required credential field is empty."""
    required = ["user_id", "password", "totp_key", "api_key", "api_secret"]
    missing = [k for k in required if not creds.get(k)]
    if missing:
        raise ValueError(
            f"Missing credentials: {missing}. "
            "Set them in your .env file or config.py."
        )
