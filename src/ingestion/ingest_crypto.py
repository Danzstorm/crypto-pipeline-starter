"""
Ingester local de precios de criptomonedas desde CoinGecko al Volume
crypto.raw.coin_prices en Databricks.

Corre en la laptop, no en Databricks (Free Edition tiene outbound
restringido). Sube via Files API.

Uso:
    DATABRICKS_CONFIG_PROFILE=crypto python src/ingestion/ingest_crypto.py
"""

import datetime as dt
import json
import os
import pathlib
import time
import uuid
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "price_change_percentage": "1h,24h,7d",
}
VOLUME_BASE = "/Volumes/crypto/raw/coin_prices"
DEAD_LETTER = "/Volumes/crypto/raw/_dead_letter"
TIMEOUT_SECONDS = 30


def log(message: str) -> None:
    """Logger simple con timestamp ISO 8601."""
    now = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"[{now}] {message}")


def fetch_coins(retries: int = 3) -> list[dict[str, Any]]:
    """Llama a CoinGecko con retry exponencial."""
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.get(API_URL, params=PARAMS, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            wait = 2**attempt
            log(f"Attempt {attempt + 1} failed: {exc}. Retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"CoinGecko fetch failed after {retries} attempts: {last_error}")


def write_jsonl_local(rows: list[dict[str, Any]], filename: str) -> pathlib.Path:
    """Escribe el JSONL en /tmp con timestamp de ingesta."""
    local_path = pathlib.Path(f"/tmp/{filename}")
    now_iso = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with local_path.open("w", encoding="utf-8") as f:
        for row in rows:
            row["_ingestion_ts"] = now_iso
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return local_path


def upload_to_volume(local_path: pathlib.Path, target_path: str) -> None:
    """Sube el archivo al Volume vía Files API."""
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    client = WorkspaceClient(profile=profile)
    with local_path.open("rb") as f:
        client.files.upload(target_path, f, overwrite=True)


def write_dead_letter(error: Exception) -> None:
    """En caso de falla total, deja un marcador en _dead_letter."""
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    client = WorkspaceClient(profile=profile)
    payload = {
        "timestamp": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "error": str(error),
    }
    target = f"{DEAD_LETTER}/{uuid.uuid4()}.json"
    client.files.upload(target, json.dumps(payload).encode("utf-8"), overwrite=True)
    log(f"Dead letter written -> {target}")


def main() -> None:
    try:
        coins = fetch_coins()
        log(f"Fetched {len(coins)} coins from CoinGecko")

        now = dt.datetime.utcnow()
        filename = f"{uuid.uuid4()}.jsonl"
        local_path = write_jsonl_local(coins, filename)

        target_path = f"{VOLUME_BASE}/dt={now:%Y-%m-%d}/hh={now:%H}/{filename}"
        upload_to_volume(local_path, target_path)
        log(f"Uploaded -> {target_path}")

        local_path.unlink()
    except Exception as exc:
        log(f"Fatal error: {exc}")
        write_dead_letter(exc)
        raise


if __name__ == "__main__":
    main()
