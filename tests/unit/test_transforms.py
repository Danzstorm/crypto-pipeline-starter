"""
Tests unitarios de las transformaciones Silver.
Corre localmente con: pytest tests/unit/
"""

import json

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_transforms")
        .getOrCreate()
    )


def test_silver_extracts_symbol_uppercase(spark):
    """El símbolo debe quedar en mayúsculas y los decimales bien tipados."""
    payload = json.dumps({
        "symbol": "btc",
        "current_price": 65000.50,
        "name": "Bitcoin",
        "total_volume": 30_000_000_000,
        "market_cap": 1_300_000_000_000,
        "price_change_percentage_24h": 2.5,
    })
    df = spark.createDataFrame(
        [(payload, "2026-04-26T12:00:00Z")],
        schema=["payload", "_ingestion_ts"],
    )
    transformed = df.selectExpr(
        "UPPER(payload:symbol::string) AS symbol",
        "CAST(payload:current_price AS DECIMAL(18, 8)) AS price_usd",
    )
    row = transformed.first()
    assert row["symbol"] == "BTC"
    assert float(row["price_usd"]) == 65000.50
