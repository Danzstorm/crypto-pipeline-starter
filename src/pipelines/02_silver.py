"""
Capa Silver — typing, dedup y expectations.
"""

from pyspark import pipelines as dp


@dp.table(
    name="coin_prices",
    schema="crypto.silver",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    cluster_by=["symbol"],
    comment="Precios tipados y deduplicados por (symbol, snapshot_id).",
)
@dp.expect("price_positive", "price_usd > 0")
@dp.expect_or_drop("symbol_not_null", "symbol IS NOT NULL")
@dp.expect("valid_market_cap", "market_cap_usd >= 0")
def coin_prices():
    bronze = spark.readStream.table("crypto.bronze.coin_prices_raw")
    return (
        bronze.selectExpr(
            "UPPER(payload:symbol::string) AS symbol",
            "payload:name::string AS name",
            "CAST(payload:current_price AS DECIMAL(18, 8)) AS price_usd",
            "CAST(payload:total_volume AS DECIMAL(20, 2)) AS volume_24h_usd",
            "CAST(payload:market_cap AS DECIMAL(20, 2)) AS market_cap_usd",
            "CAST(payload:price_change_percentage_1h_in_currency AS DOUBLE) AS pct_change_1h",
            "CAST(payload:price_change_percentage_24h AS DOUBLE) AS pct_change_24h",
            "CAST(payload:price_change_percentage_7d_in_currency AS DOUBLE) AS pct_change_7d",
            "snapshot_id AS observed_at",
        )
        .withWatermark("observed_at", "1 hour")
        .dropDuplicatesWithinWatermark(["symbol", "observed_at"], "1 minute")
    )
