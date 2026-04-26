"""
Capa Bronze — append-only, lee del Volume con Auto Loader.
"""

from pyspark import pipelines as dp


@dp.table(
    name="coin_prices_raw",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Datos crudos de CoinGecko, JSON sin tipar.",
)
@dp.expect_or_drop(
    "non_empty_payload",
    "payload IS NOT NULL AND length(payload) > 10",
)
def coin_prices_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/crypto/raw/_schemas/bronze",
        )
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load("/Volumes/crypto/raw/coin_prices/")
        .selectExpr(
            "_metadata.file_path AS _source_file",
            "to_json(struct(*)) AS payload",
            "current_timestamp() AS _ingestion_ts",
            "_rescued_data",
        )
    )
