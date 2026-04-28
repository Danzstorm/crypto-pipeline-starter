"""
Capa Bronze — captura directa desde la API de CoinGecko via PySpark Custom Data Source.

Patrón:
- Cada microbatch del pipeline ejecuta UNA llamada a CoinGecko.
- La respuesta (100 monedas) se transforma en 100 filas con snapshot_id común.
- SDP la trata como cualquier otra streaming source.

Por qué Custom Data Source y no requests.get directo en una @dp.table:
- Spark necesita un schema declarado para validar.
- El patrón Custom Data Source es la forma idiomática de SDP para fuentes externas.
- Maneja correctamente reintentos y observabilidad.

Validado en Free Edition serverless con test_coingecko_databricks.py.
"""

import datetime as dt
import json
import time
from typing import Iterator

import requests
from pyspark import pipelines as dp
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "price_change_percentage": "1h,24h,7d",
}
TIMEOUT_SECONDS = 30


# =============================================================================
# Custom Data Source: CoinGeckoDataSource
# =============================================================================

class CoinGeckoDataSource(DataSource):
    """Data source que envuelve la API de CoinGecko como una fuente de Spark."""

    @classmethod
    def name(cls) -> str:
        return "coingecko"

    def schema(self) -> StructType:
        return StructType([
            StructField("snapshot_id", TimestampType(), nullable=False),
            StructField("payload", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        return CoinGeckoReader(schema)

    def streamReader(self, schema: StructType) -> "CoinGeckoStreamReader":
        return CoinGeckoStreamReader(schema)


class CoinGeckoStreamReader(DataSourceStreamReader):
    """Stream reader para SDP: cada microbatch llama una vez a CoinGecko."""

    def __init__(self, schema: StructType):
        self.schema = schema

    def initialOffset(self) -> dict:
        return {"timestamp": 0}

    def latestOffset(self) -> dict:
        return {"timestamp": int(dt.datetime.utcnow().timestamp())}

    def partitions(self, start: dict, end: dict) -> list[InputPartition]:
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        rows = _fetch_with_retry()
        snapshot_id = dt.datetime.utcnow()
        source = "coingecko-api-v3"
        for row in rows:
            yield (snapshot_id, json.dumps(row, ensure_ascii=False), source)

    def commit(self, end: dict) -> None:
        pass


class CoinGeckoReader(DataSourceReader):
    """Reader que ejecuta la llamada HTTP y emite las filas."""

    def __init__(self, schema: StructType):
        self.schema = schema

    def partitions(self) -> list[InputPartition]:
        # Una sola partición: una llamada produce todas las filas a la vez.
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        rows = _fetch_with_retry()
        snapshot_id = dt.datetime.utcnow()
        source = "coingecko-api-v3"
        for row in rows:
            yield (
                snapshot_id,
                json.dumps(row, ensure_ascii=False),
                source,
            )


def _fetch_with_retry(retries: int = 3) -> list[dict]:
    """Llama a CoinGecko con backoff exponencial."""
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.get(
                COINGECKO_URL,
                params=COINGECKO_PARAMS,
                timeout=TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            wait = 2 ** attempt
            time.sleep(wait)
    raise RuntimeError(
        f"CoinGecko fetch failed after {retries} attempts: {last_error}"
    )


# =============================================================================
# Registro del data source y declaración de la streaming table
# =============================================================================

# Registrar el data source una vez por pipeline.
spark.dataSource.register(CoinGeckoDataSource)


@dp.table(
    name="coin_prices_raw",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Snapshots crudos de CoinGecko, JSON sin tipar. 100 filas por captura.",
)
@dp.expect_or_drop(
    "non_empty_payload",
    "payload IS NOT NULL AND length(payload) > 10",
)
def coin_prices_raw():
    return (
        spark.readStream
        .format("coingecko")
        .load()
    )
