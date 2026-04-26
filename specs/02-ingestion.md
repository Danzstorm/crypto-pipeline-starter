# 02 — Ingesta

## Fuente

- API: CoinGecko `/coins/markets`
- URL base: `https://api.coingecko.com/api/v3`
- Auth: ninguna (free tier).
- Rate limit: ~50 req/min sin API key.

## Parámetros

```json
{
  "vs_currency": "usd",
  "order": "market_cap_desc",
  "per_page": 100,
  "page": 1,
  "price_change_percentage": "1h,24h,7d"
}
```

## Patrón de ingesta

CoinGecko devuelve un **snapshot completo** en cada llamada (estado actual de las 100 monedas top). No hay un endpoint incremental ni eventos. Por lo tanto:

- Cada captura agrega 100 filas nuevas a Bronze, etiquetadas con un `snapshot_id` (timestamp UTC del momento de la captura).
- Bronze crece linealmente: ~100 filas × 96 capturas/día = ~9,600 filas/día. Trivial para Free Edition.
- Silver deduplica por `(symbol, snapshot_id)` con ventana corta para protegerse de re-ejecuciones del Job.
- Gold lee solo el snapshot más reciente por símbolo para los KPIs.

## Implementación: PySpark Custom Data Source

La captura se hace con una clase Python `CoinGeckoDataSource` que extiende `pyspark.sql.datasource.DataSource`. SDP la trata como cualquier otra streaming source. Beneficios:

- No necesitamos Volume intermedio.
- No necesitamos cron externo: el Job de Databricks dispara el pipeline y el data source captura automáticamente.
- Schema declarado, así Spark valida la respuesta de la API.
- Re-intentos automáticos si la API falla en una ejecución (el siguiente trigger del Job lo intenta de nuevo).

## Cadencia

- Frecuencia: cada 15 minutos vía Lakeflow Job.
- Cada ejecución del Job → 1 llamada a CoinGecko → 100 filas nuevas en Bronze.
- 96 capturas por día = 9,600 filas/día en Bronze.

## Resiliencia

- 3 intentos con backoff exponencial dentro del data source.
- Si los 3 intentos fallan, el batch falla y el Job notifica por email.
- El siguiente trigger lo retoma sin pérdida de datos (es snapshot, no eventos).

## Por qué esto funciona en Free Edition

Validado empíricamente con `test_coingecko_databricks.py`: el cómputo serverless de Free Edition puede alcanzar `api.coingecko.com` con `requests` estándar. Si en el futuro Databricks cambia las reglas de outbound, este componente sería el primero en romperse y habría que volver al patrón "ingester local + Volume" de la v1.
