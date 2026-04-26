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

## Por qué corre en local y no en Databricks

Databricks Free Edition restringe el outbound del cómputo serverless a un set acotado de dominios "trusted". CoinGecko no está garantizado en esa lista.

Solución: el ingester corre en la laptop. Captura el JSON, lo escribe localmente, lo sube al Volume vía Files API (que sí es trusted). SDP lee del Volume internamente.

Este patrón también es buena práctica en producción real: separa captura de procesamiento.

## Cadencia y particionado

- Frecuencia: cada 10 minutos (manual durante el desarrollo).
- Path destino: `/Volumes/crypto/raw/coin_prices/dt=YYYY-MM-DD/hh=HH/<uuid>.jsonl`
- Formato: JSON Lines (un JSON object por línea).
- Tamaño esperado: ~80 KB por archivo, ~14 MB/día.

## Resiliencia

- 3 intentos con backoff exponencial (1s, 2s, 4s).
- Si los 3 fallan: escribir a `/Volumes/crypto/raw/_dead_letter/` con el error.
- Logs con timestamp ISO 8601 a stdout.

## Autenticación

- Variable de entorno: `DATABRICKS_CONFIG_PROFILE=crypto`.
- El SDK lee `~/.databrickscfg` automáticamente.
- No hardcodear el PAT en el código bajo ninguna circunstancia.
