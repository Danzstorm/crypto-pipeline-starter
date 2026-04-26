# 03 — Medallion (SDP)

## Diseño general

- **Bronze:** streaming table desde un PySpark Custom Data Source que llama directamente a la API de CoinGecko.
- **Silver:** streaming table con typing, dedup, expectations.
- **Gold:** vistas materializadas (no streaming) para los KPIs.

## Configuración del pipeline

- Tipo: triggered (no continuous).
- Compute: serverless (obligatorio en Free Edition).
- Catalog: `crypto`.
- Edition: ADVANCED.
- Channel: PREVIEW.

## Bronze — reglas

- Source: `CoinGeckoDataSource` (clase Python registrada en el pipeline).
- Cada microbatch del Job ejecuta una llamada a `api.coingecko.com/api/v3/coins/markets` y produce 100 filas.
- Cada fila lleva un `snapshot_id` (timestamp UTC) que identifica unívocamente la captura.
- Conserva el JSON crudo en columna `payload`.
- Append-only.

## Silver — reglas

- Lee de `crypto.bronze.coin_prices_raw` como streaming.
- Extrae cada campo del payload con `selectExpr` y notación `:`.
- Aplica `dropDuplicatesWithinWatermark` con ventana 1 min en (`symbol`, `snapshot_id`).
- Watermark: 1 hora.
- Liquid Clustering por `symbol`.

## Gold — reglas

- Vistas materializadas (refresh on update).
- `coin_momentum_24h`: snapshot del estado más reciente por símbolo, con `RANK()`.
- `coin_volatility_7d`: agregación últimos 7 días, devuelve volatilidad anualizada.

## Calidad

| Tabla | Expectation | Acción si falla |
|---|---|---|
| bronze | `non_empty_payload` | drop |
| silver | `price_positive` | warn |
| silver | `symbol_not_null` | drop |
| silver | `valid_market_cap` | warn |
