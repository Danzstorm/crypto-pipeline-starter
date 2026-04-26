# 03 — Medallion (SDP)

## Diseño general

- **Bronze:** streaming table desde Auto Loader sobre el Volume.
- **Silver:** streaming table con typing, dedup, expectations.
- **Gold:** vistas materializadas (no streaming) para los KPIs.

## Configuración del pipeline

- Tipo: triggered (no continuous).
- Compute: serverless (obligatorio en Free Edition).
- Catalog: `crypto`.
- Edition: ADVANCED.
- Channel: PREVIEW.

## Bronze — reglas

- Auto Loader format: `json`.
- Schema location: `/Volumes/crypto/raw/_schemas/bronze/`.
- Schema evolution: `addNewColumns`.
- Conserva el JSON crudo en columna `payload`.

## Silver — reglas

- Lee de `crypto.bronze.coin_prices_raw` como streaming.
- Extrae cada campo del payload con `selectExpr` y notación `:`.
- Aplica `dropDuplicatesWithinWatermark` con ventana 1 min en (`symbol`, `observed_at`).
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
