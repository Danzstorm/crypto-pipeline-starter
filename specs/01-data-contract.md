# 01 — Data contract

## Capa Bronze — `crypto.bronze.coin_prices_raw`

Append-only, payload sin tipar.

| Columna | Tipo | Descripción |
|---|---|---|
| `_ingestion_ts` | TIMESTAMP | Timestamp UTC de cuando el ingester local capturó el dato. |
| `_source_file` | STRING | Path en el Volume del archivo origen. |
| `payload` | STRING | JSON crudo con la fila de la API. |
| `_rescued_data` | STRING | Auto Loader: campos no esperados. |

**Expectations:**

- `non_empty_payload`: `payload IS NOT NULL AND length(payload) > 10` → drop.

## Capa Silver — `crypto.silver.coin_prices`

Tipado, deduplicado (ventana 1 minuto), validado.

| Columna | Tipo | Descripción |
|---|---|---|
| `symbol` | STRING NOT NULL | Símbolo de la moneda (ej. BTC). |
| `name` | STRING | Nombre completo (ej. Bitcoin). |
| `price_usd` | DECIMAL(18, 8) | Precio actual en USD. |
| `volume_24h_usd` | DECIMAL(20, 2) | Volumen 24h en USD. |
| `market_cap_usd` | DECIMAL(20, 2) | Capitalización en USD. |
| `pct_change_1h` | DOUBLE | Cambio % última hora. |
| `pct_change_24h` | DOUBLE | Cambio % últimas 24h. |
| `pct_change_7d` | DOUBLE | Cambio % últimos 7 días. |
| `observed_at` | TIMESTAMP NOT NULL | Timestamp de observación. |

**Clustering:** `CLUSTER BY (symbol)` (Liquid Clustering).

**Expectations:**

- `price_positive`: `price_usd > 0` → warn.
- `symbol_not_null`: `symbol IS NOT NULL` → drop.
- `valid_market_cap`: `market_cap_usd >= 0` → warn.

## Capa Gold

### `crypto.gold.coin_momentum_24h` (vista materializada)

| Columna | Tipo |
|---|---|
| `symbol` | STRING |
| `name` | STRING |
| `price_usd` | DECIMAL(18, 8) |
| `pct_change_24h` | DOUBLE |
| `volume_24h_usd` | DECIMAL(20, 2) |
| `market_cap_usd` | DECIMAL(20, 2) |
| `momentum_rank` | INT |
| `observed_at` | TIMESTAMP |

### `crypto.gold.coin_volatility_7d` (vista materializada)

| Columna | Tipo |
|---|---|
| `symbol` | STRING |
| `realized_volatility_7d` | DOUBLE |
| `n_observations` | LONG |
| `last_observed_at` | DATE |

## Convenciones

- Todas las fechas en UTC.
- Decimales en presentación: `pct_change_*` redondeados a 4 decimales.
- Naming: `snake_case` para columnas, `lowercase` para catálogos/schemas/tablas.
