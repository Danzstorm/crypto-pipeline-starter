# 04 — Genie Space

## Configuración

- Title: "Crypto Insights ES"
- Idioma de respuestas: español neutro.
- Warehouse: Serverless Starter Warehouse.
- Tablas:
  - `crypto.gold.coin_momentum_24h`
  - `crypto.gold.coin_volatility_7d`
  - `crypto.silver.coin_prices`

## Knowledge snippets

| Asset | Descripción |
|---|---|
| `coin_momentum_24h` | Snapshot del último estado de cada moneda, con su ranking de momentum por cambio porcentual de 24 horas. |
| `coin_volatility_7d` | Volatilidad realizada anualizada por moneda en los últimos 7 días. |
| `pct_change_24h` | Cambio porcentual del precio en USD en las últimas 24 horas. |
| `momentum_rank` | Ranking del 1 al N donde 1 = mayor subida. |
| `realized_volatility_7d` | Volatilidad anualizada usando desviación estándar de retornos diarios × √365. |

## Sinónimos

- "subió" → `pct_change_24h > 0`
- "bajó" → `pct_change_24h < 0`
- "más volátil" → `realized_volatility_7d` mayor.
- "top" / "mejores" / "ganadoras" → `momentum_rank` bajo.

## Sample queries (Trusted)

### Q1: top 10 momentum hoy

```sql
SELECT symbol, name, pct_change_24h, momentum_rank
FROM crypto.gold.coin_momentum_24h
WHERE momentum_rank <= 10
ORDER BY momentum_rank;
```

### Q2: monedas más volátiles

```sql
SELECT symbol, realized_volatility_7d
FROM crypto.gold.coin_volatility_7d
ORDER BY realized_volatility_7d DESC
LIMIT 10;
```

### Q3: precio actual de una moneda específica

```sql
SELECT symbol, price_usd, pct_change_24h, observed_at
FROM crypto.gold.coin_momentum_24h
WHERE UPPER(symbol) = UPPER(:moneda);
```

## Instrucciones generales

- Siempre responder en español.
- Redondear porcentajes a 2 decimales en la presentación.
- Si la pregunta es ambigua, hacer una pregunta de aclaración antes de generar SQL.
