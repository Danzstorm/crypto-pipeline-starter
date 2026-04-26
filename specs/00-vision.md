# 00 — Visión

## Problema

Como analista financiero quiero entender en tiempo casi real qué criptomonedas están mostrando momentum positivo, qué tan volátiles son en la última semana, y poder hacerle preguntas en lenguaje natural a un asistente que conoce mis datos.

## Audiencia

- Analista (yo) preguntando en lenguaje natural a Genie en español.
- Equipo: vista compartida vía AI/BI Dashboard.

## KPIs en Gold

| KPI | Definición |
|-----|-----------|
| `pct_change_24h` | Variación porcentual del precio en USD respecto a hace 24h. |
| `realized_volatility_7d` | Desviación estándar de los retornos diarios en los últimos 7 días, anualizada. |
| `momentum_rank` | Ranking 1..N por `pct_change_24h` descendente. |

## SLA

- Latencia de Bronze a Gold ≤ 5 minutos después de cada captura.
- Frescura del Gold ≤ 15 minutos.
- Cobertura ≥ 100 monedas top por market cap.

## Fuera de alcance

- Trading real o ejecución de órdenes.
- Modelos predictivos (ML).
- Datos intradía a granularidad de tick.

## Decisión de stack

- **Cómputo:** Databricks Free Edition (serverless).
- **Gobernanza:** Unity Catalog.
- **Pipeline:** Lakeflow Spark Declarative Pipelines con PySpark Custom Data Source.
- **Consumo:** AI/BI Genie + Dashboard.
- **Orquestación:** Lakeflow Job cada 15 min.
- **Ingesta:** captura directa desde el pipeline a `api.coingecko.com` (validado en notebook de prueba). Sin ingester local.

## Por qué cambió la arquitectura (vs v1)

La v1 asumía que Free Edition restringía outbound a un set acotado de dominios "trusted", lo cual está documentado oficialmente. Validamos empíricamente con un notebook de prueba que `api.coingecko.com` es alcanzable desde el cómputo serverless. Eso nos permite:

- Eliminar el ingester local (no más cron en laptop).
- Eliminar el Volume `crypto.raw.coin_prices` y el schema `crypto.raw`.
- Simplificar a 3 capas (Bronze, Silver, Gold).
- Tener un proyecto 100% reproducible: clonar el repo + `databricks bundle deploy` y todo arranca.
