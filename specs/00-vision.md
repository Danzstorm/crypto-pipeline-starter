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

- Latencia de Bronze a Gold ≤ 5 minutos después de cada ingesta.
- Frescura del Gold ≤ 15 minutos.
- Cobertura ≥ 100 monedas top por market cap.

## Fuera de alcance

- Trading real o ejecución de órdenes.
- Modelos predictivos (ML).
- Datos intradía a granularidad de tick.

## Decisión de stack

- **Cómputo:** Databricks Free Edition (serverless).
- **Gobernanza:** Unity Catalog.
- **Pipeline:** Lakeflow Spark Declarative Pipelines.
- **Consumo:** AI/BI Genie + Dashboard.
- **Orquestación:** Lakeflow Jobs cada 15 min.
- **Ingesta:** script Python local que sube JSONL al Volume vía Files API (workaround para outbound restringido en Free Edition).
