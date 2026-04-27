# crypto-pipeline-starter (v2.0 — self-contained)

Pipeline financiero end-to-end de criptomonedas en Databricks Free Edition. Construido con Claude Code, el toolkit `ai-dev-kit` de Databricks y arquitectura medallion declarativa.

## Qué hace este proyecto

Captura precios de las top 100 criptomonedas desde CoinGecko cada 15 minutos, los procesa en una arquitectura medallion (Bronze → Silver → Gold) y los expone vía AI/BI Genie en español y un AI/BI Dashboard.

Todo corre **dentro de Databricks**. Sin scripts locales, sin cron, sin ingesters externos.

## Arquitectura

```
┌──────────────┐    ┌────────────┐    ┌─────────┐    ┌──────┐
│ CoinGecko    │───>│ Bronze     │───>│ Silver  │───>│ Gold │
│ (API)        │    │ (Custom DS)│    │         │    │      │
└──────────────┘    └────────────┘    └─────────┘    └──────┘
                          ↑
                   Job de Databricks dispara cada 15 min
```

- **Bronze:** streaming table que llama a CoinGecko via PySpark Custom Data Source. Append-only, JSON crudo.
- **Silver:** tipado, deduplicado por `(symbol, snapshot_id)`, expectations de calidad.
- **Gold:** vistas materializadas con KPIs (`coin_momentum_24h`, `coin_volatility_7d`).

## Qué incluye este starter

- **6 specs en `specs/`** — la fuente de verdad del proyecto.
- **Código del pipeline en `src/pipelines/`** — Bronze (con Custom Data Source), Silver, Gold.
- **Bundle declarativo en `resources/`** — todos los recursos de Databricks como YAML.
- **`CLAUDE.md`** — instrucciones para Claude Code.
- **`databricks.yml`** — punto de entrada del bundle.
- **`tests/integration/test_coingecko_databricks.py`** — notebook de prueba para validar conectividad a CoinGecko.

## Empezar

Sigue la guía de implementación paso a paso (`docs/guia_implementacion.md`). Los pasos clave:

1. Crear cuenta Databricks Free Edition + PAT.
2. Instalar Databricks CLI `0.278.0+`, `uv`, Claude Code.
3. `git clone` este repo y entrar a la carpeta `crypto-pipeline-starter`.
4. Editar `databricks.yml` y poner la URL real del workspace en `targets.dev.workspace.host`.
5. Instalar el `ai-dev-kit` **desde dentro de la carpeta** con `irm https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.ps1 | iex` (Windows) o el equivalente bash (macOS/Linux).
6. Configurar perfil CLI `crypto` con `databricks configure --profile crypto --token`.
7. (Recomendado) Correr `tests/integration/test_coingecko_databricks.py` en un notebook para validar conectividad a CoinGecko.
8. Desde Claude Code, ejecutar `CREATE CATALOG IF NOT EXISTS crypto;` vía MCP.
9. `databricks bundle validate --profile crypto`.
10. `databricks bundle deploy --target dev --profile crypto`.
11. `databricks bundle run crypto_medallion --target dev --profile crypto`.
12. Crear Genie Space y Dashboard vía Claude Code.

## Estructura

```
crypto-pipeline-starter/
├── CLAUDE.md                  # Instrucciones para Claude Code
├── README.md                  # Este archivo
├── databricks.yml             # Bundle root
├── .gitignore
├── specs/                     # 6 specs (fuente de verdad)
│   ├── 00-vision.md
│   ├── 01-data-contract.md
│   ├── 02-ingestion.md
│   ├── 03-medallion.md
│   ├── 04-genie.md
│   └── 05-runbook.md
├── src/
│   └── pipelines/             # Corre en Databricks via SDP
│       ├── 01_bronze.py       # Custom Data Source para CoinGecko
│       ├── 02_silver.py
│       └── 03_gold.py
├── resources/                 # Bundle declarativo
│   ├── schemas/
│   │   └── crypto_schemas.yml  # Catálogo creado vía SQL, no en el bundle
│   ├── pipelines/
│   │   └── crypto_medallion.yml
│   ├── jobs/
│   │   └── crypto_orchestrator.yml
│   ├── dashboards/            # (lo creas via Claude Code)
│   └── genie_spaces/          # (lo creas via Claude Code)
└── tests/
    ├── unit/
    │   └── test_transforms.py
    └── integration/
        └── test_coingecko_databricks.py
```

## Replica en otro workspace Free Edition

```powershell
# 1. Crear segunda cuenta + segundo PAT
# 2. Configurar segundo perfil CLI
databricks configure --profile crypto2 --token

# 3. Clonar
git clone https://github.com/danzstorm/crypto-pipeline-starter crypto-pipeline-replica
cd crypto-pipeline-replica

# 4. Editar databricks.yml: poner la URL del segundo workspace en targets.dev.workspace.host

# 5. Instalar ai-dev-kit (desde dentro de la carpeta) eligiendo perfil crypto2
irm https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.ps1 | iex

# 6. Crear el catálogo en el segundo workspace (desde Claude Code vía MCP)
# CREATE CATALOG IF NOT EXISTS crypto;

# 7. Desplegar
databricks bundle validate --profile crypto2
databricks bundle deploy --target dev --profile crypto2

# 8. Disparar
databricks bundle run crypto_medallion --target dev --profile crypto2
```

El bundle reconstruye catálogo, schemas, pipeline y job idénticos. La captura empieza automáticamente.

## Versiones

- **v1.0** — Patrón con ingester local + Volume `raw`. Asumía outbound restringido.
- **v2.0 (actual)** — Captura directa desde el pipeline via Custom Data Source. Validado empíricamente que CoinGecko es alcanzable desde Free Edition serverless.

## Licencia

MIT.
