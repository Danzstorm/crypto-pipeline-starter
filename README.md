# crypto-pipeline-starter

Pipeline financiero end-to-end de criptomonedas en Databricks Free Edition, construido vibe-coding desde la terminal con Claude Code y el toolkit oficial `ai-dev-kit` de Databricks.

## Qué incluye este starter

- **6 specs en `specs/`** — la fuente de verdad del proyecto.
- **Código del pipeline en `src/pipelines/`** — Bronze, Silver, Gold con SDP.
- **Ingester local en `src/ingestion/`** — script Python que corre en tu laptop.
- **Bundle declarativo en `resources/`** — todos los recursos de Databricks como YAML.
- **`CLAUDE.md`** — instrucciones para Claude Code.
- **`databricks.yml`** — punto de entrada del bundle.

## Empezar

Sigue la guía de implementación paso a paso (archivo `guia.md` que viene aparte). Los pasos clave:

1. Crear cuenta Databricks Free Edition + PAT.
2. Instalar Databricks CLI, `uv`, Claude Code.
3. Instalar el `ai-dev-kit` desde dentro de esta carpeta.
4. Configurar perfil CLI `crypto`.
5. `databricks bundle deploy --target dev --profile crypto`.
6. Correr el ingester local.
7. Disparar el pipeline.
8. Crear Genie Space y Dashboard vía Claude Code.

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
│   ├── ingestion/             # Corre LOCAL en tu laptop
│   │   ├── ingest_crypto.py
│   │   └── requirements.txt
│   ├── pipelines/             # Corre en Databricks vía SDP
│   │   ├── 01_bronze.py
│   │   ├── 02_silver.py
│   │   └── 03_gold.py
│   └── exploration/           # Notebooks de debug (opcional)
├── resources/                 # Bundle declarativo
│   ├── schemas/
│   │   ├── crypto_catalog.yml
│   │   └── crypto_schemas.yml
│   ├── volumes/
│   │   └── coin_prices.yml
│   ├── pipelines/
│   │   └── crypto_medallion.yml
│   ├── jobs/
│   │   └── crypto_orchestrator.yml
│   ├── dashboards/            # (lo creas vía Claude Code)
│   └── genie_spaces/          # (lo creas vía Claude Code)
└── tests/
    ├── unit/
    └── integration/
```

## Réplica en otro workspace Free Edition

Para clonar el proyecto a un segundo workspace (por ejemplo, otra cuenta Free):

1. Crear segunda cuenta Databricks Free Edition + segundo PAT.
2. `databricks configure --profile crypto2 --token`
3. Copiar esta carpeta a `crypto-pipeline-replica/`.
4. Editar `.mcp.json` para que el MCP apunte al perfil `crypto2`.
5. `databricks bundle deploy --target dev --profile crypto2`.
6. Correr el ingester con `DATABRICKS_CONFIG_PROFILE=crypto2`.

El bundle reconstruye catálogo, schemas, volume, pipeline y job idénticos.
