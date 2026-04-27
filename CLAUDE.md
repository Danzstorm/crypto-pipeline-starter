# CLAUDE.md — Instrucciones para Claude Code en este proyecto

Este archivo lo lee Claude Code automáticamente al iniciarse en esta carpeta. Define cómo trabajar en el proyecto. Si trabajas con otro cliente (Cursor, Antigravity, GitHub Copilot Chat), revisa la sección final de equivalencias.

## Qué es este proyecto

Pipeline financiero end-to-end de precios de criptomonedas en Databricks Free Edition, **100% self-contained**:

- **Ingesta:** capturada directamente desde el pipeline via PySpark Custom Data Source que llama a CoinGecko. Sin ingester local, sin cron en laptop.
- **Procesamiento:** Lakeflow Spark Declarative Pipelines (SDP) con arquitectura medallion: Bronze (snapshots crudos) → Silver (tipado, deduplicado) → Gold (vistas materializadas con KPIs).
- **Consumo:** AI/BI Genie Space en español + AI/BI Dashboard.
- **Orquestación:** Lakeflow Job que dispara el pipeline cada 15 minutos.
- **Gobernanza:** Unity Catalog (catálogo `crypto`).
- **Infraestructura como código:** Databricks Asset Bundle (DAB) en `resources/` y `databricks.yml`.

> **Validación previa de la arquitectura:** el notebook `tests/integration/test_coingecko_databricks.py` confirma que `api.coingecko.com` es alcanzable desde el cómputo serverless de Free Edition. Si en algún workspace ese test falla, considerar revertir a la v1 con ingester local.

## Idioma y tono

- Responde **siempre en español neutro** (sin localismos regionales). Usa "tú" en segunda persona.
- En los snippets de código, mantén comentarios en español también.
- En los nombres de variables, columnas y archivos, usa inglés `snake_case`.
- Los mensajes de log, igualmente en inglés (`Fetched`, `Processed`, etc.) — convención técnica estándar.

## Workflow obligatorio: spec-driven development

Antes de implementar cualquier cosa, lee los specs. La fuente de verdad del proyecto está en estos seis archivos, en este orden:

1. `specs/00-vision.md` — qué resolvemos, KPIs, SLAs.
2. `specs/01-data-contract.md` — schemas Bronze/Silver/Gold.
3. `specs/02-ingestion.md` — fuente, cadencia, patrón Custom Data Source.
4. `specs/03-medallion.md` — reglas SDP, expectations.
5. `specs/04-genie.md` — knowledge snippets y sample queries del Genie Space.
6. `specs/05-runbook.md` — operación diaria y troubleshooting.

Cuando el usuario te pida implementar algo:

1. Lee primero el spec relevante.
2. Lee también las skills aplicables en `.claude/skills/` (instaladas por el ai-dev-kit).
3. Propone un plan ANTES de escribir código. Resume qué archivos vas a tocar y por qué.
4. Espera la aprobación del usuario.
5. Después implementa, mostrando el diff.
6. Para acciones que tocan el workspace (deploy, ejecutar SQL, crear recursos), pide confirmación explícita antes de llamar al MCP.

## Skills disponibles en este proyecto

Las skills del `ai-dev-kit` están en `.claude/skills/`. Para este pipeline, las más relevantes son:

- `databricks-config` (siempre cargada)
- `databricks-docs` (siempre cargada)
- `databricks-unity-catalog`
- `databricks-spark-declarative-pipelines` — patrones SDP, AUTO CDC, Custom Data Sources
- `databricks-asset-bundles` — DAB
- `databricks-jobs` — workflows
- `databricks-genie` — Genie Spaces vía Conversation API
- `databricks-python-sdk` — SDK + CLI + REST

Si te preguntan algo que no está cubierto en los specs ni en las skills, NO INVENTES. Pídeme la información que falta.

## MCP — qué herramientas usar y cuándo

El MCP server de Databricks corre localmente y expone ~75 herramientas. Las que usamos más en este proyecto:

| Tool | Cuándo usarlo |
|---|---|
| `execute_sql` | Inspeccionar tablas, debug, queries ad-hoc. Para SELECT/SHOW. |
| `list_warehouses`, `get_best_warehouse` | Antes de crear Genie Spaces o dashboards. |
| `get_table_details` | Verificar esquemas reales antes de modificar specs. |
| `execute_databricks_command` | Para ejecutar `databricks bundle validate/deploy/run` desde dentro de Claude Code. |
| `list_tracked_resources`, `delete_tracked_resource` | Para limpiar recursos huérfanos al final del proyecto o entre experimentos. |

Reglas duras del MCP:

- **Nunca** ejecutes `DROP CATALOG`, `DROP SCHEMA CASCADE`, `DELETE`, `TRUNCATE`, ni `bundle destroy` sin aprobación explícita en chat.
- **Nunca** modifiques recursos directamente con SQL si están declarados en el bundle. Cambia el YAML y haz `bundle deploy`.
- Si una operación afecta más de 10 filas o crea más de 1 recurso, muestra el plan antes de ejecutar.

## Convenciones del proyecto

### Naming

- Catálogo único: `crypto`. **El catálogo se crea una sola vez vía SQL (`CREATE CATALOG IF NOT EXISTS crypto;`) y no se declara en el bundle** — los recursos `catalog` en DABs requieren motor especial y complican el setup.
- Schemas: `bronze`, `silver`, `gold` (sin `raw` — eliminado en v2).
- Tablas/vistas en `snake_case`: `coin_prices_raw`, `coin_momentum_24h`.
- Recursos del bundle: usa el sufijo `${bundle.target}` (ej. `crypto_medallion_dev`).

### Estructura de archivos

- Specs en `specs/`. Versionados, fuente de verdad.
- Código del pipeline en `src/pipelines/`. Numerado por capa (`01_bronze.py`, `02_silver.py`, `03_gold.py`).
- Bronze contiene la clase `CoinGeckoDataSource` que implementa la captura.
- Recursos del bundle en `resources/<tipo>/`. Un YAML por recurso. **No existe `crypto_catalog.yml`** — el catálogo se gestiona fuera del bundle.
- `databricks.yml` en la raíz, con `include:` apuntando a `resources/`. El campo `targets.dev.workspace.host` tiene la URL real del workspace (no una variable dinámica).
- Tests en `tests/`. Unitarios en `tests/unit/`, de integración (incluye notebook de validación de CoinGecko) en `tests/integration/`.

### Workflow Git

- Después de cada paso significativo, sugiere un commit con mensaje en formato Conventional Commits:
  - `feat: ...` para nuevas funcionalidades.
  - `fix: ...` para correcciones.
  - `docs: ...` para specs y documentación.
  - `chore: ...` para configuración.
- Para cambios mayores, sugiere usar branch + PR. Para cambios menores, push directo a `main` está bien.

### Targets del bundle

- `dev` (default): pipeline en modo development, jobs pausados.
- No hay `prod` por ahora; cuando se cree, se debe usar otro perfil CLI con un PAT de service principal.

## Restricciones de Free Edition que NO debes intentar saltar

- **Outbound limitado:** Free Edition restringe el outbound serverless. Validamos en `tests/integration/test_coingecko_databricks.py` que CoinGecko sí es alcanzable. Para cualquier OTRO API externo, sugiere correr un test similar antes de asumir conectividad. Si una API no es alcanzable, vuelve al patrón "ingester local + Volume" de la v1.
- **1 pipeline activo por tipo:** no propongas múltiples pipelines paralelos.
- **1 SQL warehouse 2X-Small:** no propongas crear warehouses adicionales.
- **5 tareas de job concurrentes:** no propongas DAGs masivos.
- **Sin Knowledge Assistants ni Supervisor Agents:** si la solución sugiere agentes multi-paso encima de los datos, marca el límite y propone Genie Space como alternativa.

## El patrón Custom Data Source (importante)

El archivo `src/pipelines/01_bronze.py` implementa una clase `CoinGeckoDataSource` que extiende `pyspark.sql.datasource.DataSource`. Esto es lo que reemplaza al ingester local.

Si el usuario te pide cambios en la captura (otra API, otros parámetros, schema diferente), modifica esa clase. Mantén siempre estos cuatro métodos:

- `name()` — identificador único del data source (ej. `"coingecko"`).
- `schema()` — define columnas y tipos.
- `reader()` — devuelve la instancia del Reader.
- `Reader.read()` — generador que produce las filas.

Y siempre registra el data source con `spark.dataSource.register(CoinGeckoDataSource)` antes de declarar la `@dp.table`.

## Cómo presentar resultados

- Para queries: muestra el SQL, ejecútalo vía MCP, y resume el resultado en máximo 5 líneas. Si hay muchas filas, devuelve solo top 10.
- Para deploys: muestra el comando exacto que vas a ejecutar antes de correrlo.
- Para errores: muestra el error literal y propone máximo 3 hipótesis con la siguiente acción concreta.

## Equivalencias para otros clientes

- **Cursor:** este archivo se llama `.cursorrules`. El instalador del `ai-dev-kit` debe haberlo creado también.
- **GitHub Copilot Chat:** revisa `.github/copilot-instructions.md`.
- **Antigravity / Codex:** revisa `.codex/instructions.md`.

Si trabajas en uno de esos, copia el contenido de este archivo al equivalente.

---

**Versión:** 2.0
**Última actualización:** abril 2026
**Mantenedor:** Daniel Santos
