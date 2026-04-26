# CLAUDE.md — Instrucciones para Claude Code en este proyecto

Este archivo lo lee Claude Code automáticamente al iniciarse en esta carpeta. Define cómo trabajar en el proyecto. Si trabajas con otro cliente (Cursor, Antigravity, GitHub Copilot Chat), revisa la sección final de equivalencias.

## Qué es este proyecto

Pipeline financiero end-to-end de precios de criptomonedas en Databricks Free Edition:

- **Ingesta:** script Python local que captura precios de CoinGecko cada 10 min y los sube a un Volume de Unity Catalog vía Files API.
- **Procesamiento:** Lakeflow Spark Declarative Pipelines (SDP) con arquitectura medallion: Bronze (JSON crudo) → Silver (tipado, deduplicado) → Gold (vistas materializadas con KPIs).
- **Consumo:** AI/BI Genie Space en español + AI/BI Dashboard.
- **Orquestación:** Lakeflow Job que dispara el pipeline cada 15 minutos.
- **Gobernanza:** Unity Catalog (catálogo `crypto`).
- **Infraestructura como código:** Databricks Asset Bundle (DAB) en `resources/` y `databricks.yml`.

## Idioma y tono

- Responde **siempre en español neutro** (sin localismos regionales). Usa "tú" en segunda persona.
- En los snippets de código, mantén comentarios en español también.
- En los nombres de variables, columnas y archivos, usa inglés `snake_case`.
- Los mensajes de log, igualmente en inglés (`Fetched`, `Uploaded`, etc.) — convención técnica estándar.

## Workflow obligatorio: spec-driven development

Antes de implementar cualquier cosa, lee los specs. La fuente de verdad del proyecto está en estos seis archivos, en este orden:

1. `specs/00-vision.md` — qué resolvemos, KPIs, SLAs.
2. `specs/01-data-contract.md` — schemas Bronze/Silver/Gold.
3. `specs/02-ingestion.md` — fuente, cadencia, paths.
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
- `databricks-spark-declarative-pipelines` — patrones SDP, AUTO CDC, Auto Loader
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
| `upload_file`, `upload_folder` | NO usar para data ingestada — eso es responsabilidad del ingester local. Solo para artefactos de configuración. |
| `execute_databricks_command` | Para ejecutar `databricks bundle validate/deploy/run` desde dentro de Claude Code. |
| `list_tracked_resources`, `delete_tracked_resource` | Para limpiar recursos huérfanos al final del proyecto o entre experimentos. |

Reglas duras del MCP:

- **Nunca** ejecutes `DROP CATALOG`, `DROP SCHEMA CASCADE`, `DELETE`, `TRUNCATE`, ni `bundle destroy` sin aprobación explícita en chat.
- **Nunca** modifiques recursos directamente con SQL si están declarados en el bundle. Cambia el YAML y haz `bundle deploy`.
- Si una operación afecta más de 10 filas o crea más de 1 recurso, muestra el plan antes de ejecutar.

## Convenciones del proyecto

### Naming

- Catálogo único: `crypto`.
- Schemas: `raw`, `bronze`, `silver`, `gold`.
- Tablas/vistas en `snake_case`: `coin_prices_raw`, `coin_momentum_24h`.
- Recursos del bundle: usa el sufijo `${bundle.target}` (ej. `crypto_medallion_dev`).

### Estructura de archivos

- Specs en `specs/`. Versionados, fuente de verdad.
- Código del pipeline en `src/pipelines/`. Numerado por capa (`01_bronze.py`, `02_silver.py`, `03_gold.py`).
- Ingester en `src/ingestion/`. Corre LOCAL, nunca en Databricks.
- Recursos del bundle en `resources/<tipo>/`. Un YAML por recurso.
- `databricks.yml` solo en la raíz, con `include:` apuntando a `resources/`.

### Workflow Git

- Después de cada paso significativo, sugiere un commit con mensaje en formato Conventional Commits:
  - `feat: ...` para nuevas funcionalidades.
  - `fix: ...` para correcciones.
  - `docs: ...` para specs y documentación.
  - `chore: ...` para configuración.
- No uses GitHub a menos que el usuario lo pida explícitamente.

### Targets del bundle

- `dev` (default): pipeline en modo development, jobs pausados.
- No hay `prod` por ahora; cuando se cree, se debe usar otro perfil CLI con un PAT de service principal.

## Restricciones de Free Edition que NO debes intentar saltar

- **Outbound limitado:** no escribas código que llame APIs externas desde el cómputo de Databricks. La ingesta es siempre local.
- **1 pipeline activo por tipo:** no propongas múltiples pipelines paralelos.
- **1 SQL warehouse 2X-Small:** no propongas crear warehouses adicionales.
- **5 tareas de job concurrentes:** no propongas DAGs masivos.
- **Sin Knowledge Assistants ni Supervisor Agents:** si la solución sugiere agentes multi-paso encima de los datos, marca el límite y propone Genie Space como alternativa.

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

**Versión:** 1.0
**Última actualización:** abril 2026
**Mantenedor:** [tu nombre]
