# Guía de implementación — Pipeline crypto end-to-end en Databricks Free Edition

Esta guía te lleva paso a paso desde cero hasta tener un pipeline financiero completo de criptomonedas corriendo en Databricks Free Edition. Asume que descargaste el `crypto-pipeline-starter` (zip o `git clone`) y todos los archivos de código, specs y bundle ya están escritos.

> **Importante:** este documento NO contiene fragmentos de código embebidos para copiar al pipeline. La fuente de verdad de cada archivo del pipeline es el archivo mismo en el starter. Si modificas algo, modifícalo en el archivo, no en esta guía.

## Tabla de contenidos

- [Arquitectura del proyecto](#arquitectura-del-proyecto)
- [Conceptos clave antes de empezar](#conceptos-clave-antes-de-empezar)
- [Pre-requisitos](#pre-requisitos)
- [Parte A — Setup (una sola vez)](#parte-a--setup-una-sola-vez)
- [Parte B — Bootstrap del proyecto](#parte-b--bootstrap-del-proyecto)
- [Parte C — Validar conectividad a CoinGecko](#parte-c--validar-conectividad-a-coingecko)
- [Parte D — Despliegue de infraestructura y pipeline](#parte-d--despliegue-de-infraestructura-y-pipeline)
- [Parte E — Verificar las tablas Bronze, Silver, Gold](#parte-e--verificar-las-tablas-bronze-silver-gold)
- [Parte F — Capa de consumo (Genie + Dashboard)](#parte-f--capa-de-consumo-genie--dashboard)
- [Parte G — Activar el Job para producción continua](#parte-g--activar-el-job-para-producción-continua)
- [Réplica en otro workspace Free Edition](#réplica-en-otro-workspace-free-edition)
- [Cómo usar Claude Code en este proyecto](#cómo-usar-claude-code-en-este-proyecto)
- [Troubleshooting](#troubleshooting)

---

## Arquitectura del proyecto

```
┌──────────────┐    ┌────────────┐    ┌─────────┐    ┌──────┐
│ CoinGecko    │───>│ Bronze     │───>│ Silver  │───>│ Gold │
│ (API)        │    │ (Custom DS)│    │         │    │      │
└──────────────┘    └────────────┘    └─────────┘    └──────┘
                          ↑
                   Job de Databricks dispara cada 15 min
```

Tres capas, todas dentro de Databricks:

- **Bronze:** streaming table que llama a CoinGecko via PySpark Custom Data Source. Append-only, JSON crudo con `snapshot_id`.
- **Silver:** tipado, deduplicado por `(symbol, snapshot_id)`, expectations de calidad.
- **Gold:** vistas materializadas con KPIs (`coin_momentum_24h`, `coin_volatility_7d`).

Todo orquestado por un Job de Databricks que dispara el pipeline cada 15 minutos. Sin scripts locales, sin cron en laptop, sin ingester externo.

---

## Conceptos clave antes de empezar

### ¿Qué es un Databricks Asset Bundle (DAB)?

Un Bundle es un conjunto de archivos YAML que describen recursos de Databricks como código. Cuando ejecutas `databricks bundle deploy`, la CLI crea o actualiza esos recursos en tu workspace. Es Infrastructure-as-Code, equivalente a Terraform pero nativo de Databricks.

En este proyecto, el bundle declara: el catálogo, los schemas, el pipeline SDP y el job orquestador. Cuando hagas `bundle deploy`, todos esos recursos se crean en orden correcto.

### ¿Qué es un PySpark Custom Data Source?

Es una clase Python que extiende `pyspark.sql.datasource.DataSource` y le enseña a Spark cómo leer de una fuente externa. SDP la trata como cualquier otra streaming source.

En este proyecto, `CoinGeckoDataSource` (en `src/pipelines/01_bronze.py`) implementa la captura: cada vez que el pipeline corre, esta clase llama a la API de CoinGecko, recibe los precios de las 100 monedas top, y los emite como filas con un `snapshot_id` (timestamp UTC común para todas las filas de esa captura).

### ¿Qué es el ai-dev-kit y para qué sirve?

Es un instalador de Databricks que configura Claude Code con dos cosas:
- `.claude/skills/` — documentación técnica de Databricks que Claude Code usa como contexto al responder.
- `.mcp.json` — configuración del MCP server local que permite a Claude Code ejecutar acciones en tu workspace (queries SQL, deploy de bundles, gestión de recursos).

**Importante:** el ai-dev-kit debe instalarse desde dentro de la carpeta del proyecto, no desde otra carpeta. Si se instala en el lugar equivocado, Claude Code no carga las skills ni el MCP.

### ¿Qué es el perfil CLI `crypto`?

Es un nombre que le damos a la conexión entre la CLI y tu workspace. El perfil agrupa: URL del workspace + PAT. Puedes llamarlo como quieras; `crypto` es la convención de este proyecto para que todos los comandos sean consistentes.

### ¿Necesitas GitHub?

No estrictamente. Necesitas Git local para versionar tus cambios. GitHub es opcional pero muy recomendable si vas a distribuir el proyecto a alumnos del bootcamp.

### ¿Por qué validamos primero CoinGecko?

Free Edition restringe el outbound del cómputo serverless a un set de dominios. Si CoinGecko no es alcanzable desde tu workspace específico, el pipeline falla en Bronze. Por eso la Parte C corre un notebook de prueba antes del primer deploy: si pasa, sigues. Si falla, hay que cambiar de arquitectura.

---

## Pre-requisitos

- Computadora con Windows, macOS, o Linux.
- Email para registrarte en Databricks Free Edition.
- Cuenta Anthropic con créditos para Claude Code.
- Vas a instalar: Databricks CLI, `uv`, Claude Code, Git.

---

## Parte A — Setup (una sola vez)

### A.1 Crear cuenta Databricks Free Edition

1. Ve a https://signup.databricks.com → Express Setup.
2. Verifica el email.
3. **Anota la URL de tu workspace** (ej. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`). La necesitarás en varios pasos.

### A.2 Generar Personal Access Token (PAT)

1. En tu workspace: avatar (arriba derecha) → **Settings** → **Developer** → **Access tokens** → **Manage** → **Generate new token**.
2. Comment: `crypto-pipeline-laptop`. Lifetime: `90` días.
3. **Copia el token y guárdalo ahora.** No se vuelve a mostrar.

### A.3 Instalar Databricks CLI

**Windows (PowerShell):**
```powershell
winget install Databricks.DatabricksCLI
```

**macOS / Linux:**
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Verifica: `databricks --version` debe mostrar `0.278.0` o superior. Si es más vieja, actualiza con el mismo comando de instalación.

### A.4 Instalar `uv`

`uv` es el gestor de entornos Python que usa el ai-dev-kit internamente.

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verifica en una terminal nueva: `uv --version`.

### A.5 Instalar Claude Code

Sigue las instrucciones en https://claude.ai/code. Puedes usar la extensión de VS Code (recomendada para Windows) o el CLI.

Verifica que puedes abrir Claude Code y chatear con él.

### A.6 Configurar perfil CLI

```powershell
databricks configure --profile crypto --token
```

Cuando pregunte:
- **Host:** la URL del workspace que anotaste en A.1.
- **Token:** el PAT del paso A.2.

Verifica:
```powershell
databricks current-user me --profile crypto
```

Debe devolver tu email. Si lo hace, el perfil está bien configurado.

---

## Parte B — Bootstrap del proyecto

### B.1 Obtener el starter

**Opción 1 (recomendada): clone desde GitHub**
```powershell
git clone https://github.com/<tu-usuario>/crypto-pipeline-starter
```

**Opción 2: descomprimir el zip**
```powershell
Expand-Archive -Path "$env:USERPROFILE\Downloads\crypto-pipeline-starter.zip" -DestinationPath .
```

Entra a la carpeta del proyecto:
```powershell
cd crypto-pipeline-starter
```

**A partir de aquí, todos los comandos de esta guía asumen que estás dentro de `crypto-pipeline-starter`.**

### B.2 Configurar el host del workspace en el bundle

Abre `databricks.yml` y reemplaza el valor del `host` con la URL real de tu workspace:

```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dbc-xxxxxxxx-xxxx.cloud.databricks.com   # ← tu URL aquí
```

Guarda el archivo. Este paso es necesario para que `databricks bundle validate` pueda conectarse al workspace correcto.

### B.3 Instalar el ai-dev-kit

> **Requisito previo:** este comando debe ejecutarse desde dentro de la carpeta `crypto-pipeline-starter`. Si lo corres desde otra carpeta, el instalador crea los archivos en el lugar equivocado y Claude Code no los carga.

**Windows (PowerShell):**
```powershell
irm https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.ps1 | iex
```

**macOS / Linux:**
```bash
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh) --profile crypto
```

En el instalador interactivo elige:
- **Tool:** Claude Code
- **Scope:** Project
- **Skills profile:** Data Engineer (o All)
- **Profile:** `crypto`
- Confirma con `y`

El instalador crea dentro de `crypto-pipeline-starter`:
- `.claude/skills/` — skills de Databricks para Claude Code.
- `.mcp.json` — configuración del MCP server apuntando al perfil `crypto`.

Verifica que los archivos existen:
```powershell
# Windows
Test-Path .mcp.json
Test-Path .claude/skills

# macOS / Linux
ls -la .mcp.json .claude/skills
```

### B.4 Verificar el MCP desde Claude Code

Abre Claude Code en la carpeta del proyecto. En el chat, escribe:

```
/mcp
```

Debe aparecer el servidor `databricks` como conectado. Luego prueba:

```
Lista mis SQL warehouses usando el MCP de Databricks.
```

Si Claude Code responde con la lista de warehouses, el MCP está funcionando correctamente.

> Nota: `/mcp` es un comando de Claude Code que se escribe en el chat, no en la terminal de PowerShell.

### B.5 Commit del setup (si es repo Git)

```powershell
git add .
git commit -m "feat: install ai-dev-kit and configure workspace host"
git push
```

---

## Parte C — Validar conectividad a CoinGecko

> Esta parte es crítica. No la saltes. Si CoinGecko no es alcanzable desde tu workspace específico, el pipeline fallará en Bronze.

### C.1 Subir el notebook de validación a Databricks

1. UI → Workspace → tu carpeta personal.
2. Click derecho → **Import**.
3. Selecciona `tests/integration/test_coingecko_databricks.py` desde tu carpeta local.
4. **Import format:** Source.

### C.2 Ejecutar el notebook

1. Abre el notebook recién importado.
2. **Connect** → **Serverless**.
3. **Run all**.

El notebook ejecuta cuatro tests:

- **Test 1:** resolución DNS de `api.coingecko.com`.
- **Test 2:** llamada HTTP simple al endpoint `/ping`.
- **Test 3:** llamada con datos reales (10 monedas).
- **Test 4:** control con `pypi.org` (debe pasar siempre).

### C.3 Interpretar los resultados

**Si los 4 tests pasan:** sigue con la Parte D.

**Si Test 2 o Test 3 falla con TIMEOUT:** Free Edition bloquea outbound a CoinGecko en tu workspace. Opciones:

1. **Probar otro API:** considera CoinCap (`api.coincap.io`) o Binance (`api.binance.com`). Si alguno funciona, modifica `src/pipelines/01_bronze.py`.
2. **Volver a v1:** la versión anterior con ingester local funciona en cualquier caso. Disponible en el repo bajo el tag `v1.0`.

**Si Test 4 (PyPI) también falla:** reinicia el cluster serverless y vuelve a probar.

---

## Parte D — Despliegue de infraestructura y pipeline

El bundle gestiona schemas, pipeline y job. El catálogo se crea una sola vez vía SQL antes del primer deploy — los recursos de tipo `catalog` en DABs requieren un motor especial que complica el setup, así que seguimos el patrón recomendado: crear el catálogo primero y dejarlo fuera del bundle.

### D.1 Crear el catálogo (una sola vez)

Desde Claude Code, escribe en el chat:

```
Ejecuta este SQL en el warehouse:
CREATE CATALOG IF NOT EXISTS crypto;
```

Claude lo ejecutará vía MCP. Solo necesitas hacerlo una vez por workspace.

### D.2 Validar el bundle

```powershell
databricks bundle validate --profile crypto
```

Salida esperada: `Validation OK!` sin errores.

### D.3 Desplegar

```powershell
databricks bundle deploy --target dev --profile crypto
```

Esto crea, en orden:
- Schemas `crypto.bronze`, `crypto.silver`, `crypto.gold`
- Pipeline `crypto_medallion_dev`
- Job `crypto_orchestrator_dev` (pausado por defecto)

Salida esperada (resumida):
```
Applying changes...
Deployment complete!
```

### D.4 Verificar en la UI

1. UI → Catalog → `crypto` con `bronze`, `silver`, `gold`.
2. UI → Workflows → Pipelines → `crypto_medallion_dev`.
3. UI → Workflows → Jobs → `crypto_orchestrator_dev` (Status: PAUSED).

### D.4 Disparar el pipeline manualmente

```powershell
databricks bundle run crypto_medallion --target dev --profile crypto
```

O desde Claude Code:
```
Usando el MCP, dispara una update en el pipeline crypto_medallion_dev y monitoréalo
cada 30 segundos hasta que termine. Reporta cuántas filas se procesaron en cada capa.
```

La primera corrida tarda 2-4 minutos:
- Aprovisiona el cluster serverless.
- `CoinGeckoDataSource` llama a CoinGecko.
- Bronze recibe 100 filas (una por moneda).
- Silver tipa y deduplica.
- Gold materializa los KPIs.

---

## Parte E — Verificar las tablas Bronze, Silver, Gold

Desde Claude Code:

```
Ejecuta estas queries y muestrame los resultados:

1. SELECT COUNT(*) AS bronze_rows, MAX(snapshot_id) AS last_capture
   FROM crypto.bronze.coin_prices_raw;

2. SELECT COUNT(*) AS silver_rows, MAX(observed_at) AS last_silver
   FROM crypto.silver.coin_prices;

3. SELECT * FROM crypto.gold.coin_momentum_24h
   ORDER BY momentum_rank LIMIT 10;

4. SELECT symbol, realized_volatility_7d
   FROM crypto.gold.coin_volatility_7d
   ORDER BY realized_volatility_7d DESC LIMIT 5;
```

Después de la primera corrida:
- Bronze: 100 filas.
- Silver: hasta 100 filas (si hubo dedup, puede ser menos).
- Gold momentum: 100 filas, una por moneda, con ranking del 1 al 100.
- Gold volatility: 0 filas (necesita varios días de datos para poblar).

Si Bronze tiene 100 filas, el Custom Data Source funciona.

---

## Parte F — Capa de consumo (Genie + Dashboard)

### F.1 Asegurar que el SQL Warehouse está corriendo

1. UI → SQL Warehouses.
2. Si el "Serverless Starter Warehouse" está STOPPED, click en **Start**.
3. Espera 1-2 minutos hasta que esté RUNNING.

### F.2 Crear el Genie Space

Desde Claude Code:

```
Crea un Genie Space con esta configuración:

- Title: "Crypto Insights ES"
- Description: "Análisis de momentum y volatilidad de criptomonedas en español"
- Warehouse: el Serverless Starter Warehouse (busca su id con list_warehouses)
- Tablas:
    - crypto.gold.coin_momentum_24h
    - crypto.gold.coin_volatility_7d
    - crypto.silver.coin_prices

Después agrega los knowledge snippets leyéndolos de specs/04-genie.md
sección "Knowledge snippets".

Y agrega las 3 sample queries de la sección "Sample queries (Trusted)"
del mismo spec.

Las instrucciones generales del spec también: idioma español, redondear
porcentajes a 2 decimales, pedir aclaración si la pregunta es ambigua.
```

### F.3 Probar Genie

1. UI → Genie en el sidebar.
2. Abre "Crypto Insights ES".
3. Pruebas:
   - *"¿Cuáles son las 5 monedas con mayor subida hoy?"*
   - *"Top 10 monedas por capitalización de mercado."*
   - *"Dame el precio actual de Bitcoin."*

Genie debe generar SQL, ejecutarlo, y devolverte una tabla con resultados.

### F.4 Crear el AI/BI Dashboard

Desde Claude Code:

```
Crea un AI/BI Dashboard llamado "Crypto Overview" con estas visualizaciones:

Dataset 1 (Top Momentum):
SELECT symbol, name, pct_change_24h, momentum_rank
FROM crypto.gold.coin_momentum_24h
WHERE momentum_rank <= 10
ORDER BY momentum_rank;

Dataset 2 (Distribución 24h):
SELECT symbol, pct_change_24h
FROM crypto.gold.coin_momentum_24h
WHERE momentum_rank <= 20;

Dataset 3 (Top Market Cap):
SELECT symbol, name, market_cap_usd
FROM crypto.gold.coin_momentum_24h
ORDER BY market_cap_usd DESC LIMIT 20;

Dataset 4 (Más volátiles):
SELECT symbol, realized_volatility_7d, n_observations
FROM crypto.gold.coin_volatility_7d
ORDER BY realized_volatility_7d DESC LIMIT 10;

Visualizaciones:
1. Tabla con Dataset 1.
2. Bar chart vertical: Dataset 2 (eje X = symbol, eje Y = pct_change_24h).
3. Bar chart horizontal: Dataset 3 (eje Y = symbol, eje X = market_cap_usd).
4. Bar chart vertical: Dataset 4 (eje X = symbol, eje Y = realized_volatility_7d).

Usa el Serverless Starter Warehouse.
```

Verifica en UI → Dashboards → Crypto Overview.

---

## Parte G — Activar el Job para producción continua

Por defecto el Job está PAUSED para que puedas desarrollar sin ejecuciones automáticas. Cuando estés listo:

### G.1 Editar el archivo del Job

Abre `resources/jobs/crypto_orchestrator.yml` y cambia:

```yaml
pause_status: PAUSED
```

Por:

```yaml
pause_status: UNPAUSED
```

### G.2 Redesplegar

```powershell
databricks bundle deploy --target dev --profile crypto
```

### G.3 Verificar

UI → Workflows → Jobs → `crypto_orchestrator_dev` debe estar **Active**.

A partir de este momento:
- Cada 15 minutos, el Job dispara el pipeline.
- El pipeline llama a CoinGecko via `CoinGeckoDataSource`.
- Bronze, Silver, Gold se actualizan.
- Tu Dashboard y Genie tienen siempre datos frescos.

### G.4 Commit Git

```powershell
git add resources/jobs/crypto_orchestrator.yml
git commit -m "chore: activate orchestrator job for continuous production"
git push
```

---

## Réplica en otro workspace Free Edition

Para clonar el proyecto a un segundo workspace:

### R1 — Crear segunda cuenta Free Edition

Con un email distinto, repite la Parte A.

### R2 — Configurar segundo perfil CLI

```powershell
databricks configure --profile crypto2 --token
```

Con la URL y PAT del segundo workspace.

### R3 — Clonar el repo

```powershell
git clone https://github.com/<tu-usuario>/crypto-pipeline-starter crypto-pipeline-replica
cd crypto-pipeline-replica
```

### R4 — Configurar el host en `databricks.yml`

Igual que en B.2 pero con la URL del segundo workspace.

### R5 — Instalar el ai-dev-kit

Desde dentro de `crypto-pipeline-replica`:

```powershell
irm https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.ps1 | iex
```

Elige perfil `crypto2`.

### R6 — Validar CoinGecko en el nuevo workspace

Repite la Parte C. La política de outbound puede ser diferente entre workspaces.

### R7 — Desplegar y correr

```powershell
databricks bundle deploy --target dev --profile crypto2
databricks bundle run crypto_medallion --target dev --profile crypto2
```

### R8 — Recrear Genie Space y Dashboard

Reinicia Claude Code (para que cargue el nuevo `.mcp.json`) y repite la Parte F.

En menos de 15 minutos tienes el mismo pipeline corriendo en el segundo workspace.

---

## Cómo usar Claude Code en este proyecto

El flujo es **spec-driven**: lees specs, Claude propone implementaciones, tú apruebas, el MCP ejecuta.

### Tres tipos de prompts que vas a usar

**1. Planificación (al inicio de cada cambio):**
```
Lee specs/03-medallion.md y dime qué cambiarías en src/pipelines/02_silver.py
para agregar el campo "circulating_supply" al Silver. NO escribas código todavía,
solo el plan.
```

**2. Implementación (después de aprobar el plan):**
```
Procede con el plan. Modifica el spec primero, después el código,
después el bundle si hace falta. Muéstrame el diff de cada archivo.
```

**3. Ejecución (cuando hay que tocar el workspace):**
```
Despliega el bundle al target dev y dispara el pipeline. Reportame
el resultado de cada paso.
```

### Ejemplos de prompts útiles

**Inspección de datos:**
```
Cuántas filas tiene crypto.silver.coin_prices y cuál es el rango
de observed_at? Dame las 5 monedas con mayor pct_change_24h ahora.
```

**Debugging:**
```
El pipeline falló en Bronze con error de timeout a CoinGecko.
Corre el notebook tests/integration/test_coingecko_databricks.py
y dime qué pasa con la conectividad.
```

**Nuevo feature con specs:**
```
Quiero agregar una vista Gold que calcule la correlación entre BTC y ETH
en los últimos 7 días. Actualiza specs/01-data-contract.md y
specs/03-medallion.md, después implementa src/pipelines/03_gold.py.
```

### Cómo funciona `CLAUDE.md`

Cuando abres Claude Code en la raíz del proyecto, automáticamente lee `CLAUDE.md`. Ahí están definidos el idioma de respuesta, el workflow spec-driven, las skills, las reglas del MCP y las restricciones de Free Edition. Si quieres cambiar el comportamiento de Claude, edita `CLAUDE.md` y reinicia la sesión.

---

## Troubleshooting

### El `bundle validate` falla con "host doesn't match"

Ocurre cuando el `host` en `databricks.yml` no coincide con el del perfil CLI. Asegúrate de haber completado el paso B.2: abre `databricks.yml` y pon la URL real de tu workspace en `targets.dev.workspace.host`.

### El `bundle deploy` falla con "Catalog resources are only supported with direct deployment mode"

El catálogo no debe estar declarado en el bundle. Asegúrate de haber eliminado `resources/schemas/crypto_catalog.yml` del proyecto y de haber creado el catálogo manualmente con `CREATE CATALOG IF NOT EXISTS crypto;` antes del deploy (paso D.1).

### El `bundle deploy` falla con "cannot create catalog"

Posibles causas:

1. **El catálogo ya existe** y no eres su owner. Ejecuta `DROP CATALOG crypto CASCADE;` desde Claude Code y redepliega.
2. **Tu PAT tiene scopes restringidos.** Genera uno nuevo sin scopes específicos.
3. **Tu CLI es vieja.** Actualiza con el comando de instalación del paso A.3.

### El pipeline falla con "ModuleNotFoundError: pyspark.sql.datasource"

El Custom Data Source requiere DBR 14.3+ con channel `PREVIEW`. Verifica en `resources/pipelines/crypto_medallion.yml`:

```yaml
channel: PREVIEW
```

Si está en `CURRENT`, cámbialo y redepliega.

### El pipeline falla con "Connection timeout" hacia CoinGecko

1. Vuelve a correr `tests/integration/test_coingecko_databricks.py` para confirmar si el problema es la conectividad.
2. Si CoinGecko no es alcanzable, considera revertir al patrón v1 con ingester local.

### El MCP no aparece en Claude Code

1. Verifica que `.mcp.json` existe en la raíz de `crypto-pipeline-starter` (no en la carpeta padre).
2. Cierra y vuelve a abrir Claude Code para que recargue la configuración.
3. Verifica que `uv` está instalado: `uv --version` en una terminal nueva.
4. Si nada funciona, reinstala el ai-dev-kit desde dentro de la carpeta del proyecto.

### Bronze tiene 0 filas después del primer trigger

1. Revisa el log: UI → Workflows → Pipelines → `crypto_medallion_dev` → Updates → último update.
2. Si dice "Source data source 'coingecko' not found": verifica que `01_bronze.py` tiene `spark.dataSource.register(CoinGeckoDataSource)` antes del `@dp.table`.
3. Si dice "Connection timeout": ver troubleshooting de CoinGecko arriba.

### CoinGecko devuelve 429 (rate limit)

Hacemos 1 request cada 15 min, muy por debajo del límite. Si ocurre igual:
- Aumenta el intervalo del Job a 30 minutos en `crypto_orchestrator.yml`.
- Considera obtener una API key Demo gratuita en https://www.coingecko.com/en/api.

### El warehouse del Genie no arranca

UI → SQL Warehouses → Start. Si no arranca, tu cuota Free puede haberse agotado (resetea al día siguiente UTC).

### Genie no usa los knowledge snippets

UI del Genie Space → Configure → haz click en **Save** después de cualquier cambio. Los snippets aplican a la próxima conversación, no a la activa.

### El segundo workspace replicado no encuentra el catálogo

Cada Free Edition tiene su propio metastore. El bundle crea el catálogo de nuevo al hacer `bundle deploy --profile crypto2`.

---

## Resumen del flujo de extremo a extremo

```
1. Setup (A)              → CLI + uv + Claude Code + perfil
2. Bootstrap (B)          → clone + host en databricks.yml + ai-dev-kit + verificar MCP
3. Validar CoinGecko (C)  → notebook test_coingecko_databricks.py
4. Deploy (D)             → bundle validate + bundle deploy + bundle run
5. Verificar (E)          → queries SQL en bronze/silver/gold
6. Consumo (F)            → Genie Space + Dashboard via Claude Code
7. Producción (G)         → activar Job (UNPAUSED)
```

Una vez completas el flujo, tu pipeline:

- Se actualiza solo cada 15 minutos.
- No depende de tu laptop.
- Es 100% reproducible vía `git clone` + `bundle deploy`.
- Está gobernado por Unity Catalog.
- Es consumible en lenguaje natural (Genie) y visual (Dashboard).
