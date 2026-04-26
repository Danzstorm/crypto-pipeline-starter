# Guía de implementación v2 — Pipeline crypto end-to-end en Databricks Free Edition

Esta guía te lleva paso a paso desde cero hasta tener un pipeline financiero completo de criptomonedas corriendo en Databricks Free Edition. Asume que descargaste el `crypto-pipeline-starter` v2 (zip o `git clone`) y todos los archivos de código, specs y bundle ya están escritos.

> **Importante:** este documento NO contiene anexos con código embebido. La fuente de verdad de cada archivo es el archivo mismo en el starter. Si modificas algo, modifícalo en el archivo, no en esta guía.

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

Este patrón reemplaza al ingester local que existía en la v1.

### ¿Necesitas GitHub?

No estrictamente. Necesitas Git local para versionar tus cambios. GitHub es opcional pero muy recomendable si vas a distribuir el proyecto a alumnos del bootcamp.

### ¿Por qué validamos primero CoinGecko?

Free Edition restringe el outbound del cómputo serverless a un set de dominios "trusted". Si CoinGecko cae fuera de esa lista en tu workspace específico, todo el pipeline falla en Bronze. Por eso la Parte C corre un notebook de prueba antes del primer deploy: si pasa, sigues con la guía. Si falla, hay que cambiar a la arquitectura v1 (con ingester local).

---

## Pre-requisitos

- Computadora con macOS, Linux, o Windows.
- Email para registrarte en Databricks Free Edition.
- Cuenta Anthropic con créditos para Claude Code.
- Vas a instalar: Databricks CLI, `uv`, Python 3.10+, Claude Code, Git.

---

## Parte A — Setup (una sola vez)

### A.1 Crear cuenta Databricks Free Edition

1. https://signup.databricks.com → Express Setup.
2. Verifica el email.
3. Anota la URL del workspace (ej. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`).

### A.2 Generar Personal Access Token (PAT)

1. Avatar → Settings → Developer → Access tokens → Manage → Generate new token.
2. Comment: `crypto-pipeline-laptop`. Lifetime: `90` días.
3. **Copia el token y guárdalo.** No se vuelve a mostrar.

### A.3 Instalar Databricks CLI

**macOS / Linux:**
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

**Windows (PowerShell):**
```powershell
winget install Databricks.DatabricksCLI
```

Verifica: `databricks --version` (debe ser 0.250.0 o superior).

### A.4 Instalar `uv`

**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### A.5 Instalar Claude Code

1. Visita https://claude.ai/code y sigue las instrucciones.
2. Autentica con `claude` (te lleva al navegador).
3. Verifica: `claude --version`.

### A.6 Configurar perfil CLI

```powershell
databricks configure --profile crypto --token
```

Cuando pregunte:
- **Host:** la URL del workspace.
- **Token:** el PAT del paso A.2.

Verifica:
```powershell
databricks current-user me --profile crypto
```

Debe devolver tu email.

---

## Parte B — Bootstrap del proyecto

### B.1 Obtener el starter

**Opción 1 (recomendada): clone desde GitHub**

```powershell
git clone https://github.com/<tu-usuario>/crypto-pipeline-starter
cd crypto-pipeline-starter
```

**Opción 2: descomprimir el zip**

Si descargaste el zip:
```powershell
# Asume que el zip está en Downloads
cd "D:\Daniel\AI Data Engineer Bootcamp\webinar"
Expand-Archive -Path "$env:USERPROFILE\Downloads\crypto-pipeline-starter.zip" -DestinationPath .
cd crypto-pipeline-starter
```

A partir de aquí, todos los comandos asumen que estás dentro de la carpeta del proyecto.

### B.2 Instalar el `ai-dev-kit`

```powershell
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh) --profile crypto
```

> En Windows PowerShell, si el comando con `bash <(...)` no funciona, instala Git Bash (viene con Git for Windows) y corre el comando ahí. O alternativamente:
> ```powershell
> curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh -o install.sh
> bash install.sh --profile crypto
> Remove-Item install.sh
> ```

Cuando pregunte por tools, selecciona `claude`.

El instalador crea:
- `.claude/skills/` con 19 skills de Databricks.
- `.mcp.json` para conectar Claude Code al MCP server local.

### B.3 Verificar el MCP

```powershell
claude
```

Dentro de Claude Code:
```
/mcp
```

Debe mostrar:
```
databricks: connected ✓
```

Prueba:
```
Lista mis SQL warehouses usando el MCP de Databricks.
```

Si responde con la lista, todo está conectado. Sal con `/exit`.

### B.4 Commit del setup (si es repo Git)

```powershell
git add .
git commit -m "feat: install ai-dev-kit"
git push
```

---

## Parte C — Validar conectividad a CoinGecko

> **Esta parte es nueva en v2 y es crítica.** No la saltes. Si CoinGecko no es alcanzable desde tu workspace específico, el pipeline va a fallar y tendrás que cambiar de arquitectura.

### C.1 Subir el notebook de validación a Databricks

1. UI → Workspace → tu carpeta personal.
2. Click derecho → **Import**.
3. Selecciona `tests/integration/test_coingecko_databricks.py` desde tu carpeta local.
4. **Import format:** Source.

Alternativa: abrir el archivo en VS Code, copiar todo, y crear un notebook nuevo en Databricks pegando el contenido.

### C.2 Ejecutar el notebook

1. Abre el notebook recién importado.
2. **Connect** → **Serverless** (es la única opción en Free Edition).
3. **Run all** en la barra superior.

El notebook ejecuta cuatro tests:

- **Test 1:** resolución DNS de `api.coingecko.com`.
- **Test 2:** llamada HTTP simple al endpoint `/ping`.
- **Test 3:** llamada con datos reales (10 monedas).
- **Test 4:** control con `pypi.org` (debe pasar siempre).

### C.3 Interpretar los resultados

**Si los 4 tests pasan:** sigue con la Parte D. CoinGecko es alcanzable, el resto de la guía aplica tal cual.

**Si Test 2 o Test 3 falla con TIMEOUT:** Free Edition bloquea outbound a CoinGecko en tu workspace. Tienes tres opciones:

1. **Reportar y esperar:** los dominios trusted de Databricks cambian. Quizás en unas semanas funcione.
2. **Probar otro API:** considera CoinCap (`api.coincap.io`) o Binance (`api.binance.com`). Si alguno funciona, modifica `src/pipelines/01_bronze.py` para apuntar a ese.
3. **Volver a v1:** la versión anterior con ingester local funciona en cualquier caso. Está disponible en el repo bajo el tag `v1.0`.

**Si Test 4 (PyPI) también falla:** tu cluster está mal. Reinicia el compute y vuelve a probar.

---

## Parte D — Despliegue de infraestructura y pipeline

Esta parte crea catálogo, schemas, pipeline y job en tu workspace en un solo `bundle deploy`.

### D.1 Validar el bundle

```powershell
databricks bundle validate --profile crypto
```

Salida esperada: `Validation OK!`

Si hay errores, son típicamente de indentación YAML — revisa los espacios.

### D.2 Desplegar

```powershell
databricks bundle deploy --target dev --profile crypto
```

Esto crea, en orden:
- Catálogo `crypto`
- Schemas `crypto.bronze`, `crypto.silver`, `crypto.gold`
- Pipeline `crypto_medallion_dev`
- Job `crypto_orchestrator_dev` (paused)

Salida esperada (resumida):

```
Building deployment plan...
- catalog crypto                       (create)
- schema  crypto.bronze                (create)
- schema  crypto.silver                (create)
- schema  crypto.gold                  (create)
- pipeline crypto_medallion_dev        (create)
- job crypto_orchestrator_dev          (create)

Applying changes...
Deployment complete!
```

### D.3 Verificar en la UI

1. UI → Catalog. Debe aparecer `crypto` con `bronze`, `silver`, `gold`.
2. UI → Workflows → Pipelines. Debe aparecer `crypto_medallion_dev`.
3. UI → Workflows → Jobs. Debe aparecer `crypto_orchestrator_dev` (Status: PAUSED).

### D.4 Disparar el pipeline manualmente

```powershell
databricks bundle run crypto_medallion --target dev --profile crypto
```

O desde Claude Code:
```
claude
```
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
- Gold volatility: 0 filas (necesita 3 días de datos para empezar a poblar).

> Para que Gold volatility tenga datos, necesitas que el Job corra varios días. Lo activamos en la Parte G.

Si Bronze tiene 100 filas, el Custom Data Source funciona. ¡Felicidades, llegaste a la mitad del proyecto!

---

## Parte F — Capa de consumo (Genie + Dashboard)

### F.1 Asegurar que el SQL Warehouse está corriendo

1. UI → SQL Warehouses.
2. Si el "Serverless Starter Warehouse" está STOPPED, click en **Start**.
3. Espera 1-2 minutos hasta que esté RUNNING.

### F.2 Crear el Genie Space

Free Edition todavía no expone el recurso `genie_spaces` en DAB de manera consistente, así que lo creamos vía Claude Code + MCP.

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

Después agrega los siguientes knowledge snippets, leyéndolos
de specs/04-genie.md sección "Knowledge snippets".

Y agrega las 3 sample queries de la sección "Sample queries (Trusted)"
del mismo spec.

Las instrucciones generales del spec también: idioma español, redondear
porcentajes a 2 decimales, pedir aclaración si la pregunta es ambigua.
```

### F.3 Probar Genie

1. UI → Genie en el sidebar.
2. Abre "Crypto Insights ES".
3. Pruebas iniciales:
   - *"¿Cuáles son las 5 monedas con mayor subida hoy?"*
   - *"Top 10 monedas por capitalización de mercado."*
   - *"Dame el precio actual de Bitcoin."*

Genie debe generar SQL legible, ejecutarlo, y devolverte una tabla con resultados.

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

Por defecto, el Job está PAUSED. Esto te permitió desarrollar sin que se ejecute automáticamente. Cuando estés listo para producción continua:

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

UI → Workflows → Jobs → `crypto_orchestrator_dev` debe estar **Active** (no Paused).

A partir de este momento:
- Cada 15 minutos, el Job dispara el pipeline.
- El pipeline llama a CoinGecko via `CoinGeckoDataSource`.
- Bronze, Silver, Gold se actualizan.
- Tu Dashboard y Genie tienen siempre datos frescos.

Sin tu laptop encendida, sin cron, sin nada externo.

### G.4 Commit Git (si es repo)

```powershell
git add resources/jobs/crypto_orchestrator.yml
git commit -m "chore: activate orchestrator job for production"
git push
```

---

## Réplica en otro workspace Free Edition

Para clonar el proyecto a un segundo workspace (otra cuenta Free):

### Paso R1 — Crear segunda cuenta Free Edition

Con un email distinto al primero, repite la Parte A.

### Paso R2 — Configurar segundo perfil CLI

```powershell
databricks configure --profile crypto2 --token
```

Con la URL y PAT del segundo workspace.

### Paso R3 — Clonar el repo

```powershell
cd ~
git clone https://github.com/<tu-usuario>/crypto-pipeline-starter crypto-pipeline-replica
cd crypto-pipeline-replica
```

### Paso R4 — Editar `.mcp.json`

Cambia el perfil de `crypto` a `crypto2` para que Claude Code use el nuevo workspace:

```json
{
  "mcpServers": {
    "databricks": {
      ...
      "env": {
        "DATABRICKS_CONFIG_PROFILE": "crypto2"
      }
    }
  }
}
```

### Paso R5 — Validar conectividad CoinGecko en el nuevo workspace

Repite la Parte C en el segundo workspace. La política de outbound podría ser diferente.

### Paso R6 — Desplegar

```powershell
databricks bundle deploy --target dev --profile crypto2
databricks bundle run crypto_medallion --target dev --profile crypto2
```

### Paso R7 — Recrear Genie Space y Dashboard

Reinicia Claude Code (para que cargue el nuevo `.mcp.json`) y repite la Parte F.

En menos de 15 minutos tienes el mismo pipeline corriendo en el segundo workspace.

---

## Cómo usar Claude Code en este proyecto

El flujo es **spec-driven**: tú escribes/lees specs, Claude propone implementaciones, tú apruebas, el MCP ejecuta.

### Tres tipos de prompts que vas a usar

**1. Prompts de planificación (al inicio de cada cambio):**

```
Lee specs/03-medallion.md y dime qué cambiarías en src/pipelines/02_silver.py
para agregar el campo "circulating_supply" al Silver. NO escribas código todavía,
solo el plan.
```

Claude responde con un plan. Tú evalúas, ajustas.

**2. Prompts de implementación (después de aprobar el plan):**

```
Procede con el plan. Modifica el spec primero, después el código,
después el bundle si hace falta. Mostrame el diff de cada archivo.
```

Claude muestra los cambios. Tú revisas y apruebas.

**3. Prompts de ejecución (cuando hay que tocar el workspace):**

```
Despliega el bundle al target dev y dispara el pipeline. Reportame
el resultado de cada paso.
```

Claude llama el MCP. Antes de cada acción destructiva, espera tu confirmación.

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
y dime qué pasa con la conectividad ahora.
```

**Refactor con specs:**
```
Quiero agregar una nueva vista Gold que calcule la correlación
entre BTC y ETH en los últimos 7 días. Actualiza specs/01-data-contract.md
y specs/03-medallion.md, después implementa src/pipelines/03_gold.py.
```

### Cómo funciona `CLAUDE.md`

Cuando lanzas `claude` en la raíz del proyecto, automáticamente lee `CLAUDE.md` y carga las instrucciones que ahí defines. Para este proyecto, el `CLAUDE.md` ya está escrito y especifica:

- El idioma de respuesta (español neutro).
- El workflow obligatorio (lee specs primero, propón plan, espera aprobación).
- Las skills relevantes a este proyecto.
- Las reglas duras del MCP (no `DROP CATALOG`, no `bundle destroy` sin aprobación).
- Las restricciones de Free Edition que no debe intentar saltar.
- El patrón Custom Data Source para captura.

Si en algún momento sientes que Claude responde con un patrón que no quieres, edita `CLAUDE.md` y reinicia Claude. Es el knob que controla todo.

---

## Troubleshooting

### El bundle deploy falla con "cannot create catalog"

Posibles causas:

1. **El catálogo ya existe** y no eres su owner. Borra con `DROP CATALOG crypto CASCADE;` y redepliega.
2. **Tu PAT tiene scopes restringidos.** Genera uno nuevo sin scopes específicos.
3. **Tu CLI es vieja.** Actualiza con el script del paso A.3.

Si nada funciona, alternativa:

1. Renombra `resources/schemas/crypto_catalog.yml` a `crypto_catalog.yml.disabled`.
2. Desde Claude Code: `Ejecuta CREATE CATALOG IF NOT EXISTS crypto;`
3. Redepliega.

### El pipeline falla con "ModuleNotFoundError: pyspark.sql.datasource"

La sintaxis de Custom Data Source requiere DBR 14.3+ con channel `PREVIEW`. Verifica `resources/pipelines/crypto_medallion.yml`:

```yaml
channel: PREVIEW
```

Si está en `CURRENT`, cámbialo y redepliega.

### El pipeline falla con "Connection timeout" hacia CoinGecko

Las reglas de outbound de Free Edition cambiaron, o CoinGecko está caído.

1. Vuelve a correr `tests/integration/test_coingecko_databricks.py` para confirmar.
2. Si CoinGecko está OK pero Databricks no lo alcanza, considera revertir al patrón v1 con ingester local.

### `claude /mcp` no muestra databricks

1. Verificar que `.mcp.json` existe en la raíz del proyecto.
2. Salir y volver a entrar a Claude Code.
3. Verificar `where uv` (Windows) o `which uv` (Linux/Mac).
4. Reinstalar el ai-dev-kit con `--force`.

### Bronze tiene 0 filas después del primer trigger

1. Verifica el log del pipeline en UI → Workflows → Pipelines → `crypto_medallion_dev` → Updates → último update.
2. Si dice "Source data source 'coingecko' not found": el `spark.dataSource.register()` falló. Verifica que `01_bronze.py` esté bien copiado.
3. Si dice "Connection timeout": ver Troubleshooting anterior.

### CoinGecko devuelve 429 (rate limit)

Improbable porque hacemos 1 request cada 15 min, muy por debajo del límite. Si pasa:
- Aumenta el intervalo del Job a 30 minutos en `crypto_orchestrator.yml`.
- Considera obtener una API key Demo gratuita en https://www.coingecko.com/en/api.

### El warehouse del Genie no arranca

UI → SQL Warehouses → Start. Si no arranca, revisa que tu cuota Free no se haya agotado (resetea al día siguiente UTC).

### Genie no usa los knowledge snippets

UI del Genie Space → Configure → asegúrate de hacer click en **Save** después de cualquier cambio. Los snippets aplican a la próxima conversación, no a la activa.

### El segundo workspace replicado no encuentra el catálogo

Cada Free Edition tiene su propio metastore. El catálogo no se comparte entre cuentas. El bundle lo crea de nuevo cuando ejecutas `bundle deploy --profile crypto2`.

---

## Resumen del flujo de extremo a extremo

```
1. Setup (A)              → CLI + uv + Claude Code + perfil
2. Bootstrap (B)          → clone + ai-dev-kit + verificar MCP
3. Validar CoinGecko (C)  → notebook test_coingecko_databricks.py
4. Deploy (D)             → bundle deploy + bundle run
5. Verificar (E)          → queries SQL en bronze/silver/gold
6. Consumo (F)            → Genie Space + Dashboard via Claude
7. Producción (G)         → activar Job (UNPAUSED)
```

Una vez completas el flujo, tu pipeline:

- Se actualiza solo cada 15 minutos.
- No depende de tu laptop.
- Es 100% reproducible vía `git clone` + `bundle deploy`.
- Está gobernado por Unity Catalog.
- Es consumible en lenguaje natural (Genie) y visual (Dashboard).

Eso es lo que diferencia un proyecto de prueba de un pipeline de producción.
