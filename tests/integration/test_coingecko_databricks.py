# Databricks notebook source
# MAGIC %md
# MAGIC # Experimento: ¿CoinGecko es alcanzable desde Databricks Free Edition?
# MAGIC
# MAGIC **Objetivo:** validar empíricamente si el cómputo serverless de Free Edition
# MAGIC permite llamadas HTTP salientes a `api.coingecko.com`.
# MAGIC
# MAGIC **Por qué importa:** la respuesta determina la arquitectura del pipeline.
# MAGIC Si funciona, la ingesta puede correr 100% dentro de Databricks (Job + Python).
# MAGIC Si no funciona, necesitamos un ingester local que suba al Volume.
# MAGIC
# MAGIC **Cómo correrlo:**
# MAGIC 1. Crea un notebook nuevo en tu workspace Free Edition.
# MAGIC 2. Cópialo y pégale este contenido.
# MAGIC 3. Asegúrate de tenerlo conectado a Serverless compute (es la única opción en Free).
# MAGIC 4. Ejecuta cada celda en orden.
# MAGIC 5. Reporta el resultado (qué celda falla y con qué error).
# MAGIC
# MAGIC **Tiempo estimado:** 2 minutos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1 — Resolución DNS
# MAGIC
# MAGIC Primero verificamos si el cluster puede al menos resolver el dominio.
# MAGIC Esto es independiente de si la conexión HTTP es permitida.

# COMMAND ----------

import socket

try:
    ip = socket.gethostbyname("api.coingecko.com")
    print(f"DNS OK -> api.coingecko.com resuelve a {ip}")
except Exception as e:
    print(f"DNS FAIL -> {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2 — Llamada HTTP simple
# MAGIC
# MAGIC Ahora intentamos una llamada GET real al endpoint `/ping`, que es el más liviano de CoinGecko.

# COMMAND ----------

import requests

try:
    response = requests.get(
        "https://api.coingecko.com/api/v3/ping",
        timeout=10,
    )
    print(f"HTTP {response.status_code} -> {response.text}")
except requests.exceptions.ConnectTimeout as e:
    print(f"TIMEOUT (probablemente bloqueado por firewall) -> {e}")
except requests.exceptions.SSLError as e:
    print(f"SSL ERROR (TLS bloqueado o cert problem) -> {e}")
except requests.exceptions.ConnectionError as e:
    print(f"CONNECTION ERROR (host inalcanzable o reseteado) -> {e}")
except Exception as e:
    print(f"OTRO ERROR -> {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3 — Llamada con datos reales
# MAGIC
# MAGIC Si los anteriores pasaron, traemos un payload pequeño (10 monedas) para confirmar
# MAGIC que la respuesta llega íntegra y parseable.

# COMMAND ----------

import json

try:
    response = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 10,
            "page": 1,
        },
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    print(f"OK -> recibidas {len(data)} monedas")
    print(f"\nPrimera moneda (resumen):")
    first = data[0]
    print(f"  symbol: {first.get('symbol')}")
    print(f"  name: {first.get('name')}")
    print(f"  current_price: {first.get('current_price')}")
    print(f"  market_cap: {first.get('market_cap'):,}")
except Exception as e:
    print(f"FALLA -> {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4 — Comparación con un dominio que SÍ debería estar permitido
# MAGIC
# MAGIC Como control, intentamos llegar a `pypi.org`. Este dominio está en la lista de
# MAGIC trusted de Databricks (lo necesitan para `pip install`). Si este test falla,
# MAGIC el problema es de tu cluster, no del firewall de outbound.

# COMMAND ----------

try:
    response = requests.get("https://pypi.org/simple/", timeout=10)
    print(f"PyPI OK -> HTTP {response.status_code}")
except Exception as e:
    print(f"PyPI FAIL -> {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnóstico
# MAGIC
# MAGIC Lee los resultados y compara contra esta tabla:
# MAGIC
# MAGIC | Test 1 (DNS) | Test 2 (HTTP) | Test 3 (datos) | Test 4 (PyPI) | Diagnóstico |
# MAGIC |---|---|---|---|---|
# MAGIC | OK | OK | OK | OK | **CoinGecko es alcanzable.** Podemos eliminar el ingester local. |
# MAGIC | OK | TIMEOUT | n/a | OK | **Outbound bloqueado a CoinGecko.** Ingester local sigue siendo necesario. |
# MAGIC | OK | SSL ERROR | n/a | OK | **TLS bloqueado.** Probablemente firewall corporativo en proxy. |
# MAGIC | FAIL | n/a | n/a | OK | **DNS bloqueado para CoinGecko.** Mismo veredicto: ingester local. |
# MAGIC | OK | OK | OK | FAIL | **Algo raro con tu cluster.** Reiniciá el compute y vuelve a probar. |
# MAGIC | FAIL | FAIL | n/a | FAIL | **Outbound completamente apagado.** Algo está mal con la cuenta. |
# MAGIC
# MAGIC Reporta qué combinación obtuviste y seguimos según el resultado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Información extra de diagnóstico (opcional)

# COMMAND ----------

import platform
import sys

print(f"Python version: {sys.version}")
print(f"Platform: {platform.platform()}")
print(f"requests version: {requests.__version__}")

# Información del cluster
try:
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "n/a")
    print(f"Cluster ID: {cluster_id}")
except Exception:
    pass
