# 05 — Runbook

## Comandos de operación diaria

```powershell
# Validar el bundle
databricks bundle validate --profile crypto

# Desplegar cambios
databricks bundle deploy --target dev --profile crypto

# Disparar el pipeline manualmente
databricks bundle run crypto_medallion --target dev --profile crypto

# Listar pipelines
databricks pipelines list --profile crypto

# Ver el último estado del Job orquestador
databricks jobs list --profile crypto
```

## Cómo funciona ahora la captura

El Job `crypto_orchestrator_dev` corre cada 15 minutos. Cada ejecución dispara el pipeline `crypto_medallion_dev`. El pipeline:

1. Llama a `api.coingecko.com/api/v3/coins/markets` desde Bronze (vía `CoinGeckoDataSource`).
2. Inserta 100 filas nuevas en `crypto.bronze.coin_prices_raw` con un `snapshot_id` único.
3. Procesa el delta en Silver (typing + dedup).
4. Refresca las materialized views de Gold.

No hay ingester local. No hay cron en laptop. Todo vive en Databricks.

## Troubleshooting

### El pipeline falla con "Connection refused" o "Timeout" hacia CoinGecko

Las reglas de outbound de Free Edition cambiaron, o CoinGecko está caído.

1. Corre `test_coingecko_databricks.py` (en `tests/integration/`) en un notebook para confirmar.
2. Si el test falla, revisa el status de CoinGecko: https://status.coingecko.com.
3. Si CoinGecko está OK pero Databricks no lo alcanza, considera volver al patrón "ingester local + Volume" de la v1.

### Silver tiene > 5% de drops

CoinGecko cambió el schema. Inspecciona qué está rescatado:

```sql
SELECT _rescued_data
FROM crypto.bronze.coin_prices_raw
WHERE _rescued_data IS NOT NULL
LIMIT 5;
```

Actualiza `02_silver.py` con los nuevos campos. Redepliega.

### Bronze deja de crecer

1. Revisa el último estado del pipeline en la UI de Databricks → Workflows → Pipelines.
2. Si el pipeline está PAUSED por el modo development de DAB, dispáralo manualmente con `databricks bundle run crypto_medallion`.
3. Verifica que el Job orquestador esté UNPAUSED si lo activaste para producción.

### Genie devuelve resultados vacíos

- Verificar permisos: `SHOW GRANTS ON SCHEMA crypto.gold`.
- Verificar que el warehouse del Genie Space está RUNNING.
- Refrescar las vistas:
  ```sql
  REFRESH MATERIALIZED VIEW crypto.gold.coin_momentum_24h;
  ```

### El PAT expiró

```powershell
databricks configure --profile crypto --token
```

Genera un nuevo PAT en la UI y pégalo.

## Limpieza completa

```powershell
databricks bundle destroy --target dev --profile crypto
```

Esto borra el pipeline y el job. El catálogo y schemas permanecen — para borrarlos:

```sql
DROP CATALOG crypto CASCADE;
```
