# 05 — Runbook

## Comandos de operación diaria

```bash
# Validar el bundle
databricks bundle validate --profile crypto

# Desplegar cambios
databricks bundle deploy --target dev --profile crypto

# Disparar el pipeline manualmente
databricks bundle run crypto_medallion --target dev --profile crypto

# Listar archivos en el Volume
databricks fs ls dbfs:/Volumes/crypto/raw/coin_prices/ --profile crypto -r

# Ejecutar el ingester
DATABRICKS_CONFIG_PROFILE=crypto python src/ingestion/ingest_crypto.py
```

## Troubleshooting

### Bronze no avanza

1. Verificar que hay archivos nuevos en el Volume.
2. Si el schema location se corrompió, borrarlo:
   ```bash
   databricks fs rm -r dbfs:/Volumes/crypto/raw/_schemas/ --profile crypto
   ```
3. Re-disparar el pipeline.

### Silver tiene > 5% de drops

- CoinGecko cambió el schema. Inspeccionar `_rescued_data`:
  ```sql
  SELECT _rescued_data FROM crypto.bronze.coin_prices_raw
  WHERE _rescued_data IS NOT NULL LIMIT 5;
  ```
- Actualizar `02_silver.py` y redesplegar.

### Genie devuelve resultados vacíos

- Verificar permisos: `SHOW GRANTS ON SCHEMA crypto.gold`.
- Verificar que el warehouse del Genie Space está RUNNING.
- Refrescar las vistas:
  ```sql
  REFRESH MATERIALIZED VIEW crypto.gold.coin_momentum_24h;
  ```

### El PAT expiró

```bash
databricks configure --profile crypto --token
```

Genera un nuevo PAT en la UI y pégalo.

## Limpieza completa

```bash
databricks bundle destroy --target dev --profile crypto
```

Esto borra el pipeline, jobs y dashboard. El catálogo y volume permanecen (porque son recursos de UC) — para borrarlos:

```sql
DROP CATALOG crypto CASCADE;
```
