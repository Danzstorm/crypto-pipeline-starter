"""
Capa Gold — vistas materializadas con KPIs.

Sin cambios respecto a v1 — Silver sigue exponiendo observed_at.
"""

from pyspark import pipelines as dp


@dp.materialized_view(
    name="gold.coin_momentum_24h",
    comment="Último estado de cada moneda con ranking de momentum.",
)
def coin_momentum_24h():
    return spark.sql(
        """
        WITH latest AS (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY observed_at DESC
                 ) AS rn
          FROM LIVE.silver.coin_prices
          WHERE observed_at >= current_timestamp() - INTERVAL 24 HOURS
        )
        SELECT symbol,
               name,
               price_usd,
               pct_change_24h,
               volume_24h_usd,
               market_cap_usd,
               RANK() OVER (ORDER BY pct_change_24h DESC) AS momentum_rank,
               observed_at
        FROM latest
        WHERE rn = 1
        """
    )


@dp.materialized_view(
    name="gold.coin_volatility_7d",
    comment="Volatilidad realizada anualizada en los últimos 7 días.",
)
def coin_volatility_7d():
    return spark.sql(
        """
        WITH daily_returns AS (
          SELECT symbol,
                 DATE(observed_at) AS day,
                 (MAX(price_usd) - MIN(price_usd)) / MIN(price_usd) AS daily_return
          FROM LIVE.silver.coin_prices
          WHERE observed_at >= current_timestamp() - INTERVAL 7 DAYS
          GROUP BY symbol, DATE(observed_at)
        )
        SELECT symbol,
               STDDEV(daily_return) * SQRT(365) AS realized_volatility_7d,
               COUNT(*) AS n_observations,
               MAX(day) AS last_observed_at
        FROM daily_returns
        GROUP BY symbol
        HAVING COUNT(*) >= 3
        """
    )
