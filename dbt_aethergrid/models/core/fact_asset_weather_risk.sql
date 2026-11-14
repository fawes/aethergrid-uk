{{
    config(
        materialized='incremental',
        unique_key='forecast_fact_sk',
        incremental_strategy='merge'
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_iceberg_forecasts') }}
),
dim AS (
    SELECT * FROM {{ ref('dim_energy_assets') }}
)

SELECT
    s.forecast_fact_sk,
    d.asset_scd_sk,
    s.asset_id,
    s.forecast_cycle,
    s.valid_time_utc,
    s.temperature_2m_c,
    s.wind_speed_100m_ms,
    s.surface_pressure_hpa,
    d.rated_capacity_mw,
    -- Recalculate generation using point-in-time SCD Type-2 rated capacity!
    CASE
        WHEN d.asset_type = 'OFFSHORE_WIND' THEN
            CASE
                WHEN s.wind_speed_100m_ms < 3.0 OR s.wind_speed_100m_ms > 25.0 THEN 0.0
                WHEN s.wind_speed_100m_ms >= 13.0 THEN d.rated_capacity_mw
                ELSE ROUND(d.rated_capacity_mw * POWER((s.wind_speed_100m_ms - 3.0) / 10.0, 3), 2)
            END
        ELSE s.raw_generation_mw
    END AS calibrated_generation_mw,
    CASE
        WHEN d.rated_capacity_mw > 0 THEN
            ROUND((
                CASE
                    WHEN d.asset_type = 'OFFSHORE_WIND' AND s.wind_speed_100m_ms >= 13.0 AND s.wind_speed_100m_ms <= 25.0
                        THEN d.rated_capacity_mw
                    ELSE s.raw_generation_mw
                END / d.rated_capacity_mw
            ) * 100.0, 1)
        ELSE 0.0
    END AS capacity_factor_pct,
    (s.wind_speed_100m_ms > 25.0) AS is_storm_cutout,
    s.hdd_hourly,
    s.ingested_at_utc
FROM stg s
INNER JOIN dim d
    ON s.asset_id = d.asset_id
   AND s.valid_time_utc >= d.valid_from_utc
   AND s.valid_time_utc < d.valid_to_utc

{% if is_incremental() %}
WHERE s.ingested_at_utc >= (SELECT MAX(ingested_at_utc) FROM {{ this }})
{% endif %}
