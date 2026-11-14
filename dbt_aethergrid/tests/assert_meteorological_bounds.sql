-- DataOps Circuit Breaker: Halts pipeline if physical atmospheric bounds are breached
SELECT *
FROM {{ ref('fact_asset_weather_risk') }}
WHERE temperature_2m_c < -45.0
   OR temperature_2m_c > 45.0
   OR wind_speed_100m_ms < 0.0
   OR wind_speed_100m_ms > 75.0
   OR surface_pressure_hpa < 870.0
   OR surface_pressure_hpa > 1085.0
   OR calibrated_generation_mw < 0.0
