{{ config(materialized='table') }}

SELECT
    f.asset_id,
    d.asset_name,
    d.asset_type,
    d.grid_zone,
    d.rated_capacity_mw AS active_capacity_mw,
    f.forecast_cycle,
    ROUND(AVG(f.wind_speed_100m_ms), 2) AS avg_wind_ms,
    ROUND(MAX(f.wind_speed_100m_ms), 2) AS peak_wind_ms,
    ROUND(MIN(f.surface_pressure_hpa), 1) AS min_pressure_hpa,
    ROUND(SUM(f.calibrated_generation_mw), 1) AS total_48h_generation_mwh,
    ROUND(SUM(f.hdd_hourly), 2) AS total_48h_hdd,
    SUM(CASE WHEN f.is_storm_cutout THEN 1 ELSE 0 END) AS cutout_hours,
    CASE
        WHEN SUM(CASE WHEN f.is_storm_cutout THEN 1 ELSE 0 END) > 0 THEN 'STORM_CUTOUT_ALERT'
        WHEN SUM(f.hdd_hourly) >= 14.0 THEN 'GAS_DEMAND_SURGE'
        WHEN AVG(f.capacity_factor_pct) >= 75.0 THEN 'OPTIMAL_WIND_DISPATCH'
        ELSE 'NOMINAL_GRID_STATE'
    END AS decision_signal
FROM {{ ref('fact_asset_weather_risk') }} f
JOIN {{ ref('dim_energy_assets') }} d
    ON f.asset_scd_sk = d.asset_scd_sk
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY total_48h_generation_mwh DESC, total_48h_hdd DESC
