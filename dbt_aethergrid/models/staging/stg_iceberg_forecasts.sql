{{ config(materialized='view') }}

SELECT
    md5(asset_id || '|' || CAST(valid_time_utc AS VARCHAR) || '|' || forecast_cycle) AS forecast_fact_sk,
    forecast_cycle,
    ingested_at_utc,
    valid_time_utc,
    asset_id,
    asset_name,
    asset_type,
    region,
    temperature_2m_c,
    wind_speed_100m_ms,
    surface_pressure_hpa,
    predicted_generation_mw AS raw_generation_mw,
    hdd_hourly
FROM iceberg_scan('/home/kaleem/projects/aethergrid-uk/data/iceberg_warehouse/energy_lakehouse/uk_asset_forecasts/metadata/00002-b66bb356-f235-4f3f-86b0-09f86eb553e7.metadata.json')
