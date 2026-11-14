{{ config(materialized='table') }}

SELECT
    md5(asset_id || '|' || CAST(valid_from_utc AS VARCHAR)) AS asset_scd_sk,
    asset_id,
    asset_name,
    asset_type,
    region,
    grid_zone,
    rated_capacity_mw,
    valid_from_utc,
    valid_to_utc,
    is_current
FROM raw_enterprise.asset_master_history
