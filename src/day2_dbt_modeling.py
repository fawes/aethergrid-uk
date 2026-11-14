"""
AetherGrid UK - Day 2: Enterprise Data Modeling & DataOps Governance (dbt Core + DuckDB + Iceberg)
Demonstrates:
1. Direct Lakehouse Integration: dbt reading Day 1 Apache Iceberg metadata via DuckDB iceberg_scan()
2. Kimball Star Schema: dim_energy_assets (SCD Type-2) + fact_asset_weather_risk (Incremental MERGE)
3. Gold Consumption Mart: mart_uk_grid_trading_signals (Ready for Day 3 Kafka/FastAPI Outbox)
4. DataOps Circuit-Breaker Tests: Schema contracts, referential integrity & physical bounds assertions
"""

import json
import subprocess
import sys
from pathlib import Path
import duckdb


def get_latest_iceberg_metadata_path() -> Path:
    meta_dir = Path("data/iceberg_warehouse/energy_lakehouse/uk_asset_forecasts/metadata").resolve()
    meta_files = sorted(meta_dir.glob("*.metadata.json"))
    if not meta_files:
        raise FileNotFoundError("No Apache Iceberg metadata.json found! Run src/day1_iceberg_ingestion.py first.")
    return meta_files[-1]


def seed_scd2_asset_registry(db_path: Path) -> None:
    """Seeds raw asset master data including an SCD Type-2 capacity expansion for Dogger Bank."""
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_enterprise;")
    con.execute("""
        CREATE OR REPLACE TABLE raw_enterprise.asset_master_history (
            asset_id VARCHAR,
            asset_name VARCHAR,
            asset_type VARCHAR,
            region VARCHAR,
            grid_zone VARCHAR,
            rated_capacity_mw DOUBLE,
            valid_from_utc TIMESTAMPTZ,
            valid_to_utc TIMESTAMPTZ,
            is_current BOOLEAN
        );
    """)
    con.execute("""
        INSERT INTO raw_enterprise.asset_master_history VALUES
        -- SCD Type-2 Historical Record (Phase A capacity before expansion)
        ('UK-WIND-DOGGER', 'Dogger Bank Wind Farm', 'OFFSHORE_WIND', 'North_Sea', 'UK-N-01', 1200.0,
         TIMESTAMPTZ '2024-01-01 00:00:00+00', TIMESTAMPTZ '2026-06-01 00:00:00+00', FALSE),
        -- SCD Type-2 Current Record (Phase A + B commissioned capacity = 2400 MW)
        ('UK-WIND-DOGGER', 'Dogger Bank Wind Farm', 'OFFSHORE_WIND', 'North_Sea', 'UK-N-01', 2400.0,
         TIMESTAMPTZ '2026-06-01 00:00:00+00', TIMESTAMPTZ '2099-12-31 23:59:59+00', TRUE),

        ('UK-WIND-HORNSEA', 'Hornsea Two Offshore', 'OFFSHORE_WIND', 'North_Sea', 'UK-N-02', 1320.0,
         TIMESTAMPTZ '2022-08-01 00:00:00+00', TIMESTAMPTZ '2099-12-31 23:59:59+00', TRUE),
        ('UK-WIND-MORAY', 'Moray East Offshore', 'OFFSHORE_WIND', 'North_Sea', 'UK-SCOT-01', 950.0,
         TIMESTAMPTZ '2022-04-01 00:00:00+00', TIMESTAMPTZ '2099-12-31 23:59:59+00', TRUE),
        ('UK-SOLAR-SHOTWICK', 'Shotwick Solar Park', 'UTILITY_SOLAR', 'Wales_NW', 'UK-WALES-01', 72.0,
         TIMESTAMPTZ '2016-05-01 00:00:00+00', TIMESTAMPTZ '2099-12-31 23:59:59+00', TRUE),
        ('UK-GAS-BACTON', 'Bacton Gas Interconnector', 'GAS_TERMINAL', 'East_England', 'UK-EAST-01', 0.0,
         TIMESTAMPTZ '2010-01-01 00:00:00+00', TIMESTAMPTZ '2099-12-31 23:59:59+00', TRUE),
        ('UK-GRID-LONDON', 'National Grid London Hub', 'DEMAND_HUB', 'Greater_London', 'UK-LON-01', 0.0,
         TIMESTAMPTZ '2010-01-01 00:00:00+00', TIMESTAMPTZ '2099-12-31 23:59:59+00', TRUE);
    """)
    con.close()


def scaffold_dbt_project(dbt_dir: Path, db_path: Path, iceberg_meta_path: Path) -> None:
    """Generates a complete dbt Core project with Staging, SCD2 Dimension, Incremental Fact, Mart & DataOps Tests."""
    for sub in ["models/staging", "models/core", "models/marts", "tests"]:
        (dbt_dir / sub).mkdir(parents=True, exist_ok=True)

    # 1. dbt_project.yml
    (dbt_dir / "dbt_project.yml").write_text("""name: 'aethergrid_uk'
version: '1.0.0'
config-version: 2

profile: 'aethergrid_profile'

model-paths: ["models"]
test-paths: ["tests"]

models:
  aethergrid_uk:
    staging:
      +materialized: view
    core:
      +materialized: table
    marts:
      +materialized: table
""", encoding="utf-8")

    # 2. profiles.yml (configured with Iceberg extension)
    (dbt_dir / "profiles.yml").write_text(f"""aethergrid_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '{db_path.resolve()}'
      extensions:
        - iceberg
      threads: 4
""", encoding="utf-8")

    # 3. Staging Model: Reads directly from Apache Iceberg metadata.json
    (dbt_dir / "models/staging/stg_iceberg_forecasts.sql").write_text(f"""{{{{ config(materialized='view') }}}}

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
FROM iceberg_scan('{iceberg_meta_path}')
""", encoding="utf-8")

    # 4. Core Dimension: SCD Type-2 Energy Assets Dimension
    (dbt_dir / "models/core/dim_energy_assets.sql").write_text("""{{ config(materialized='table') }}

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
""", encoding="utf-8")

    # 5. Core Fact Table: Incremental MERGE joining Iceberg forecasts to SCD Type-2 Dimension
    (dbt_dir / "models/core/fact_asset_weather_risk.sql").write_text("""{{
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
""", encoding="utf-8")

    # 6. Gold Consumption Mart: Trading & Grid Operations Signals
    (dbt_dir / "models/marts/mart_uk_grid_trading_signals.sql").write_text("""{{ config(materialized='table') }}

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
""", encoding="utf-8")

    # 7. DataOps Schema Contracts & Referential Integrity Tests (schema.yml)
    (dbt_dir / "models/schema.yml").write_text("""version: 2

models:
  - name: dim_energy_assets
    description: "SCD Type-2 Dimension table tracking UK energy & grid asset capacity history."
    columns:
      - name: asset_scd_sk
        tests: [unique, not_null]
      - name: asset_id
        tests: [not_null]
      - name: asset_type
        tests:
          - accepted_values:
              arguments:
                values: ['OFFSHORE_WIND', 'UTILITY_SOLAR', 'GAS_TERMINAL', 'DEMAND_HUB']

  - name: fact_asset_weather_risk
    description: "Incremental Fact table at grain (asset_id, valid_time_utc, forecast_cycle)."
    columns:
      - name: forecast_fact_sk
        tests: [unique, not_null]
      - name: asset_scd_sk
        tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_energy_assets')
                field: asset_scd_sk

  - name: mart_uk_grid_trading_signals
    description: "Gold Decision Mart powering downstream Kafka CloudEvents & FastAPI OGC endpoints."
    columns:
      - name: asset_id
        tests: [unique, not_null]
      - name: decision_signal
        tests:
          - accepted_values:
              arguments:
                values: ['STORM_CUTOUT_ALERT', 'GAS_DEMAND_SURGE', 'OPTIMAL_WIND_DISPATCH', 'NOMINAL_GRID_STATE']
""", encoding="utf-8")

    # 8. Singular DataOps Circuit-Breaker Test: Physical Meteorological Bounds
    (dbt_dir / "tests/assert_meteorological_bounds.sql").write_text("""-- DataOps Circuit Breaker: Halts pipeline if physical atmospheric bounds are breached
SELECT *
FROM {{ ref('fact_asset_weather_risk') }}
WHERE temperature_2m_c < -45.0
   OR temperature_2m_c > 45.0
   OR wind_speed_100m_ms < 0.0
   OR wind_speed_100m_ms > 75.0
   OR surface_pressure_hpa < 870.0
   OR surface_pressure_hpa > 1085.0
   OR calibrated_generation_mw < 0.0
""", encoding="utf-8")


def main() -> None:
    dbt_dir = Path("dbt_aethergrid").resolve()
    db_path = Path("data/iceberg_warehouse/aethergrid_analytics.duckdb").resolve()

    iceberg_meta = get_latest_iceberg_metadata_path()
    print(f">>> Linking dbt Core to Apache Iceberg Metadata: {iceberg_meta.name}")

    seed_scd2_asset_registry(db_path)
    scaffold_dbt_project(dbt_dir, db_path, iceberg_meta)

    dbt_bin = str(Path(sys.executable).parent / "dbt")

    print("\n>>> Executing `dbt run` (Staging -> SCD2 Dimension -> Incremental Fact -> Gold Mart)...")
    subprocess.run([dbt_bin, "run", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)], check=True)

    print("\n>>> Executing `dbt test` (DataOps Circuit-Breaker Quality Contracts)...")
    subprocess.run([dbt_bin, "test", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)], check=True)

    # Query and display the resulting Kimball Star Schema & Gold Mart
    con = duckdb.connect(str(db_path))
    print("\n=== SCD TYPE-2 DIMENSION SAMPLE (dim_energy_assets — Dogger Bank Capacity History) ===")
    print(
        con.execute("""
            SELECT asset_id, rated_capacity_mw, valid_from_utc, valid_to_utc, is_current
            FROM dim_energy_assets
            WHERE asset_id = 'UK-WIND-DOGGER'
            ORDER BY valid_from_utc
        """).df().to_string(index=False)
    )

    print("\n=== GOLD CONSUMPTION MART (mart_uk_grid_trading_signals) ===")
    print(con.execute("SELECT * FROM mart_uk_grid_trading_signals").df().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()