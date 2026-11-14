"""
AetherGrid UK - Day 1: Real-World Weather Ingestion to Apache Iceberg Lakehouse
Demonstrates:
1. Live UK Meteorological Acquisition (ECMWF/Open-Meteo API with resilient fallback)
2. Domain Engineering: Wind Turbine Power Curves (MW) & UK Heating Degree Days (HDD)
3. True Apache Iceberg v2 Table Creation, Hidden Partitioning & Atomic Snapshots
4. Physical Inspection of Iceberg Metadata Tree (metadata.json -> Manifests -> Parquet)
5. Serverless SQL Time-Travel Delta Analysis (00z vs. 12z Forecast Shift) via DuckDB
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import requests
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
    TimestamptzType,
)

# 1. Define Strategic UK Energy & Grid Portfolio
UK_ENERGY_ASSETS = [
    {"asset_id": "UK-WIND-DOGGER", "asset_name": "Dogger Bank Wind Farm", "asset_type": "OFFSHORE_WIND", "region": "North_Sea", "lat": 54.75, "lon": 1.90, "capacity_mw": 1200.0},
    {"asset_id": "UK-WIND-HORNSEA", "asset_name": "Hornsea Two Offshore", "asset_type": "OFFSHORE_WIND", "region": "North_Sea", "lat": 53.91, "lon": 1.79, "capacity_mw": 1320.0},
    {"asset_id": "UK-WIND-MORAY", "asset_name": "Moray East Offshore", "asset_type": "OFFSHORE_WIND", "region": "North_Sea", "lat": 58.10, "lon": -2.72, "capacity_mw": 950.0},
    {"asset_id": "UK-SOLAR-SHOTWICK", "asset_name": "Shotwick Solar Park", "asset_type": "UTILITY_SOLAR", "region": "Wales_NW", "lat": 53.23, "lon": -3.01, "capacity_mw": 72.0},
    {"asset_id": "UK-GAS-BACTON", "asset_name": "Bacton Gas Interconnector", "asset_type": "GAS_TERMINAL", "region": "East_England", "lat": 52.85, "lon": 1.46, "capacity_mw": 0.0},
    {"asset_id": "UK-GRID-LONDON", "asset_name": "National Grid London Hub", "asset_type": "DEMAND_HUB", "region": "Greater_London", "lat": 51.51, "lon": -0.12, "capacity_mw": 0.0},
]


def fetch_uk_weather_forecast(cycle_tag: str, storm_perturbation: bool = False) -> pd.DataFrame:
    """Fetches real 48-hour hourly forecast for UK energy assets, applying commercial physics."""
    records = []
    base_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    for asset in UK_ENERGY_ASSETS:
        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast?"
                f"latitude={asset['lat']}&longitude={asset['lon']}"
                f"&hourly=temperature_2m,wind_speed_100m,surface_pressure,shortwave_radiation"
                f"&wind_speed_unit=ms&forecast_days=2&timezone=UTC"
            )
            resp = requests.get(url, timeout=8)
            resp.raise_for_status()
            hourly = resp.json()["hourly"]
            times = pd.to_datetime(hourly["time"], utc=True)
            temps = np.array(hourly["temperature_2m"], dtype=float)
            winds = np.array(hourly["wind_speed_100m"], dtype=float)
            pressures = np.array(hourly["surface_pressure"], dtype=float)
            solar_wm2 = np.array(hourly["shortwave_radiation"], dtype=float)
        except Exception:
            # Resilient physics-based fallback if offline
            times = pd.date_range(base_time, periods=48, freq="1h", tz="UTC")
            hours = np.arange(48)
            temps = 7.5 + 3.0 * np.sin(2 * np.pi * (hours - 6) / 24) - (asset["lat"] - 51.5) * 0.6
            winds = 11.5 + 4.5 * np.sin(2 * np.pi * hours / 36) + (1.8 if asset["asset_type"] == "OFFSHORE_WIND" else -3.0)
            pressures = 1008.0 - 8.0 * np.sin(2 * np.pi * hours / 48)
            solar_wm2 = np.maximum(0.0, 420.0 * np.sin(np.pi * ((hours % 24) - 6) / 12))

        # Simulate a realistic 12z ECMWF mid-day North Sea storm + cold snap revision
        if storm_perturbation:
            if asset["region"] == "North_Sea":
                winds = winds * 1.38  # Strong North Sea gale intensification (triggers turbine cut-out risk!)
                pressures = pressures - 14.5
            temps = temps - 2.6  # Colder continental air mass -> spikes UK Heating Degree Days

        for t, temp_c, wind_ms, hpa, rad in zip(times, temps, winds, pressures, solar_wm2):
            # Commercial Domain Translation:
            # 1. Offshore Wind Power Curve (Cut-in 3 m/s, Rated 13 m/s, Storm Cut-out > 25 m/s!)
            if asset["asset_type"] == "OFFSHORE_WIND":
                if wind_ms < 3.0 or wind_ms > 25.0:
                    gen_mw = 0.0  # Safety storm shut-down!
                elif wind_ms >= 13.0:
                    gen_mw = asset["capacity_mw"]
                else:
                    gen_mw = asset["capacity_mw"] * ((wind_ms - 3.0) / 10.0) ** 3
            elif asset["asset_type"] == "UTILITY_SOLAR":
                gen_mw = min(asset["capacity_mw"], asset["capacity_mw"] * (rad / 850.0))
            else:
                gen_mw = 0.0

            # 2. UK Gas Industry Standard Heating Degree Days (Base 15.5 C)
            hdd_hourly = max(0.0, 15.5 - float(temp_c)) / 24.0

            records.append({
                "forecast_cycle": cycle_tag,
                "ingested_at_utc": datetime.now(timezone.utc),
                "valid_time_utc": t.to_pydatetime(),
                "asset_id": asset["asset_id"],
                "asset_name": asset["asset_name"],
                "asset_type": asset["asset_type"],
                "region": asset["region"],
                "temperature_2m_c": round(float(temp_c), 2),
                "wind_speed_100m_ms": round(float(wind_ms), 2),
                "surface_pressure_hpa": round(float(hpa), 2),
                "predicted_generation_mw": round(float(gen_mw), 2),
                "hdd_hourly": round(float(hdd_hourly), 4),
            })

    return pd.DataFrame(records)


def main() -> None:
    warehouse_dir = Path("data/iceberg_warehouse").resolve()
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    catalog_db = warehouse_dir / "iceberg_catalog.db"
    if catalog_db.exists():
        catalog_db.unlink()

    # 2. Initialize PyIceberg SQL Catalog (Local equivalent of AWS Glue / BigLake Metastore)
    catalog = SqlCatalog(
        "aethergrid",
        **{
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": f"file://{warehouse_dir}",
        },
    )
    catalog.create_namespace_if_not_exists("energy_lakehouse")

    # 3. Define Explicit Iceberg Schema & Hidden Partitioning
    schema = Schema(
        NestedField(1, "forecast_cycle", StringType(), required=True),
        NestedField(2, "ingested_at_utc", TimestamptzType(), required=True),
        NestedField(3, "valid_time_utc", TimestamptzType(), required=True),
        NestedField(4, "asset_id", StringType(), required=True),
        NestedField(5, "asset_name", StringType(), required=True),
        NestedField(6, "asset_type", StringType(), required=True),
        NestedField(7, "region", StringType(), required=True),
        NestedField(8, "temperature_2m_c", DoubleType(), required=True),
        NestedField(9, "wind_speed_100m_ms", DoubleType(), required=True),
        NestedField(10, "surface_pressure_hpa", DoubleType(), required=True),
        NestedField(11, "predicted_generation_mw", DoubleType(), required=True),
        NestedField(12, "hdd_hourly", DoubleType(), required=True),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="valid_day"),
        PartitionField(source_id=6, field_id=1001, transform=IdentityTransform(), name="asset_type"),
    )

    table_identifier = "energy_lakehouse.uk_asset_forecasts"
    if catalog.table_exists(table_identifier):
        catalog.drop_table(table_identifier)

    iceberg_table = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
        properties={"write.format.default": "parquet", "write.parquet.compression-codec": "zstd"},
    )

    arrow_schema = iceberg_table.schema().as_arrow()

    # 4. Commit Snapshot 1: 00z Morning Forecast Run
    print(">>> Ingesting 00z Morning ECMWF/UK Forecast Cycle...")
    df_00z = fetch_uk_weather_forecast(cycle_tag="2026-11-13T00Z", storm_perturbation=False)
    iceberg_table.overwrite(pa.Table.from_pandas(df_00z, schema=arrow_schema, preserve_index=False))
    snap_00z = iceberg_table.current_snapshot()
    print(f"    Committed Snapshot 1 (00z) | Snapshot ID: {snap_00z.snapshot_id}")

    # 5. Commit Snapshot 2: 12z Mid-Day Storm & Cold Snap Revision (Atomic Overwrite)
    print(">>> Ingesting 12z Mid-Day Storm Revision Cycle...")
    df_12z = fetch_uk_weather_forecast(cycle_tag="2026-11-13T12Z", storm_perturbation=True)
    iceberg_table.overwrite(pa.Table.from_pandas(df_12z, schema=arrow_schema, preserve_index=False))
    snap_12z = iceberg_table.current_snapshot()
    print(f"    Committed Snapshot 2 (12z) | Snapshot ID: {snap_12z.snapshot_id}")

    # 6. Inspect the Physical Apache Iceberg Metadata Tree
    meta_loc = iceberg_table.metadata_location.replace("file://", "")
    with open(meta_loc, "r", encoding="utf-8") as f:
        meta_json = json.load(f)

    print("\n=== APACHE ICEBERG METADATA TREE INSPECTION ===")
    print(f"Format Version     : v{meta_json['format-version']}")
    print(f"Metadata File      : {Path(meta_loc).name}")
    print(f"Current Snapshot   : {meta_json['current-snapshot-id']}")
    print(f"Total Snapshots    : {len(meta_json['snapshots'])}")
    print(f"Manifest List Avro : {Path(snap_12z.manifest_list).name}")

    # 7. Run Serverless DuckDB SQL Time-Travel Comparison (00z vs. 12z)
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")

    # Locate the exact metadata.json file whose current-snapshot-id matches snap_00z
    prev_meta_file = None
    for mf in sorted(Path(meta_loc).parent.glob("*.metadata.json")):
        if json.loads(mf.read_text()).get("current-snapshot-id") == snap_00z.snapshot_id:
            prev_meta_file = mf
            break

    query = f"""
    WITH run_00z AS (
        SELECT
            asset_id,
            asset_name,
            asset_type,
            ROUND(AVG(wind_speed_100m_ms), 2) AS wind_00z_ms,
            ROUND(SUM(predicted_generation_mw), 1) AS gen_00z_mwh,
            ROUND(SUM(hdd_hourly), 2) AS hdd_00z
        FROM iceberg_scan('{prev_meta_file}')
        GROUP BY 1, 2, 3
    ),
    run_12z AS (
        SELECT
            asset_id,
            ROUND(AVG(wind_speed_100m_ms), 2) AS wind_12z_ms,
            ROUND(MAX(wind_speed_100m_ms), 2) AS peak_wind_12z_ms,
            ROUND(SUM(predicted_generation_mw), 1) AS gen_12z_mwh,
            ROUND(SUM(hdd_hourly), 2) AS hdd_12z
        FROM iceberg_scan('{meta_loc}')
        GROUP BY 1
    )
    SELECT
        a.asset_id,
        a.asset_type,
        a.wind_00z_ms,
        b.wind_12z_ms,
        b.peak_wind_12z_ms,
        ROUND(b.gen_12z_mwh - a.gen_00z_mwh, 1) AS delta_generation_mwh,
        ROUND(b.hdd_12z - a.hdd_00z, 2) AS delta_hdd,
        CASE
            WHEN b.peak_wind_12z_ms > 25.0 THEN 'STORM_CUTOUT_RISK'
            WHEN (b.hdd_12z - a.hdd_00z) > 0.20 THEN 'GAS_DEMAND_SPIKE'
            ELSE 'NOMINAL'
        END AS trading_desk_action
    FROM run_00z a
    JOIN run_12z b ON a.asset_id = b.asset_id
    ORDER BY ABS(b.gen_12z_mwh - a.gen_00z_mwh) DESC, delta_hdd DESC;
    """

    result_df = con.execute(query).df()
    print("\n=== DUCKDB TIME-TRAVEL DELTA ANALYSIS (00z vs. 12z REVISION) ===")
    print(result_df.to_string(index=False))


if __name__ == "__main__":
    main()