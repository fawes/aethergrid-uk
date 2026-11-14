"""
AetherGrid UK - Day 3: Enterprise Integration, Transactional Outbox, Redpanda/Kafka Streaming & FastAPI
Demonstrates:
1. Transactional Outbox Pattern: Atomic persistence of Gold Mart signals into `enterprise_event_outbox`
2. Redpanda / Kafka Partitioned Event Router: Deterministic key hashing by `asset_id`, CloudEvents v1.0,
   Idempotency Keys, and Content-Based Routing to SAP & Salesforce topics
3. FastAPI Stateless Decision Microservice: REST endpoints serving live UK grid signals & hourly Iceberg/dbt data
4. Automated End-to-End API Verification via ASGI TestClient (httpx)
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field

DB_PATH = Path("data/iceberg_warehouse/aethergrid_analytics.duckdb").resolve()
BROKER_DIR = Path("data/redpanda_broker_logs").resolve()


class CloudEventEnvelope(BaseModel):
    specversion: str = "1.0"
    id: str
    source: str = "urn:aethergrid-uk:decision-engine"
    type: str
    subject: str
    time: str
    datacontenttype: str = "application/json"
    idempotency_key: str
    kafka_topic: str
    kafka_partition: int = Field(ge=0, le=2)
    data: dict


def deterministic_partition(asset_id: str, num_partitions: int = 3) -> int:
    """Guarantees strict chronological ordering per asset_id across broker partitions."""
    digest = hashlib.md5(asset_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % num_partitions


def populate_transactional_outbox(db_path: Path) -> int:
    """
    Atomically reads Day 2's dbt Gold Mart (`mart_uk_grid_trading_signals`) and inserts
    idempotent outbox records inside a single ACID transaction (eliminating Dual-Write bugs).
    """
    if not db_path.exists():
        raise FileNotFoundError("DuckDB analytics store not found! Run src/day2_dbt_modeling.py first.")

    con = duckdb.connect(str(db_path))
    con.execute("""
        CREATE TABLE IF NOT EXISTS enterprise_event_outbox (
            event_id VARCHAR PRIMARY KEY,
            idempotency_key VARCHAR UNIQUE,
            asset_id VARCHAR,
            target_topic VARCHAR,
            partition_id INTEGER,
            payload_json VARCHAR,
            status VARCHAR,
            created_at_utc TIMESTAMPTZ,
            published_at_utc TIMESTAMPTZ
        );
    """)

    signals_df = con.execute("SELECT * FROM mart_uk_grid_trading_signals").df()
    inserted = 0

    con.execute("BEGIN TRANSACTION;")
    try:
        for _, row in signals_df.iterrows():
            asset_id = row["asset_id"]
            cycle = row["forecast_cycle"]
            signal = row["decision_signal"]

            if signal == "STORM_CUTOUT_ALERT" or row["asset_type"] == "OFFSHORE_WIND":
                topic = "sap.pm.uk_offshore_asset_operations"
                event_type = "uk.co.aethergrid.sap.asset.dispatch"
            else:
                topic = "sfdc.trading.uk_power_gas_signals"
                event_type = "uk.co.aethergrid.trading.demand_signal"

            idem_key = hashlib.sha256(f"{asset_id}:{cycle}:{signal}".encode("utf-8")).hexdigest()[:24]
            part_id = deterministic_partition(asset_id, num_partitions=3)
            now_iso = datetime.now(timezone.utc).isoformat()

            envelope = CloudEventEnvelope(
                id=f"evt-{idem_key[:12]}",
                type=event_type,
                subject=asset_id,
                time=now_iso,
                idempotency_key=idem_key,
                kafka_topic=topic,
                kafka_partition=part_id,
                data={
                    "asset_id": asset_id,
                    "asset_name": row["asset_name"],
                    "asset_type": row["asset_type"],
                    "grid_zone": row["grid_zone"],
                    "active_capacity_mw": float(row["active_capacity_mw"]),
                    "forecast_cycle": cycle,
                    "peak_wind_ms": float(row["peak_wind_ms"]),
                    "total_48h_generation_mwh": float(row["total_48h_generation_mwh"]),
                    "total_48h_hdd": float(row["total_48h_hdd"]),
                    "decision_signal": signal,
                },
            )

            exists = con.execute(
                "SELECT COUNT(*) FROM enterprise_event_outbox WHERE idempotency_key = ?", [idem_key]
            ).fetchone()[0]

            if exists == 0:
                con.execute(
                    """
                    INSERT INTO enterprise_event_outbox
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', CURRENT_TIMESTAMP, NULL)
                    """,
                    [envelope.id, idem_key, asset_id, topic, part_id, envelope.model_dump_json()],
                )
                inserted += 1

        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()

    return inserted


def relay_outbox_to_redpanda_logs(db_path: Path, broker_dir: Path) -> list[dict]:
    """
    Polls PENDING records from `enterprise_event_outbox`, appends them to partitioned
    Redpanda/Kafka commit-log segments on disk, and marks them PUBLISHED.
    """
    broker_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    pending = con.execute(
        "SELECT event_id, target_topic, partition_id, payload_json FROM enterprise_event_outbox WHERE status = 'PENDING'"
    ).fetchall()

    for event_id, topic, part_id, payload_json in pending:
        part_dir = broker_dir / f"{topic}-{part_id}"
        part_dir.mkdir(parents=True, exist_ok=True)
        segment_file = part_dir / "00000000000000000000.log"

        offset = len(segment_file.read_text().splitlines()) if segment_file.exists() else 0
        record = {"offset": offset, "cloudevent": json.loads(payload_json)}

        with open(segment_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        con.execute(
            "UPDATE enterprise_event_outbox SET status = 'PUBLISHED', published_at_utc = CURRENT_TIMESTAMP WHERE event_id = ?",
            [event_id],
        )

    all_events = [
        json.loads(r[0])
        for r in con.execute("SELECT payload_json FROM enterprise_event_outbox ORDER BY asset_id").fetchall()
    ]
    con.close()
    return all_events


app = FastAPI(
    title="AetherGrid UK — Climate & Energy Decision API",
    description="Stateless FastAPI service exposing dbt Gold Mart signals, SCD Type-2 asset telemetry & Outbox CloudEvents.",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {"status": "HEALTHY", "service": "aethergrid-uk-decision-api", "lakehouse_connected": DB_PATH.exists()}


@app.get("/api/v1/signals")
def get_all_trading_signals():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("SELECT * FROM mart_uk_grid_trading_signals").df()
    con.close()
    return {"count": len(df), "signals": df.to_dict(orient="records")}


@app.get("/api/v1/assets/{asset_id}/forecast")
def get_asset_hourly_forecast(asset_id: str, limit: int = 12):
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(
        """
        SELECT asset_id, forecast_cycle, valid_time_utc, temperature_2m_c,
               wind_speed_100m_ms, rated_capacity_mw, calibrated_generation_mw, capacity_factor_pct, hdd_hourly
        FROM fact_asset_weather_risk
        WHERE asset_id = ?
        ORDER BY valid_time_utc ASC
        LIMIT ?
        """,
        [asset_id, limit],
    ).df()
    con.close()

    if df.empty:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found in Lakehouse.")

    df["valid_time_utc"] = df["valid_time_utc"].astype(str)
    return {"asset_id": asset_id, "hours_returned": len(df), "hourly_forecast": df.to_dict(orient="records")}


@app.get("/api/v1/outbox/events")
def get_outbox_audit_trail():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(
        "SELECT event_id, idempotency_key, asset_id, target_topic, partition_id, status FROM enterprise_event_outbox ORDER BY asset_id"
    ).df()
    con.close()
    return {"total_events": len(df), "outbox_records": df.to_dict(orient="records")}


def main() -> None:
    print(">>> Step 1: Writing Gold Mart Decisions to Transactional Outbox...")
    newly_queued = populate_transactional_outbox(DB_PATH)
    print(f"    Queued {newly_queued} new idempotent events in `enterprise_event_outbox`.")

    print("\n>>> Step 2: Relaying Outbox Events to Redpanda/Kafka Partitioned Commit Logs...")
    events = relay_outbox_to_redpanda_logs(DB_PATH, BROKER_DIR)
    for evt in events:
        print(
            f"    [Topic: {evt['kafka_topic']} | Partition {evt['kafka_partition']}] "
            f"Key={evt['subject']} | Signal={evt['data']['decision_signal']} | IdemKey={evt['idempotency_key']}"
        )

    print("\n>>> Step 3: Verifying Live FastAPI Endpoints via ASGI TestClient...")
    client = TestClient(app)

    r_health = client.get("/health")
    assert r_health.status_code == 200
    print(f"    GET /health                              -> {r_health.status_code} {r_health.json()}")

    r_signals = client.get("/api/v1/signals")
    assert r_signals.status_code == 200
    print(f"    GET /api/v1/signals                      -> {r_signals.status_code} ({r_signals.json()['count']} asset signals)")

    r_dogger = client.get("/api/v1/assets/UK-WIND-DOGGER/forecast?limit=3")
    assert r_dogger.status_code == 200
    sample_hr = r_dogger.json()["hourly_forecast"][0]
    print(
        f"    GET /api/v1/assets/UK-WIND-DOGGER/...    -> {r_dogger.status_code} "
        f"(Active Capacity={sample_hr['rated_capacity_mw']} MW, Calibrated Gen={sample_hr['calibrated_generation_mw']} MW)"
    )

    r_outbox = client.get("/api/v1/outbox/events")
    assert r_outbox.status_code == 200
    print(f"    GET /api/v1/outbox/events                -> {r_outbox.status_code} ({r_outbox.json()['total_events']} PUBLISHED events)")


if __name__ == "__main__":
    main()