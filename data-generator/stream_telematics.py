"""
Telematics streaming simulator.
Posts GPS/temperature pings to a Fabric Eventstream Custom Endpoint (Event Hub compatible).

Setup in Fabric:
  1. Create Eventstream `es_telematics`, add a Custom Endpoint source.
  2. Copy the Event Hub connection string and set it below or via env var.
  3. pip install azure-eventhub

Event shape (one ping every ~30s per active truck):
  truck_id, carrier_id, shipment_id, ts, lat, lon, speed_kph,
  temp_c (reefer only), fuel_pct, event_type

Anomalies injected for Activator to catch:
  - Reefer temp excursions above 8C  (~2% of reefer pings)
  - Route deviation flag             (~1% of pings)
  - Harsh braking events             (~0.5%)
"""

import json
import os
import time

import numpy as np
from azure.eventhub import EventData, EventHubProducerClient

CONN_STR = os.environ.get("EVENTSTREAM_CONN_STR", "<paste-custom-endpoint-connection-string>")
EVENTHUB_NAME = os.environ.get("EVENTSTREAM_HUB", "es-telematics")

RNG = np.random.default_rng()
N_TRUCKS = 60
BATCH_INTERVAL_SEC = 5          # send a batch every 5s; ~12 pings/truck/min compressed for demo
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "45"))

trucks = [{
    "truck_id": f"TRK{str(i+1).zfill(4)}",
    "carrier_id": f"CAR{str(RNG.integers(1, 41)).zfill(3)}",
    "shipment_id": f"SHP{str(RNG.integers(1, 1_200_000)).zfill(8)}",
    "is_reefer": bool(RNG.uniform() < 0.2),
    "lat": float(RNG.uniform(48.0, 53.6)),
    "lon": float(RNG.uniform(2.3, 12.0)),
    "temp": float(RNG.uniform(2.0, 5.0)),
} for i in range(N_TRUCKS)]


def next_ping(t):
    t["lat"] += float(RNG.normal(0, 0.01))
    t["lon"] += float(RNG.normal(0, 0.012))
    speed = max(0.0, float(RNG.normal(78, 14)))
    event_type = "ping"
    roll = RNG.uniform()
    if roll < 0.005:
        event_type = "harsh_brake"
        speed = float(RNG.uniform(15, 40))
    elif roll < 0.015:
        event_type = "route_deviation"
    if t["is_reefer"]:
        # Slow drift plus occasional excursion
        t["temp"] += float(RNG.normal(0, 0.15))
        if RNG.uniform() < 0.02:
            t["temp"] = float(RNG.uniform(8.5, 13.0))
        t["temp"] = max(-2.0, min(t["temp"], 15.0))
    return {
        "truck_id": t["truck_id"], "carrier_id": t["carrier_id"], "shipment_id": t["shipment_id"],
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "lat": round(t["lat"], 5), "lon": round(t["lon"], 5),
        "speed_kph": round(speed, 1),
        "temp_c": round(t["temp"], 2) if t["is_reefer"] else None,
        "fuel_pct": round(float(RNG.uniform(15, 95)), 1),
        "event_type": event_type,
    }


def main():
    producer = EventHubProducerClient.from_connection_string(CONN_STR, eventhub_name=EVENTHUB_NAME)
    deadline = time.time() + RUN_MINUTES * 60
    sent = 0
    with producer:
        while time.time() < deadline:
            batch = producer.create_batch()
            for t in trucks:
                batch.add(EventData(json.dumps(next_ping(t))))
            producer.send_batch(batch)
            sent += len(trucks)
            print(f"sent {sent:,} events", end="\r")
            time.sleep(BATCH_INTERVAL_SEC)
    print(f"\nDone. {sent:,} events over {RUN_MINUTES} minutes.")


if __name__ == "__main__":
    main()
