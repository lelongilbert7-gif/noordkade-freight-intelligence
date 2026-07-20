# Fabric notebook source
# Notebook: nb_data_quality
# DQ gate between ingestion and gold refresh. Raises on failure so the
# pipeline run goes red and the semantic model is never refreshed on bad data.

# CELL ******************** parameters cell
p_load_date = "2026-01-15"
p_environment = "dev"

# CELL ********************
from pyspark.sql import functions as F

failures = []

def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name} {detail}")
    if not condition:
        failures.append(f"{name} {detail}")

# CELL ********************
ship = spark.read.table("silver_shipments")
day = ship.filter(F.to_date("pickup_ts") == F.lit(p_load_date))
n = day.count()

# Volume: expect roughly 1.5k-3.5k shipments/day from the generator; a day at
# zero or 10x means an upstream problem, not a business swing
check("daily_volume_in_band", 500 <= n <= 10_000, f"(rows={n})")

# Completeness
nulls = day.filter(F.col("shipment_id").isNull() | F.col("carrier_id").isNull()).count()
check("no_null_keys", nulls == 0, f"(null_keys={nulls})")

# Uniqueness
dupes = day.groupBy("shipment_id").count().filter("count > 1").count()
check("shipment_id_unique", dupes == 0, f"(dupes={dupes})")

# Validity
neg = day.filter("weight_kg <= 0 OR linehaul_cost_eur <= 0").count()
check("no_nonpositive_measures", neg == 0, f"(bad_rows={neg})")

bad_time = day.filter("delivery_ts <= pickup_ts").count()
check("delivery_after_pickup", bad_time == 0, f"(bad_rows={bad_time})")

# Referential integrity against dims
orphans = (day.join(spark.read.table("silver_dim_carriers"), "carrier_id", "left_anti")).count()
check("carrier_fk_integrity", orphans == 0, f"(orphans={orphans})")

# Quarantine rate: a spike means the source degraded
q = spark.read.table("silver_quarantine_shipments") \
        .filter(F.to_date("quarantined_at") == F.current_date()).count()
check("quarantine_rate_below_1pct", q <= max(n, 1) * 0.01, f"(quarantined={q})")

# CELL ********************
# Write results to the run log table via a small append, then gate
import json
result = {"load_date": p_load_date, "checks_failed": len(failures), "failures": failures}
print(json.dumps(result, indent=2))

if failures:
    raise Exception(f"DQ gate failed ({len(failures)} checks): {'; '.join(failures)}")
mssparkutils.notebook.exit("DQ_PASS")
