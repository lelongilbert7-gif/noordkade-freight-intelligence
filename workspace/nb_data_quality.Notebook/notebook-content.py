# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "42305316-24c1-4368-a3b6-875aa6ae31d6",
# META       "default_lakehouse_name": "lh_freight",
# META       "default_lakehouse_workspace_id": "7e236ae2-d6a3-453e-bcd5-fc54c4f753a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "42305316-24c1-4368-a3b6-875aa6ae31d6"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

p_load_date = "2026-01-15"
p_environment = "dev"
p_is_full = "false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

failures = []

def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name} {detail}")
    if not condition:
        failures.append(f"{name} {detail}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

is_full = str(p_is_full).lower() == "true"
ship = spark.read.table("silver_shipments")

if is_full:
    # Full-reload mode: validate the whole table, not one day
    n = ship.count()
    check("total_volume_in_band", 800_000 <= n <= 1_200_000, f"(rows={n})")
    scope = ship
else:
    scope = ship.filter(F.to_date("pickup_ts") == F.lit(p_load_date))
    n = scope.count()
    check("daily_volume_in_band", 500 <= n <= 10_000, f"(rows={n})")

nulls = scope.filter(F.col("shipment_id").isNull() | F.col("carrier_id").isNull()).count()
check("no_null_keys", nulls == 0, f"(null_keys={nulls})")

dupes = scope.groupBy("shipment_id").count().filter("count > 1").count()
check("shipment_id_unique", dupes == 0, f"(dupes={dupes})")

neg = scope.filter("weight_kg <= 0 OR linehaul_cost_eur <= 0").count()
check("no_nonpositive_measures", neg == 0, f"(bad_rows={neg})")

bad_time = scope.filter("delivery_ts <= pickup_ts").count()
check("delivery_after_pickup", bad_time == 0, f"(bad_rows={bad_time})")

orphans = scope.join(spark.read.table("silver_dim_carriers"), "carrier_id", "left_anti").count()
check("carrier_fk_integrity", orphans == 0, f"(orphans={orphans})")

q = spark.read.table("silver_quarantine_shipments") \
        .filter(F.to_date("quarantined_at") == F.current_date()).count()
check("quarantine_rate_below_1pct", q <= max(n, 1) * 0.01, f"(quarantined={q}, basis={n})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write results to the run log table via a small append, then gate
import json
result = {"load_date": p_load_date, "checks_failed": len(failures), "failures": failures}
print(json.dumps(result, indent=2))

if failures:
    raise Exception(f"DQ gate failed ({len(failures)} checks): {'; '.join(failures)}")
mssparkutils.notebook.exit("DQ_PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
