# Fabric notebook source
# Notebook: nb_silver_shipments
# Invoked by pl_ingest_metadata_driven with base parameters.
# Exits with the new watermark value so the pipeline can persist it.

# CELL ********************  <- toggle THIS cell as "parameters cell" in Fabric
p_table_name = "shipments"
p_load_date = "2026-01-15"
p_environment = "dev"
p_is_full = "false"

# CELL ********************
from pyspark.sql import functions as F

is_full = str(p_is_full).lower() == "true"
bronze_tbl = f"bronze_{p_table_name}"
silver_tbl = f"silver_{p_table_name}"
print(f"[{p_environment}] {bronze_tbl} -> {silver_tbl} | load_date={p_load_date} | full={is_full}")

# CELL ********************
df = spark.read.table(bronze_tbl)
if not is_full:
    # Bronze is append-only; process only the slice for this load date
    df = df.filter(F.to_date("pickup_ts") == F.lit(p_load_date))

# CELL ********************
# Conform: types, dedupe, quarantine bad rows instead of dropping silently
df = (
    df.withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
      .withColumn("delivery_ts", F.to_timestamp("delivery_ts"))
      .withColumn("weight_kg", F.col("weight_kg").cast("int"))
      .withColumn("transit_hours",
                  (F.unix_timestamp("delivery_ts") - F.unix_timestamp("pickup_ts")) / 3600)
      .dropDuplicates(["shipment_id"])
)

valid_carriers = [r.carrier_id for r in spark.read.table("silver_dim_carriers").select("carrier_id").collect()]
bad = df.filter((F.col("weight_kg") <= 0) | (~F.col("carrier_id").isin(valid_carriers)))
good = df.subtract(bad)

(bad.withColumn("quarantine_reason",
                F.when(F.col("weight_kg") <= 0, "invalid_weight").otherwise("unknown_carrier"))
    .withColumn("quarantined_at", F.current_timestamp())
    .write.mode("append").saveAsTable("silver_quarantine_shipments"))

# CELL ********************
# Merge into silver (idempotent: reruns of the same load date don't duplicate)
from delta.tables import DeltaTable

if is_full or not spark.catalog.tableExists(silver_tbl):
    good.write.mode("overwrite").saveAsTable(silver_tbl)
else:
    (DeltaTable.forName(spark, silver_tbl).alias("t")
        .merge(good.alias("s"), "t.shipment_id = s.shipment_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# CELL ********************
# Exit with new watermark = max pickup_ts actually written
new_wm = good.agg(F.max("pickup_ts")).collect()[0][0]
result = new_wm.strftime("%Y-%m-%dT%H:%M:%S") if new_wm else p_load_date
print(f"rows={good.count()} quarantined={bad.count()} watermark={result}")
mssparkutils.notebook.exit(result)
