# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "def56cdc-9ae7-40c1-8883-52bb154ce846",
# META       "default_lakehouse_name": "lh_freight",
# META       "default_lakehouse_workspace_id": "7e236ae2-d6a3-453e-bcd5-fc54c4f753a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "def56cdc-9ae7-40c1-8883-52bb154ce846"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

p_table_name = "shipments"
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

is_full = str(p_is_full).lower() == "true"
bronze_tbl = f"bronze_{p_table_name}"
silver_tbl = f"silver_{p_table_name}"
print(f"[{p_environment}] {bronze_tbl} -> {silver_tbl} | load_date={p_load_date} | full={is_full}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table(bronze_tbl)
if not is_full:
    # Bronze is append-only; process only the slice for this load date
    df = df.filter(F.to_date("pickup_ts") == F.lit(p_load_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    df.withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
      .withColumn("delivery_ts", F.to_timestamp("delivery_ts"))
      .withColumn("weight_kg", F.col("weight_kg").cast("int"))
      .withColumn("linehaul_cost_eur", F.col("linehaul_cost_eur").cast("double"))
      .withColumn("fuel_surcharge_pct", F.col("fuel_surcharge_pct").cast("double"))
      .withColumn("customer_revenue_eur", F.col("customer_revenue_eur").cast("double"))
      .withColumn("on_time_flag", F.col("on_time_flag").cast("boolean"))
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exit with new watermark = max pickup_ts actually written
new_wm = good.agg(F.max("pickup_ts")).collect()[0][0]
result = new_wm.strftime("%Y-%m-%dT%H:%M:%S") if new_wm else p_load_date
print(f"rows={good.count()} quarantined={bad.count()} watermark={result}")
mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
