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

p_table_name = "dim_carriers"
p_load_date = ""
p_environment = "dev"
p_is_full = "true"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

src = f"bronze_{p_table_name}"
tgt = f"silver_{p_table_name}"
df = spark.read.table(src).dropDuplicates()

if p_table_name == "dim_carriers":
    df = (df.withColumn("fleet_size", F.col("fleet_size").cast("int"))
            .withColumn("safety_rating", F.col("safety_rating").cast("double"))
            .withColumn("contracted_rate_per_km_eur", F.col("contracted_rate_per_km_eur").cast("double")))
elif p_table_name == "dim_lanes":
    df = (df.withColumn("distance_km", F.col("distance_km").cast("int"))
            .withColumn("std_transit_hours", F.col("std_transit_hours").cast("double")))

df.write.mode("overwrite").saveAsTable(tgt)
print(f"{tgt}: {df.count()} rows")
mssparkutils.notebook.exit("FULL_LOAD_OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
