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

p_environment = "dev"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ship = spark.read.table("silver_shipments").filter("status = 'Delivered'")
lanes = spark.read.table("silver_dim_lanes")
carriers = spark.read.table("silver_dim_carriers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_shipments: one row per delivered shipment leg, conformed keys, additive measures
fact = (
    ship.join(lanes.select("lane_id", "distance_km"), "lane_id")
        .withColumn("date_key", F.date_format("pickup_ts", "yyyyMMdd").cast("int"))
        .withColumn("total_cost_eur",
                    F.round(F.col("linehaul_cost_eur") * (1 + F.col("fuel_surcharge_pct")), 2))
        .withColumn("margin_eur", F.round(F.col("customer_revenue_eur") - F.col("total_cost_eur"), 2))
        .withColumn("cost_per_km_eur", F.round(F.col("total_cost_eur") / F.col("distance_km"), 3))
        .select("shipment_id", "date_key", "lane_id", "carrier_id", "service_type",
                "pickup_ts", "delivery_ts", "transit_hours", "weight_kg", "distance_km",
                "total_cost_eur", "customer_revenue_eur", "margin_eur",
                "cost_per_km_eur", "on_time_flag")
)
fact.write.mode("overwrite").saveAsTable("gold_fact_shipments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_date: covers data range plus one year forward
from pyspark.sql.types import DateType
dates = spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-06-30'))) AS d")
(dates
    .withColumn("date_key", F.date_format("d", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("d")).withColumn("month_num", F.month("d"))
    .withColumn("month", F.date_format("d", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("d")))
    .withColumn("week_of_year", F.weekofyear("d"))
    .withColumn("day_name", F.date_format("d", "EEE"))
    .withColumn("is_weekend", F.dayofweek("d").isin([1, 7]))
    .withColumnRenamed("d", "date")
    .write.mode("overwrite").saveAsTable("gold_dim_date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

carriers.write.mode("overwrite").saveAsTable("gold_dim_carriers")
lanes.write.mode("overwrite").saveAsTable("gold_dim_lanes")

# V-Order is on by default for Fabric Spark writes; verify for Direct Lake performance
for t in ["gold_fact_shipments", "gold_dim_date", "gold_dim_carriers", "gold_dim_lanes"]:
    spark.sql(f"OPTIMIZE {t}")
print("Gold layer rebuilt.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
