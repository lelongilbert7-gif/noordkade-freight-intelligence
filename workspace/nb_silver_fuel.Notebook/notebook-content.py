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

df = (spark.read.table("bronze_fuel_purchases").dropDuplicates()
        .withColumn("purchase_date", F.to_date("purchase_date"))
        .withColumn("litres", F.col("litres").cast("int"))
        .withColumn("price_per_litre_eur", F.col("price_per_litre_eur").cast("double")))
df.write.mode("overwrite").saveAsTable("silver_fuel_purchases")
print(f"silver_fuel_purchases: {df.count()} rows")
mssparkutils.notebook.exit("FULL_LOAD_OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
