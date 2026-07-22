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

p_notebook_name = "nb_silver_dims"
p_table_name = ""
p_load_date = ""
p_environment = "dev"
p_is_full = "false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = mssparkutils.notebook.run(p_notebook_name, 1200, {
    "p_table_name": p_table_name,
    "p_load_date": p_load_date,
    "p_environment": p_environment,
    "p_is_full": p_is_full,
})
mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
