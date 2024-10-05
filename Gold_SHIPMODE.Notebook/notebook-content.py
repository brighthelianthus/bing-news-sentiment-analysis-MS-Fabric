# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc995a33-a092-4b6b-b4ac-f0018ea4b646",
# META       "default_lakehouse_name": "Project_Lakehouse",
# META       "default_lakehouse_workspace_id": "ab48eeac-e849-42f5-b90f-39cb71ff5624",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc995a33-a092-4b6b-b4ac-f0018ea4b646"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Project_Lakehouse.gold_shipmode
# MAGIC (
# MAGIC     ShipMode_ID long ,
# MAGIC     ShipMode string,
# MAGIC     Created_TS timestamp,
# MAGIC     Modified_TS timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date = spark.sql("select coalesce(max(Modified_TS),'1900-01-01') from Project_Lakehouse.gold_shipmode").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.sql("select distinct ship_mode from Project_Lakehouse.bronze_sales where Modified_TS>'{}'".format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_ID = spark.sql("select coalesce(max(Shipmode_ID),0) from Project_Lakehouse.gold_shipmode").first()[0]
df_final = df_bronze.withColumn("ShipMode_ID", monotonically_increasing_id()+Max_ID+1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.createOrReplaceTempView("ViewShipMode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from ViewShipMode

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC merge into Project_Lakehouse.gold_shipmode as gs
# MAGIC using ViewShipMode as vs 
# MAGIC on gs.ShipMode = vs.Ship_Mode
# MAGIC when MATCHED THEN 
# MAGIC update set
# MAGIC Modified_TS = current_timestamp()
# MAGIC 
# MAGIC when not matched then
# MAGIC INSERT(
# MAGIC gs.ShipMode_ID,
# MAGIC gs.ShipMode,
# MAGIC gs.Created_TS,
# MAGIC gs.Modified_TS
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC     vs.ShipMode_ID,
# MAGIC     vs.Ship_Mode,
# MAGIC     current_timestamp(),
# MAGIC     CURRENT_TIMESTAMP()
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from gold_shipmode

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
