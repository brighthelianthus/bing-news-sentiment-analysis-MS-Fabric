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
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### For the bronze_sales and gold_orderreturn tables, we were loading into order priority table but this time we are not creating dataframe, tempview and inserting into table.
# ##### But this time, for the orderpriority table, We will use pyspark to create a delta table, implement an incremental logic and to insert the data into the delta table

# CELL ********************

DeltaTable.createIfNotExists(spark)\
            .tableName("Gold_OrderPriority")\
            .addColumn("OrderPriority_ID", LongType())\
            .addColumn ("Order_Priority", StringType())\
            .addColumn ("Created_TS", TimestampType())\
            .addColumn ("Modified_TS", TimestampType())\
            .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Project_Lakehouse.gold_orderpriority")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date = df.selectExpr("coalesce(max(Modified_TS), '1900-01-01')").first()[0]
print(Max_Date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table("Project_Lakehouse.bronze_sales")
df_bronze.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze_mod = df_bronze.select("Order_Priority").where (col("Modified_TS")>Max_Date).drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### OrderPriority_ID is not inside bronze_sales, hence we need to add monotonically incremental ID

# CELL ********************

Max_ID = df.selectExpr("coalesce(max(OrderPRiority_ID), 0)").first()[0]
Max_ID

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_bronze_mod.withColumn("OrderPriority_ID", Max_ID + monotonically_increasing_id() + 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_delta = DeltaTable.forPath(spark,"Tables/gold_orderpriority")
df_bronze_table = df_final
df_gold_delta.alias("gold")\
                    .merge(\
                    df_bronze_table.alias("bronze"),\
                    "gold.Order_Priority == Bronze.Order_Priority"\
                    )\
                    .whenMatchedUpdate(\
                    set= {
                        "gold.Modified_TS": current_timestamp()
                        }
                    )\
                    .whenNotMatchedInsert (\
                        values = {
                                 "gold.OrderPriority_ID" : "Bronze.OrderPriority_ID",
                                 "gold.Order_Priority" : "Bronze.Order_Priority",
                                "gold.Created_TS" : current_timestamp(),
                                "gold.Modified_TS" : current_timestamp()
                                }
                        )\
                        .execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from gold_orderpriority 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
