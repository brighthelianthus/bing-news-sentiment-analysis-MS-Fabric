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
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Project_Lakehouse.gold_customer
# MAGIC (
# MAGIC     Customer_Id string,
# MAGIC     Customer_Name string,
# MAGIC     Segment string,
# MAGIC     City string,
# MAGIC     State string,
# MAGIC     Country string,
# MAGIC     Region string,
# MAGIC     Created_TS timestamp,
# MAGIC     Modified_TS timestamp
# MAGIC )
# MAGIC using delta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date = spark.sql (""" select coalesce(max("Modified_TS"),"1900-01-01") from Project_Lakehouse.gold_customer""").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table ("Project_Lakehouse.bronze_sales")

df_final = df_bronze.selectExpr("Customer_ID", "Customer_Name", "Segment", "City", "State", \
                                "Country", "Region") \
                                .where(col("Modified_TS")>Max_Date)\
                                .drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.createOrReplaceTempView("ViewCustomer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from ViewCustomer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Merge into Project_Lakehouse.gold_customer as gc
# MAGIC using ViewCustomer as vc
# MAGIC on gc.Customer_ID = vc.Customer_ID
# MAGIC when matched then
# MAGIC update set
# MAGIC gc.Customer_Name = vc.Customer_Name,
# MAGIC gc.Segment = vc.Segment,
# MAGIC gc.City = vc.City,
# MAGIC gc.State = vc.State,
# MAGIC gc.Country = vc.Country,
# MAGIC gc.Region = vc.Region,
# MAGIC gc.Created_TS = current_timestamp(),
# MAGIC gc.Modified_TS = CURRENT_TIMESTAMP()
# MAGIC when not matched then INSERT(
# MAGIC gc.Customer_ID,
# MAGIC gc.Customer_Name ,
# MAGIC gc.Segment ,
# MAGIC gc.City ,
# MAGIC gc.State ,
# MAGIC gc.Country ,
# MAGIC gc.Region ,
# MAGIC gc.Created_TS ,
# MAGIC gc.Modified_TS )
# MAGIC values 
# MAGIC (vc.customer_id,
# MAGIC vc.Customer_Name ,
# MAGIC vc.Segment ,
# MAGIC vc.City ,
# MAGIC vc.State ,
# MAGIC vc.Country ,
# MAGIC vc.Region ,
# MAGIC CURRENT_TIMESTAMP() ,
# MAGIC CURRENT_TIMESTAMP()
# MAGIC 
# MAGIC )
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
