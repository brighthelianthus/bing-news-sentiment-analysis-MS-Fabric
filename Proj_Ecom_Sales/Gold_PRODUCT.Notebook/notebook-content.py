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
# MAGIC 
# MAGIC create table if not exists Project_Lakehouse.gold_product
# MAGIC (
# MAGIC 
# MAGIC         Product_ID long,
# MAGIC         Product_category string,
# MAGIC         Product string,
# MAGIC         Created_TS timestamp,
# MAGIC         Modified_TS timestamp
# MAGIC ) using delta

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date = spark.sql("""
select coalesce(max(Modified_TS),'1900-01-01') from Project_Lakehouse.gold_product""")\
.first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.sql(""" select distinct Product_Category, Product from Project_Lakehouse.bronze_sales 
                        where Modified_TS > '{}'""" .format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_bronze.withColumn("Product_ID", monotonically_increasing_id()+1)
df_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.createOrReplaceTempView("ViewProduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Merge into Project_lakehouse.gold_product as gp
# MAGIC using viewproduct as vp
# MAGIC on gp.product = vp.product and gp.product_category = vp.product_category
# MAGIC when matched then update set
# MAGIC gp.modified_ts = current_timestamp()
# MAGIC 
# MAGIC when not matched THEN
# MAGIC insert 
# MAGIC (
# MAGIC     gp.product_id,
# MAGIC     gp.product,
# MAGIC     gp.product_category,
# MAGIC     gp.created_ts,
# MAGIC     gp.modified_ts
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC     vp.product_id,
# MAGIC     vp.product,
# MAGIC     vp.product_category,
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     CURRENT_TIMESTAMP()
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
