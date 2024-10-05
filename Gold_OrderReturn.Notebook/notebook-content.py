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
# META       "default_lakehouse_workspace_id": "ab48eeac-e849-42f5-b90f-39cb71ff5624"
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

# MARKDOWN ********************

# ### Dimensional model


# MARKDOWN ********************

# abfss://Project_Ecom_endtoend@onelake.dfs.fabric.microsoft.com/Project_Lakehouse.Lakehouse/Files/Archive/dim model.PNG
# ![image-alt-text]("abfss://Project_Ecom_endtoend@onelake.dfs.fabric.microsoft.com/Project_Lakehouse.Lakehouse/Files/Archive/dim model.PNG")

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists Project_Lakehouse.Gold_OrderReturn
# MAGIC (
# MAGIC     OrderID string,
# MAGIC     Return string,
# MAGIC     Order_Year int,
# MAGIC     Order_Month int,
# MAGIC     Created_TS TIMESTAMP,
# MAGIC     Modified_TS TIMESTAMP)
# MAGIC     using delta 
# MAGIC     partitioned by (Order_Year, Order_Month
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


Max_Date = spark.sql(" select coalesce(max('Modified_TS'),'1900-01-01') from Project_Lakehouse.Gold_OrderReturn").first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(
"""select Order_ID,
    Return,
    Order_Year,
    Order_Month,
    Created_TS,
    Modified_TS
from Project_Lakehouse.bronze_sales where Return = 'Yes' and Modified_TS > '{}'""".format(Max_Date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView("ViewReturn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from ViewReturn

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC insert into Project_Lakehouse.Gold_OrderReturn 
# MAGIC select * from ViewReturn

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from gold_orderreturn limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
