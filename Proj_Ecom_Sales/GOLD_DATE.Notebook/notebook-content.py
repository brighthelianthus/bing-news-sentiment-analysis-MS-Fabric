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
# MAGIC create table if not exists Project_Lakehouse.gold_date
# MAGIC (
# MAGIC 
# MAGIC     BusinessDate date,
# MAGIC     Business_Year int,
# MAGIC     Business_Month int,
# MAGIC     Business_Quarter int,
# MAGIC     Business_Week int
# MAGIC ) using DELTA
# MAGIC PARTITIONED BY (Business_Year)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = '2015-01-01'
end_date = '2030-12-31'

date_diff = spark.sql("select date_diff('{}','{}')".format(end_date,start_date)).first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_diff

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

id_data = spark.range(0,date_diff)
id_data.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_data = id_data.selectExpr("date_add('{}', cast(id as int)) as BusinessDate".format(start_date))
date_data.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_data.createOrReplaceTempView("ViewDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM Project_Lakehouse.gold_date

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC insert into Project_Lakehouse.gold_date (
# MAGIC select * , 
# MAGIC Year(BusinessDate) as Business_Year,
# MAGIC  Month(BusinessDate) as Business_Month,
# MAGIC  Quarter(BusinessDate) as Business_Quarter,
# MAGIC  weekday(BusinessDate) as Business_week
# MAGIC  from ViewDate)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from Project_Lakehouse.gold_date 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from Project_Lakehouse.gold_date

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
