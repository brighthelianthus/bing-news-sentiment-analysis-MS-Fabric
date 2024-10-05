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
# MAGIC create table if not exists Project_Lakehouse.gold_fact_sale
# MAGIC (
# MAGIC     Order_ID string,
# MAGIC     Price float,
# MAGIC     quantity float,
# MAGIC     sales float,
# MAGIC     discount float,
# MAGIC     profit float,
# MAGIC     shipping_cost float,
# MAGIC     order_date date,
# MAGIC     shipping_date date,
# MAGIC     product_id long,
# MAGIC     orderpriority_id long,
# MAGIC     shipmode_id long,
# MAGIC     customer_id string,
# MAGIC     order_year integer,
# MAGIC     order_month integer,
# MAGIC     created_ts timestamp,
# MAGIC     modified_ts timestamp
# MAGIC )
# MAGIC using DELTA
# MAGIC PARTITIONED by (ORDER_YEAR,ORDER_MONTH)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Max_Date = spark.sql("select coalesce(max(Modified_TS), '1900-01-01') from Project_Lakehouse.gold_fact_sale").first()[0]
print(Max_Date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.sql(""" select 
bronze_sales.order_id,
bronze_sales.sales as price,
bronze_sales.quantity,
bronze_sales.sales * bronze_sales.quantity as sales,
bronze_sales.discount,
bronze_sales.profit,
bronze_sales.shipping_cost,
bronze_sales.order_date,
bronze_sales.shipping_date,
gold_product.product_id,
gold_orderpriority.orderpriority_id,
gold_shipmode.shipmode_id,
bronze_sales.customer_id,
Year(Order_Date) as Order_Year,
Month(Order_Date) as Order_Month
from Project_Lakehouse.bronze_sales
inner join Project_Lakehouse.gold_product on bronze_sales.Product = gold_product.Product and  bronze_sales.Product_Category = gold_product.Product_Category
inner join Project_Lakehouse.gold_shipmode on bronze_sales.ship_mode = gold_shipmode.shipmode
inner join Project_Lakehouse.gold_orderpriority on bronze_sales.Order_Priority = gold_orderpriority.order_priority
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.createOrReplaceTempView("ViewFactSale")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC merge into Project_Lakehouse.gold_fact_sale as gfs
# MAGIC using ViewFactSale as vfs
# MAGIC on gfs.order_year = vfs.order_year and gfs.order_month = vfs.order_month and gfs.order_id = vfs.order_id
# MAGIC when matched THEN update set
# MAGIC gfs.Sales = vfs.Sales,
# MAGIC gfs.price = vfs.price,
# MAGIC gfs.quantity = vfs.quantity,
# MAGIC gfs.discount = vfs.discount,
# MAGIC gfs.profit = vfs.profit,
# MAGIC gfs.shipping_cost = vfs.shipping_cost,
# MAGIC gfs.order_date = vfs.order_date,
# MAGIC gfs.shipping_date = vfs.shipping_date,
# MAGIC gfs.product_id = vfs.product_id,
# MAGIC gfs.shipmode_id = vfs.shipmode_id,
# MAGIC gfs.customer_id = vfs.customer_id,
# MAGIC gfs.modified_ts = current_timestamp()
# MAGIC 
# MAGIC 
# MAGIC when  not matched then 
# MAGIC insert 
# MAGIC (
# MAGIC gfs.Sales ,
# MAGIC gfs.price ,
# MAGIC gfs.quantity ,
# MAGIC gfs.discount ,
# MAGIC gfs.profit ,
# MAGIC gfs.shipping_cost ,
# MAGIC gfs.order_date ,
# MAGIC gfs.shipping_date ,
# MAGIC gfs.product_id ,
# MAGIC gfs.shipmode_id ,
# MAGIC gfs.customer_id ,
# MAGIC gfs.created_ts,
# MAGIC gfs.modified_ts,
# MAGIC gfs.order_year ,
# MAGIC gfs.order_month 
# MAGIC )
# MAGIC values 
# MAGIC (
# MAGIC vfs.Sales,
# MAGIC vfs.price,
# MAGIC vfs.quantity,
# MAGIC vfs.discount,
# MAGIC vfs.profit,
# MAGIC vfs.shipping_cost,
# MAGIC vfs.order_date,
# MAGIC vfs.shipping_date,
# MAGIC vfs.product_id,
# MAGIC vfs.shipmode_id,
# MAGIC vfs.customer_id,
# MAGIC current_timestamp(),
# MAGIC current_timestamp(),
# MAGIC vfs.order_year,
# MAGIC vfs.order_month
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
