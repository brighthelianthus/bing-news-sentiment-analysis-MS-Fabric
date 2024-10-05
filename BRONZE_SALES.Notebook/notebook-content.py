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
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales = pd.read_excel("abfss://Project_Ecom_endtoend@onelake.dfs.fabric.microsoft.com/Project_Lakehouse.Lakehouse/Files/Current/Sales*.xlsx", sheet_name = "Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.createDataFrame (df_sales)
display(df1.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Returns = pd.read_excel("abfss://Project_Ecom_endtoend@onelake.dfs.fabric.microsoft.com/Project_Lakehouse.Lakehouse/Files/Current/Sales*.xlsx", sheet_name = "Returns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.createDataFrame(df_Returns)
display(df2.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df1.join (df2, df1.Order_ID == df2.Order_ID, how = "left").drop(df2.Order_ID, df2.Customer_Name, df2.Sales_Amount)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mod = df_final.withColumns({"Order_Year": year("Order_Date"),\
                "Order_Month": month("Order_Date"),\
                "Create_TS": current_timestamp(), \
                "Modified_TS": current_timestamp()  }  )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mod.createOrReplaceTempView("ViewSales")
df_mod.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM ViewSales").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --drop table Project_Lakehouse.Bronze_Sales;
# MAGIC create table if not exists Project_Lakehouse.Bronze_Sales
# MAGIC (
# MAGIC     Order_ID  string,
# MAGIC     Order_Date Date,
# MAGIC     Shipping_Date date,
# MAGIC     Aging int,
# MAGIC     Ship_Mode string,
# MAGIC     Product_Category string,
# MAGIC     Product string,
# MAGIC     Sales float,
# MAGIC     Order_Priority string,
# MAGIC     Quantity float,
# MAGIC     Discount float,
# MAGIC     Profit float,
# MAGIC     Shipping_Cost float,
# MAGIC     Customer_ID string,
# MAGIC     Customer_Name string,
# MAGIC     Segment string,
# MAGIC     City string,
# MAGIC     State string,
# MAGIC     Country string,
# MAGIC     Region string,
# MAGIC     Return string,
# MAGIC     Order_Year int,
# MAGIC     Order_Month int,
# MAGIC     Created_TS TIMESTAMP,
# MAGIC     Modified_TS TIMESTAMP
# MAGIC )
# MAGIC using DELTA
# MAGIC PARTITIONED by (Order_Year, Order_Month)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Merge into Project_Lakehouse.Bronze_Sales AS BS
# MAGIC using ViewSales as VS
# MAGIC on BS.Order_Year = VS.Order_Year and BS.Order_Month = VS.Order_Month and BS.Order_ID = VS.Order_ID
# MAGIC when matched then
# MAGIC     update SET
# MAGIC     BS.Order_ID  =VS.Order_ID,
# MAGIC     BS.Order_Date =VS.Order_Date ,
# MAGIC     BS.Shipping_Date =VS.Shipping_Date,
# MAGIC     BS.Aging =VS.Aging,
# MAGIC     BS.Ship_Mode =VS.Ship_Mode,
# MAGIC     BS.Product_Category =VS.Product_Category,
# MAGIC     BS.Product =VS.Product,
# MAGIC     BS.Sales =VS.Sales,
# MAGIC     BS.Order_Priority = VS.Order_Priority,
# MAGIC     BS.Quantity =VS.Quantity,
# MAGIC     BS.Discount =VS.Discount,
# MAGIC     BS.Profit = VS.Profit,
# MAGIC     BS.Shipping_Cost =VS.Shipping_Cost,
# MAGIC     BS.Customer_ID =VS.Customer_ID ,
# MAGIC     BS.Customer_Name =VS.Customer_Name ,
# MAGIC     BS.Segment =VS.Segment ,
# MAGIC     BS.City =VS.City,
# MAGIC     BS.State =VS.State,
# MAGIC     BS.Country =VS.Country ,
# MAGIC     BS.Region =VS.Region ,
# MAGIC     BS.Return =VS.Return,
# MAGIC     BS.Order_Year =VS.Order_Year,
# MAGIC     BS.Order_Month = VS.Order_Month,
# MAGIC     BS.Created_TS =VS.Create_TS ,
# MAGIC     BS.Modified_TS =VS.Modified_TS
# MAGIC when NOT matched then
# MAGIC     insert 
# MAGIC     ( Order_ID ,
# MAGIC     Order_Date, 
# MAGIC     Shipping_Date ,
# MAGIC     Aging ,
# MAGIC     Ship_Mode,
# MAGIC     Product_Category,
# MAGIC     Product,
# MAGIC     Sales ,
# MAGIC     Order_Priority,
# MAGIC     Quantity ,
# MAGIC     Discount ,
# MAGIC     Profit ,
# MAGIC     Shipping_Cost ,
# MAGIC     Customer_ID,
# MAGIC     Customer_Name,
# MAGIC     Segment,
# MAGIC     City,
# MAGIC     State,
# MAGIC     Country,
# MAGIC     Region,
# MAGIC     Return,
# MAGIC     Order_Year ,
# MAGIC     Order_Month,
# MAGIC     Created_TS ,
# MAGIC     Modified_TS ) 
# MAGIC     values 
# MAGIC     (VS.Order_ID ,
# MAGIC     VS.Order_Date, 
# MAGIC     VS.Shipping_Date ,
# MAGIC     VS.Aging,
# MAGIC     VS.Ship_Mode,
# MAGIC     VS.Product_Category,
# MAGIC     VS.Product,
# MAGIC     VS.Sales ,
# MAGIC     Order_Priority,
# MAGIC     VS.Quantity ,
# MAGIC     VS.Discount ,
# MAGIC     VS.Profit ,
# MAGIC     VS.Shipping_Cost ,
# MAGIC     VS.Customer_ID,
# MAGIC     VS.Customer_Name,
# MAGIC     VS.Segment,
# MAGIC     VS.City,
# MAGIC     VS.State,
# MAGIC     VS.Country,
# MAGIC     VS.Region,
# MAGIC     VS.Return,
# MAGIC     VS.Order_Year ,
# MAGIC     VS.Order_Month,
# MAGIC     VS.Create_TS ,
# MAGIC     VS.Modified_TS) ;
# MAGIC     


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from Project_Lakehouse.Bronze_Sales limit (10)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
