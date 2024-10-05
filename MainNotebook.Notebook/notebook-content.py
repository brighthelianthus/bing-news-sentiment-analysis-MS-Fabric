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
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.mount("abfss://Project_Ecom_endtoend@onelake.dfs.fabric.microsoft.com/Project_Lakehouse.Lakehouse/Files","/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.getMountPath("/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

check_files = mssparkutils.fs.ls("Files/Current")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if check_files:
    mssparkutils.notebook.run("BRONZE_SALES")
    mssparkutils.notebook.run("GOLD_CUSTOMER")
    mssparkutils.notebook.run("GOLD_DATE")
    mssparkutils.notebook.run("Gold_PRODUCT")
    mssparkutils.notebook.run("Gold_SHIPMODE")
    mssparkutils.notebook.run("Gold_OrderPriority")
    mssparkutils.notebook.run("Gold_OrderReturn")
    mssparkutils.notebook.run("GOLD_FACT_SALE")
    mssparkutils.notebook.run("Move_FilesToArchive")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
