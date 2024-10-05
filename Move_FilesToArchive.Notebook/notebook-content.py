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

mssparkutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.mount("abfss://Project_Ecom_endtoend@onelake.dfs.fabric.microsoft.com/Project_Lakehouse.Lakehouse/Files/Current","/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.getMountPath('/Files')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

check_files = mssparkutils.fs.ls('Files/Current')
check_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Define the source and destination directories
source_dir = "Files/Current/"
dest_dir = "Files/Archive/"

# Loop through each file and move it
for file in check_files:
 #   source_path = f"{source_dir}{file}"
  #  dest_path = f"{dest_dir}{file}"
    
    # Move the file
    mssparkutils.fs.mv(file.path, dest_dir)
    print(f"Moved {source_path} to {dest_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
