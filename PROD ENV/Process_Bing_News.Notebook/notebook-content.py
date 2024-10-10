# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a9dc683e-ee82-47ad-b447-75ebc0f25267",
# META       "default_lakehouse_name": "bing_lake_db",
# META       "default_lakehouse_workspace_id": "95fd865b-b054-4394-95ac-2080c5b35a7a"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Read the JSON file as a Dataframe

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/dev/bing-latest-news.json")
# df now is a Spark DataFrame containing JSON data from "Files/dev/bing-latest-news.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select("value")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Explode the JSON column

# CELL ********************

from pyspark.sql.functions import explode
import json 
df_exploded = df.select(explode(df["value"]).alias("json_object"))
display(df_exploded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Converting the Exploded JSON Dataframe to a single JSON string list

# CELL ********************

json_list = df_exploded.toJSON().collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Testing the JSON string list 11th item

# CELL ********************

#print(json_list[10])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Converting JSON string to JSON dictionary

# MARKDOWN ********************


# CELL ********************

# news_json = json.loads(json_list[22])
# print(news_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### What information do we need from the json dictionary?
# description,
# title,
# category,
# image,
# url,
# provider,
# datePublished

# CELL ********************

title = []
description = []
category = []
image = []
provider = []
url = []
datePublished = []

# process for each json object inside the list
for json_str in json_list:
    try:
        #parse the JSON string into a dictionary
        article = json.loads(json_str)
        
        if article["json_object"].get("category") and article["json_object"].get("image",{}).get("thumbnail",{}).get("contentUrl"):

            #Extract information from the dictionary as needed
            title.append(article['json_object']['name'])
            description.append(article['json_object']['description'])
            category.append(article['json_object']['category'])
            datePublished.append(article['json_object']['datePublished'])
            image.append(article['json_object']['image']['thumbnail']['contentUrl'])
            provider.append(article['json_object']['provider'][0]['name'])
            url.append(article['json_object']['url'])

    except Exception as e:
        print(f"Error processing JSON object: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructField, StructType, StringType

#Combine the lists
data = list (zip(title, description, category, url, image, provider, datePublished))
print( type(data))

#Define schema
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("datePublished", StringType(), True)
])

#Create DataFrame
df_cleaned = spark.createDataFrame (data, schema = schema)
df_cleaned.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, date_format
df_cleaned_final = df_cleaned.withColumn("datePublished", date_format(to_date("datePublished"),"dd-MMM-yyyy"))

display (df_cleaned_final.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Writing the Final Cleaned Dataframe to the Lakehouse DB in a Delta Format

# CELL ********************

# df_cleaned_final.createOrReplaceTempView("tblView")
# spark.sql("""create table IF NOT EXISTS bing_lake_db.tbl_latest_news 
# using delta 
# select * from tblView where 1=2""")
# df_cleaned_final.write.format("delta").mode("append").insertInto("bing_lake_db.tbl_latest_news")

## INCREMENTAL LOAD SCD TYPE 1 AS PER PUR USE CASE

from pyspark.sql.utils import AnalysisException

try:
    table_name = 'bing_lake_db.tbl_latest_news'

    df_cleaned_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:   ### SCD type 1

    print ("Table Already Exists")

    df_cleaned_final.createOrReplaceTempView("view_df_cleaned_final")

    spark.sql(f""" MERGE INTO {table_name} target_table
                USING view_df_cleaned_final source_view
                on source_view.url = target_table.url

                WHEN MATCHED AND    ---- SOME INFORMATION FOR EXISTING NEWS URL GOT UPDATED
                (source_view.title <> target_table.title OR
                source_view.description <> target_table.description OR
                source_view.category <> target_table.category OR
                source_view.image <> target_table.image OR
                source_view.provider <> target_table.provider OR
                source_view.datePublished <> target_table.datePublished)
                THEN UPDATE SET *

                WHEN NOT MATCHED   ---- COMPLETELY NEW NEWS URL RECEIVED
                THEN INSERT *
                                    ----- MERGE SOES NOT DO ANYTHING IF URL RECORD IS PRESENT AND THERE IS NO CHANGE
                """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *, count(*) from bing_lake_db.tbl_latest_news group by title, description, url, image, provider, datePublished, category having count(*) > 1;
# MAGIC select count(*) from bing_lake_db.tbl_latest_news

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
