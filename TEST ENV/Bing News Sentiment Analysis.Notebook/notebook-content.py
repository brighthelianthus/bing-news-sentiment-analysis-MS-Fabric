# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b8fde9a0-e077-4a84-a9c4-99671b604f8f",
# META       "default_lakehouse_name": "bing_lake_db",
# META       "default_lakehouse_workspace_id": "91b20797-d3ed-4675-9e5e-8af77c7e079b"
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("select * from bing_lake_db.tbl_latest_news")
display (df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import synapse.ml.core
from synapse.ml.services import AnalyzeText

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import the model and configure the input and output columns

model = (AnalyzeText()
.setTextCol("description")
.setKind ("SentimentAnalysis")
.setOutputCol("newsSentiment")
.setErrorCol("error")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Apply the model to our dataframe

result = model.transform(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create Sentiment Column
from pyspark.sql.functions import col

sentiment_df = result.withColumn("sentiment", col("newsSentiment.documents.sentiment"))
display(sentiment_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sentiment_df_final = sentiment_df.drop("error", "newsSentiment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sentiment_df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## INCREMENTAL LOAD SCD TYPE 1 AS PER PUR USE CASE

from pyspark.sql.utils import AnalysisException

try:
    table_name = 'bing_lake_db.tbl_sentiment_analysed'

    sentiment_df_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:   ### SCD type 1

    print ("Table Already Exists")

    sentiment_df_final.createOrReplaceTempView("view_sentiment_df_final")

    spark.sql(f""" MERGE INTO {table_name} target_table
                USING view_sentiment_df_final source_view
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
