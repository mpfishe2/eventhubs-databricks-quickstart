// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
import org.apache.spark.sql.functions._

// COMMAND ----------

val connectionString = ConnectionStringBuilder("<EVENT HUBS CONNECTION STRING>").build
val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

var eventhubs = 
  spark.readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

eventhubs.select(($"body").cast("string"))

// COMMAND ----------

eventhubs.writeStream
  .outputMode("append")
  .format("json")
  .option("path", "dbfs:/<storeLocation>")
  .option("checkpointLocation", "dbfs:/<checkpointLocation>")
  .start()

// COMMAND ----------

val df = eventhubs.select(($"body").cast("string"))
display(df)

// COMMAND ----------

val jsDF = df.select(get_json_object($"body", "$.timestamp").cast("timestamp").alias("time"),
                     get_json_object($"body", "$.temperature").alias("temp"),
                     get_json_object($"body", "$.humidity").alias("humidity"),
                     get_json_object($"body", "$.city").alias("location"))


// COMMAND ----------

display(jsDF)

// COMMAND ----------

display(jsDF)

// COMMAND ----------


