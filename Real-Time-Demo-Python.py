connectionString = ""
ehConf = {
  'eventhubs.connectionString' : connectionString
}

Real_T_Stream = spark \
  .readStream \
  .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider") \
  .options(**ehConf) \
  .load()

dftxn = Real_T_Stream.select(Real_T_Stream.body.cast("string").alias('message'))

dftxn.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("path", "") \ # storage of streamed files
  .option("checkpointLocation", "") \ # checkpoint folder
  .start()
