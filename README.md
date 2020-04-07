# Spark Structured Streaming 
   
# Source : 
		Kafka ,File Systems(CSV,Delimiter,Parquet,orc,avro,json),Socket

# Target: 
		Kafka ,Console,meory,foreach 

#IMP:  Schema Definition is manadatory to process the data 

# By defualt it will fall in the  column  known as VALUE  


Structured Streaming is a stream processing engine built on the Spark SQL engine.

StructuredNetworkWordCount maintains a running word count of text data received from a TCP socket. DataFrame lines represents an unbounded table containing the streaming text. The table contains one column of strings value, and each line in the streaming text data becomes a row in the table.The DataFrame is converted to a Dataset of String using .as[String]. We then apply the flatMap to split each line into words Dataset. Finally, we define the wordCounts DataFrame by grouping it by the word and counting them. wordCounts is a streaming DataFrame representing the running word counts.


#1. Input sources:
 # i. Read From Socket Stream 
			```
			val socketDF = spark
		  .readStream
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 9999)
		  .load()

		socketDF.isStreaming    // Returns True for DataFrames that have streaming sources

		socketDF.printSchema

	```
 # ii. Read from Files(CSV,JSON,Parquet,Orc...)
		```

		// Read all the csv files written atomically in a directory
		val userSchema = new StructType().add("name", "string").add("age", "integer")
		val csvDF = spark
		  .readStream
		  .option("sep", ";")
		  .schema(userSchema)      // Specify schema of the csv files
		  .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")
		  
		  
		```




val outputPathDir = workingDir + "/output.parquet" // A subdirectory for our output
val checkpointPath = workingDir + "/checkpoint"    // A subdirectory for our checkpoint & W-A logs
val myStreamName = "lesson02_ss"                   // An arbitrary name for the stream  

# Output Modes
		Mode	    Example	Notes
		Complete	dsw.outputMode("complete")	The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table.
		Append	    dsw.outputMode("append")	Only the new rows appended to the Result Table since the last trigger are written to the sink.
		Update	    dsw.outputMode("update")	Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. Since Spark 2.1.1
		In the example below, we are writing to a Parquet directory which only supports the append mode:


# Managing Streaming Queries

	id	get unique identifier of the running query that persists across restarts from checkpoint data
	runId	get unique id of this run of the query, which will be generated at every start/restart
	name	get name of the auto-generated or user-specified name
	explain()	print detailed explanations of the query
	stop()	stop query
	awaitTermination()	block until query is terminated, with stop() or with error
	exception	exception if query terminated with error
	recentProgress	array of most recent progress updates for this query
	lastProgress	most recent progress update of this streaming query

# Output Sinks
	DataStreamWriter.format accepts the following values, among others:

	Output Sink	Example	Notes
	File	dsw.format("parquet"), dsw.format("csv")...	Dumps the Result Table to a file. Supports Parquet, json, csv, etc.
	Kafka	dsw.format("kafka")	Writes the output to one or more topics in Kafka
	Console	dsw.format("console")	Prints data to the console (useful for debugging)
	Memory	dsw.format("memory")	Updates an in-memory table, which can be queried through Spark SQL or the DataFrame API
	foreach	dsw.foreach(writer: ForeachWriter)	This is your "escape hatch", allowing you to write your own type of sink.
	Delta	dsw.format("delta")	A proprietary sink
 # iii. Read From Kafka Source (Kafka Server)

		```

		// Subscribe to 1 topic
		val df = spark
		  .readStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1")
		  .load()
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		  .as[(String, String)]

		// Subscribe to multiple topics
		val df = spark
		  .readStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1,topic2")
		  .load()
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		  .as[(String, String)]

		// Subscribe to a pattern
		val df = spark
		  .readStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribePattern", "topic.*")
		  .load()
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		  .as[(String, String)]
		  
		```	

# Note : Each Row As a Stream of Record : Each row in the source has the following schema:
		```
		Column	Type
		key	binary
		value	binary
		topic	string
		partition	int
		offset	long
		timestamp	long
		timestampType	int

		```

# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")  .as[(String, String)]

# Here key and Value as columns of the stream record



# 3 The output can be defined in a different mode:

Complete Mode - The entire Result Table will be written.
Append Mode - Only new appended rows will be written. (Assume existing rows do not changed.)
Update Mode - Updated rows in the Result Table will be written.

# 4. Selection, Projection, Aggregation

```
case class DeviceData(device: String, deviceType: String, signal: Double, time: DateTime)

val df: DataFrame = ... // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
val ds: Dataset[DeviceData] = df.as[DeviceData]    // streaming Dataset with IOT device data

// Select the devices which have signal more than 10
df.select("device").where("signal > 10")      // using untyped APIs   
ds.filter(_.signal > 10).map(_.device)         // using typed APIs

// Running count of the number of updates for each device type
df.groupBy("deviceType").count()                          // using untyped API

// Running average signal for each device type
import org.apache.spark.sql.expressions.scalalang.typed
ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))    // using typed API


```

# 5. Windows operation on event time

	We want to have a 10-minutes window that report on every 5 minutes:
```
import spark.implicits._

val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

// Group the data by window and word and compute the count of each group
val windowedCounts = words.groupBy(
  window($"timestamp", "10 minutes", "5 minutes"),
  $"word"
).count()


```

# 6. Handling Late Data and Watermarking
		Late arrived data can be updated in the correct window:
      In the update mode, rows in the result table can be updated. To reduce the amount of intermediate in-memory state to maintain, we keep a watermarking as a threshold on how late a data can arrive.

```
import spark.implicits._

val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

// Group the data by window and word and compute the count of each group
val windowedCounts = words
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        $"word")
    .count()
Some sinks (like files) do not supported the updates that the Update Mode required. In Append Mode, new rows will not be appended until the watermarking period has passed so we can account for late arrived data.

```

# 7. Join operations
```

Static DataFrames can be joined with streaming DataFrame:

val staticDf = spark.read. ...
val streamingDf = spark.readStream. ...

streamingDf.join(staticDf, "type")          // inner equi-join with a static DF
streamingDf.join(staticDf, "type", "right_join")  // right outer join with a static DF

```

# 8. Streaming Deduplication


```

To filter duplicate records:

val streamingDf = spark.readStream. ...  // columns: guid, eventTime, ...

// Without watermark using guid column
streamingDf.dropDuplicates("guid")

// With watermark using guid and eventTime columns
streamingDf
  .withWatermark("eventTime", "10 seconds")
  .dropDuplicates("guid", "eventTime")

```

#9. Sink :
  #i File based.
  
  ```
  writeStream
    .format("parquet")        // can be "orc", "json", "csv", etc.
    .option("path", "path/to/destination/dir")
    .start()
	
	```
 # ii.For debugging
```
// Write to console
writeStream
    .format("console")
    .start()
```	
# iii. To memory
```
writeStream
    .format("memory")
    .queryName("tableName")
    .start()		
```
# iv Write into Kafka Topic:
  ```
  
  spark.readStream.format("kafka")
 .option("kafka.bootstartup.servers","host:port")
 .option("subscribe","kafkaTopic")
 .option("startingOffsets","latest")
 .load()
 .selectExpr("CAST(value as STRING) as json ","timestamp")
 .select(from_json(col("json"),streamctl).alias("parsed"))
 .select("parsed.*")
 .withColumn("results",explode($"results"))
 .select("results.user.username")
 .withColumn("value",regexp_replace(col("username"),"([0-9])","")
 .select("Value")
 .writeStream
 .option("checkpointLocation","D:/dev_path/checkpoint")
 .format("kafak")
 .option("kafka.bootstartup.servers","host:ip")
 .option("topic","spark-report-topic")
 .start()
 .awaitTermination()
 
  ```

# Write into foreachBatch (MongoDB)
```
ageAverage
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("complete").foreachBatch { (batchDf: DataFrame, batchId: Long) =>
      val df = batchDf.withColumn("batchId", lit(batchId))
      df.printSchema()
      df.write.format("mongo").mode(SaveMode.Append)
        .option("uri", MongoDBConstants.spark_mongodb_output_uri)
        .option("database", MongoDBConstants.mongodb_database)
        .option("collection", MongoDBConstants.mongodb_collection_tbl)
        .save();
      df.show(20, false);
    }.start();

```
## Write into foreachBatch (MySQL DB) 

```
    ageAverage
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("complete").foreachBatch{(batchDf:DataFrame,batchId:Long) =>
      val df=batchDf.withColumn("batchId",lit(batchId))
      df.printSchema()
      df.write.mode(SaveMode.Append).jdbc(url,"meetup_rsvp_tbl",prop)
      df.show(20,false);

```
#10 .Managing Streaming Query

```
val query = df.writeStream.format("console").start()   // get the query object

query.id          // get the unique identifier of the running query that persists across restarts from checkpoint data

query.runId       // get the unique id of this run of the query, which will be generated at every start/restart

query.name        // get the name of the auto-generated or user-specified name

query.explain()   // print detailed explanations of the query

query.stop()      // stop the query

query.awaitTermination()   // block until query is terminated, with stop() or with error

query.exception       // the exception if the query has been terminated with error

query.recentProgress  // an array of the most recent progress updates for this query

query.lastProgress    // the most recent progress update of this streaming query

```	


#11 Asynchronous API

```

Query listener:

val spark: SparkSession = ...

spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
    }
})

```

# 12 .Recovering from Failures with Checkpointing

```
aggDF
  .writeStream
  .outputMode("complete")
  .option("checkpointLocation", "path/to/HDFS/dir")
  .format("memory")
  .start()
  
 ```
 
