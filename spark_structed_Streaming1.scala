
#######################################################################################  
#######################################################################################
#######################################################################################

https://stackoverflow.com/questions/46198243/how-to-continuously-monitor-a-directory-by-using-spark-structured-streaming/46198303#46198303
https://stackoverflow.com/questions/42621803/how-to-read-csv-files-using-spark-streaming-and-write-to-parquet-file-using-sca
import org.apache.spark.sql.types._
// Read all the csv files written atomically in a directory
val userSchema = new StructType().add("name", "string").add("age", "string")
val csvDF = spark
  .readStream
  .option("sep", "|")
  .schema(userSchema)      // Specify schema of the csv files
  .csv("D:/files/") 
  

#######################################################################################  
#######################################################################################
#######################################################################################
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._

//StreamingExamples.setStreamingLogLevels()
val sparkConf = new SparkConf().setAppName("HdfsWordCount").set(“spark.driver.allowMultipleContexts”, “true”);

// Create the context
val ssc = new StreamingContext(sparkConf, Seconds(5))
// Create the FileInputDStream on the directory and use the
// stream to count words in new files created
val lines = ssc.textFileStream("D:/files/")

// val words = lines.flatMap(_.split(" "))

// Get the lines, split them into words, count the words and print
//val lines = messages.map(x => x.value)

lines.foreachRDD { rdd =>
import spark.implicits._
val split = rdd.map(line => line.split("\\|")).map(x => (x(0), x(1), x(2), x(3)))
val myDf = split.toDF("field1", "field2", "field3", "field4")
myDf.printSchema()
//myDf.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark_kafka_mysql_db?user=root&password=root","streams_tbl",connectionProperties)
myDf.show(10)
}
ssc.start()
ssc.awaitTermination()


#######################################################################################  
#######################################################################################
#######################################################################################
