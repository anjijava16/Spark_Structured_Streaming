package com.mmtechsoft.spark.operations.mongodb

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkStructuredStreaming {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR");


    val INPUT_DIRECTORY = "M:\\sai_workspace\\KafkaStreaming\\spark_spin-master\\data\\"

    //2- Define the input data schema
    val personSchema = new StructType()
      .add("firstName", "string")
      .add("lastName", "string")
      .add("sex", "string")
      .add("age", "long");

    val personStream = spark.readStream.schema(personSchema).json(INPUT_DIRECTORY)
    //4 - Create a temporary table so we can use SQL queries
    // 4 - Create a temporary table so we can use SQL queries

    personStream.createOrReplaceTempView("people")

    val sql = "SELECT AVG(age) as average_age, sex FROM people GROUP BY sex"
    val ageAverage = spark.sql(sql)
    ageAverage.printSchema()
    //ageAverage.show(10,false);

    val prop = new java.util.Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")

    //  jdbc:mysql://localhost:3306/sonoo","root","root");
    val url = "jdbc:mysql://localhost:3306/meetup_db"
    //5 - Write the the output of the query to the consold//5 - Write the the output of the query to the consold

    /**
     *ageAverage.writeStream.outputMode("complete").foreachBatch{(batchDf:DataFrame,batchId:Long) =>
     * val df=batchDf;
     *df.printSchema()
     *df.show(20,false);
     * }.start();
     *
     */


    val query = ageAverage.writeStream.trigger(Trigger.ProcessingTime("10 seconds")).outputMode("complete").format("console").start

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



    query.awaitTermination()

  }
}
