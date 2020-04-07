import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

case class Status(id:Int,fname:String,lname:String,startdate:Timestamp)


object RDDDB {


  def sparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Practice2")
    val sc = new SparkContext(conf)
    return sc;
  }

  def getTimestamp(x: Any): Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
    if (x.toString() == "")
      return null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      return t
    }
  }


  def parse(rdd: org.apache.spark.rdd.RDD[String]) = {

    val rdd = sparkContext().textFile("M:/files/input.csv");
    var splitByComm = rdd.map(_.split(","))
    val statusRows = splitByComm.map(cols => Status(cols(0).toInt, cols(1), cols(2), getTimestamp(cols(3))))
    statusRows.foreach(x => println(x));


  }


  def main(args: Array[String]): Unit = {

    val rdd = sparkContext().textFile("M:/files/input.csv");
    var splitByComm = rdd.map(_.split(","))
    val statusRows = splitByComm.map(cols => Status(cols(0).toInt, cols(1), cols(2), getTimestamp(cols(3))))
    statusRows.foreach(x => println(x));


  }
}
