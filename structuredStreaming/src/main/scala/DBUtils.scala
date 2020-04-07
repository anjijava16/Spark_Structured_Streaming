object DBUtils {


  val mongodb_host_name="localhost";
  val mongodb_port="27017";
  val mongodb_username="admin";
  val mongodb_password="admin";
  val mongodb_database="";
  val mongodb_collection_tbl="";


  val spark_mongodb_output_uri="mongodb://"+mongodb_username+":"+mongodb_password+"@"+mongodb_host_name+":"+mongodb_port;
}
