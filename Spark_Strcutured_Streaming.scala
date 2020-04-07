Write to Cassandra using foreachBatch() in Scala

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector._

val host = "<ip address>"
val clusterName = "<cluster name>"
val keyspace = "<keyspace>"
val tableName = "<tableName>"

spark.setCassandraConf(clusterName, CassandraConnectorConf.ConnectionHostParam.option(host))
spark.readStream.format("rate").load()
  .selectExpr("value % 10 as key")
  .groupBy("key")
  .count()
  .toDF("key", "value")
  .writeStream
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

    batchDF.write       // Use Cassandra batch data source to write streaming out
      .cassandraFormat(tableName, keyspace)
      .option("cluster", clusterName)
      .mode("append")
      .save()
  }
  .outputMode("update")
  .start()
 
#######################################################################################
#######################################################################################
# Write to Azure SQL Data Warehouse using foreachBatch() in Python
from pyspark.sql.functions import *
from pyspark.sql import *

def writeToSQLWarehouse(df, epochId):
  df.write \
    .format("com.databricks.spark.sqldw") \
    .mode('overwrite') \
    .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>") \
    .option("forward_spark_azure_storage_credentials", "true") \
    .option("dbtable", "my_table_in_dw_copy") \
    .option("tempdir", "wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>") \
    .save()

spark.conf.set("spark.sql.shuffle.partitions", "1")

query = (
  spark.readStream.format("rate").load()
    .selectExpr("value % 10 as key")
    .groupBy("key")
    .count()
    .toDF("key", "count")
    .writeStream
    .foreachBatch(writeToSQLWarehouse)
    .outputMode("update")
    .start()
    )


#######################################################################################
#######################################################################################
# Write to Amazon DynamoDB 
table_name = "PythonForeachTest"

def get_dynamodb():
  import boto3

  access_key = "<access key>"
  secret_key = "<secret key>"
  region = "<region name>"
  return boto3.resource('dynamodb',
                 aws_access_key_id=access_key,
                 aws_secret_access_key=secret_key,
                 region_name=region)

def createTableIfNotExists():
    '''
    Create a DynamoDB table if it does not exist.
    This must be run on the Spark driver, and not inside foreach.
    '''
    dynamodb = get_dynamodb()

    existing_tables = dynamodb.meta.client.list_tables()['TableNames']
    if table_name not in existing_tables:
      print("Creating table %s" % table_name)
      table = dynamodb.create_table(
          TableName=table_name,
          KeySchema=[ { 'AttributeName': 'key', 'KeyType': 'HASH' } ],
          AttributeDefinitions=[ { 'AttributeName': 'key', 'AttributeType': 'S' } ],
          ProvisionedThroughput = { 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
      )

      print("Waiting for table to be ready")

table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

from pyspark.sql.functions import *

spark.conf.set("spark.sql.shuffle.partitions", "1")

query = (
  spark.readStream.format("rate").load()
    .selectExpr("value % 10 as key")
    .groupBy("key")
    .count()
    .toDF("key", "count")
    .writeStream
    .foreach(SendToDynamoDB_ForeachWriter())
    #.foreach(sendToDynamoDB_simple)  // alternative, use one or the other
    .outputMode("update")
    .start()
)	
