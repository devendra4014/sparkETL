package org.etl.sparketl.common

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.etl.sparketl.conf.{Connection, Parameter}

import scala.collection.mutable

object Resources {

  var sparkSession: SparkSession = _
  var configFile: String = _
  var accessKey: String =  _
  var secretKey: String = _
  var region: String = _
  var skipNullDataframe: String = _
  var azureAccessKey: String = _
  var storageName: String = _
  var log4j: String = _
  var connections : Array[Connection] = _
  var dataframeManager: DataframeManager = _
//  var endPointManager: EndPointManager = _


  def setArgVar(args: Array[String]): Unit = {

    val argumentParser: ArgParser = new ArgParser(args)
    configFile = argumentParser.configFile()
    log4j = argumentParser.log4jFile()
    accessKey = argumentParser.accessKey()
    secretKey = argumentParser.secreteKey()
    region = argumentParser.region()
  }

  def getDataFrameManger: DataframeManager = {
    if (dataframeManager == null) {
      dataframeManager = new DataframeManager
    }
    dataframeManager
  }


  def setConnections(conns: Array[Connection]): Unit = {
    this.connections = conns
  }

  def getConnectionParams(id: String): Array[Parameter] = {
    var params: Array[Parameter] = null
      this.connections.foreach { x =>
        if (x.getConnectionId == id)
        {
          params = x.getParams
        }
      }
    params
  }

  def getSparkSession: SparkSession = {
    if (sparkSession == null) {
      // Create SparkConf and set some default configurations
      val sparkConf = new SparkConf()
        .setAppName("Spark Framework - Starting at:" + System.currentTimeMillis())
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .setMaster("local") // Set the master URL (adjust the number of cores as needed)

      sparkSession = SparkSession
        .builder()
        .appName("Spark Framework -  Starting at:" + System.currentTimeMillis())
        .config(sparkConf)
        .getOrCreate()

    }
    sparkSession
  }

  def setSparkSession(spark: SparkSession): Unit = {
    sparkSession = spark
  }



}
