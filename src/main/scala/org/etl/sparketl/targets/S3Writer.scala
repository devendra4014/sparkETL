package org.etl.sparketl.targets

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.etl.sparketl.common.{Constant, Logging, Resources}
import org.etl.sparketl.conf.{Parameter, Target}
import sun.security.util.FilePaths

import scala.collection.mutable

class S3Writer {

  private val MODULE: String = "S3Writer"
  private val sparkSession: SparkSession = Resources.getSparkSession
  private var format: String = _
  private var s3ObjectPath: String = _
  private var bucket: String = _
  private var awsAccessKey: String = _
  private var awsSecreteKey: String = _
  private var region: String = _
  private var partitionColumn: String = _
  // private var numBuckets: Int = _
  // private var bucketColumn: String = _
  // private var table: String = _
  private val options: mutable.HashMap[String, String] = new mutable.HashMap[String, String].empty
  private var mode: String = _

  private def setOptions(options: Array[Parameter]): Unit = {
    options.foreach{
      x => this.options.put(x.key, x.value)
    }
  }

  private def setConnection(connectionId: String): Unit = {
    val connParams : Map[String, String] = Resources.getConnectionParams(connectionId).map(x => x.key -> x.value ).toMap

    try {
      awsAccessKey = connParams(Constant.ACCESSKEY)
      awsSecreteKey = connParams(Constant.SECRETKEY)
      region = connParams(Constant.REGION)
    }
    catch {
      case nullPointerException: NullPointerException =>
        throw new NullPointerException("`AccessKey` OR `SecreteKey` OR `Region` is missing.")
    }
  }

  private def setParameters(parameter: Array[Parameter]): Unit = {
    parameter.foreach{
      x =>
        if (x.key.equalsIgnoreCase(Constant.FORMAT)) {
          format = x.value
        } else if (x.key.equalsIgnoreCase(Constant.MODE)) {
          mode = x.value
        } else if (x.key.equalsIgnoreCase(Constant.BUCKET)) {
          bucket = x.value
        } else if (x.key.equalsIgnoreCase(Constant.OBJECT)) {
          s3ObjectPath = x.value
        }
        /*else if (x.key.equalsIgnoreCase("partitionColumn")) {
                this.partitionColumn = x.value
              }
              else if (x.key.equalsIgnoreCase("numBuckets")) {
                this.numBuckets = x.value.toInt
              }
              else if (x.key.equalsIgnoreCase("bucketColumn")) {
                this.bucketColumn = x.value
              }
              else if (x.key.equalsIgnoreCase("table")) {
                this.table = x.value
              }*/
    }
  }

  def loadDataframe(target: Target): Unit = {
    // set connections first
    setConnection(target.getConnectionID)

    // set parameters then
    setParameters(target.getParams)

    if (format != null) {
      if (format.equalsIgnoreCase(Constant.CSV)) {
        options.put("header", "true")
      }
    } else {
      Logging.logInfo(MODULE, "file Format is Not Provided.")
    }

    val filePath: String = "s3a://" + bucket + "/" + s3ObjectPath

    // Append is default
    var saveMode: SaveMode = SaveMode.Append
    if (mode != null && mode.equalsIgnoreCase(Constant.OVERWRITE)) {
      saveMode = SaveMode.Overwrite
    }

    val dataframe: DataFrame = Resources.getDataFrameManger.getDataFrame(target.getDataframe)
    saveDataframe(dataframe, saveMode, filePath)

  }

  private def saveDataframe(df:DataFrame, saveMode: SaveMode, filePaths: String): Unit = {
    sparkSession.sparkContext.hadoopConfiguration
      .set(Constant.ACCESS_KEY_CONF, awsAccessKey)
    sparkSession.sparkContext.hadoopConfiguration
      .set(Constant.SECRET_KEY_CONF, awsSecreteKey)
    sparkSession.sparkContext.hadoopConfiguration
      .set(Constant.S3_END_POINT_CONF, Constant.S3_END_POINT)

    var query = df.write

    if (partitionColumn != null) {
      query = query.partitionBy(partitionColumn)
    }

    query
      .format(format)
      .options(options)
      .mode(saveMode)
      .save(filePaths)

    /*if (numBuckets != null && bucketColumn != null && table != null) {
           query
             .bucketBy(numBuckets, bucketColumn)
             .format(format)
             .options(optionHashMap)
             .mode(saveMode)
             .saveAsTable(table)
        }
        else {
          query
            .format(format)
            .options(optionHashMap)
            .mode(saveMode)
            .save(filePath)
        }*/


  }

}
