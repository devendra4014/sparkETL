package org.etl.sparketl.sources

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.etl.sparketl.common.{Constant, Logging, Resources}
import org.etl.sparketl.conf.{Parameter, Source}

import scala.collection.mutable

class S3Reader {

  private var fileCount: Int = _
  private var awsAccessKeyId: String = _
  private var awsSecretAccessKey: String = _
  private var region: String = _
  private var bucket: String = _
  private var s3ObjectPath: String = _
  private var schema: StructType = _
  private var schemaPath: String = _
  private var inputDataFrame: DataFrame = _
  private var fileFormat: String = _
  private val sparkSession: SparkSession = Resources.getSparkSession

  private var optionHashMap: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()
  private val MODULE: String = "S3Reader"


  private def setConnectionParameters(connectionParams: Array[Parameter]): Unit = {
    val connectionParamsMap: Map[String, String] = connectionParams.map(x => x.key -> x.value).toMap
    //      Resources.getConnectionParams(source.getConnectionId).map(x => x.key -> x.value).toMap

    try {
      awsAccessKeyId = connectionParamsMap(Constant.ACCESSKEY)
      awsSecretAccessKey = connectionParamsMap(Constant.SECRETKEY)
      region = connectionParamsMap(Constant.REGION)
    }
    catch {
      case nullPointerException: Exception =>
        Logging.logInfo(
          MODULE,
          Constant.ACCESSKEY
            .concat(", ")
            .concat(Constant.SECRETKEY)
            .concat(
              " or Region (if null default is us-west-2) is missing from EndPoint." +
                " Please check config "
            )
        )
        nullPointerException.printStackTrace()
        throw new RuntimeException(nullPointerException)
    }

  }

  private def setSourceParameters(source: Source): Unit = {
    val sourceParametersMap: Map[String, String] = source.getParamsMap

    try {
      bucket = sourceParametersMap(Constant.BUCKET)
      fileFormat = sourceParametersMap(Constant.FORMAT)
      s3ObjectPath = sourceParametersMap(Constant.OBJECT)
    }
    catch {
      case nullPointerException: Exception =>
        Logging.logInfo(
          MODULE,
          (
            "Bucket or object or format is missing. Please provide this parameters."
            )
        )
        nullPointerException.printStackTrace()
        throw new RuntimeException(nullPointerException)
    }
  }

  private def setOptions(source: Source): Unit = {
    if (fileFormat.equalsIgnoreCase(Constant.JSON)) {
      // As JSON is multiline to adding option for multiline
      // and mode is PERMISSIVE so corrupt record will be converted to null
      // Header = true is by default true so skipping this option.
      optionHashMap.put(Constant.MULTILINE, Constant.TRUE)
      optionHashMap.put(Constant.MODE, Constant.PERMISSIVE)
    } else {
      // For other formats use header = true
      optionHashMap.put(Constant.HEADER, Constant.TRUE)
    }

    source.getOptions.foreach(x => optionHashMap.put(x.key, x.value))

    // schema and schema file option set up
    if (source.getSchemaFilePath != null) {
      val schemaPath = source.getSchemaFilePath
      if (fileFormat.equalsIgnoreCase(Constant.JSON)) {
        this.schema = sparkSession.read.json(schemaPath).schema
      } else if (fileFormat.equalsIgnoreCase(Constant.CSV)) {
        this.schema = sparkSession.read
          .option(Constant.HEADER, Constant.TRUE)
          .csv(schemaPath)
          .schema
      }
    } else if (
      source.getSchema != null && this.fileFormat.equalsIgnoreCase(Constant.CSV)
    ) {
      this.schema = StructType(
        source.getSchema
          .split(",")
          .map(fieldName ⇒ StructField(fieldName, StringType, nullable = true))
      )
    } else if (
      this.fileFormat.equalsIgnoreCase(Constant.CSV) || this.fileFormat
        .equalsIgnoreCase(Constant.JSON)
    ) {
      Logging.logError(
        MODULE,
        "schema and schemaFilePath:is Empty. Please define a schemaFilePath  or schema to skip this validation."
      )
    }

  }

  def readDataframe(source: Source): DataFrame = {
    setConnectionParameters(Resources.getConnectionParams(source.getConnectionId))
    setSourceParameters(source)
    setOptions(source)

    /*sparkSession.sparkContext.hadoopConfiguration
      .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
*/
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkSession.sparkContext.hadoopConfiguration
      .set(Constant.ACCESS_KEY_CONF, awsAccessKeyId)
    sparkSession.sparkContext.hadoopConfiguration
      .set(Constant.SECRET_KEY_CONF, awsSecretAccessKey)
    sparkSession.sparkContext.hadoopConfiguration
      .set(Constant.S3_END_POINT_CONF, Constant.S3_END_POINT)
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    val filePath: String = "s3a://"
      .concat(bucket)
      .concat("/")
      .concat(s3ObjectPath)

    if (
      this.fileFormat
        .equalsIgnoreCase(Constant.CSV) || this.fileFormat.equalsIgnoreCase(
        Constant.JSON
      ) && source.getSchemaFilePath != null || source.getSchema != null
    ) {
      inputDataFrame = sparkSession.read
        .format(fileFormat)
        .options(optionHashMap)
        .schema(this.schema)
        .load(filePath)
    } else {
      inputDataFrame = sparkSession.read
        .format(fileFormat)
        .options(optionHashMap)
        .load(filePath)
    }
    inputDataFrame.printSchema()
    inputDataFrame
  }


}
