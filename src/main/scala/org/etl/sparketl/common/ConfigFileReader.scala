package org.etl.sparketl.common

import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.etl.sparketl.conf.{Data, Transformation}

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable

class ConfigFileReader {
  private val MODULE: String = "ConfigFileReader"


  def readConfiguration(): Data = {
    var configFileReader: BufferedReader = null
    val configFilePath: String = Resources.configFile

    if (configFilePath != null) {
      Logging.logInfo(
        MODULE,
        "[readConfiguration] Config File Path: " + configFilePath
      )
    }

    if (
      configFilePath.startsWith("s3a://") && (Resources.accessKey
        .equalsIgnoreCase(Constant.DEFAULT) || Resources.secretKey
        .equalsIgnoreCase(Constant.DEFAULT) || Resources.region
        .equalsIgnoreCase(Constant.DEFAULT))
    ) {
      Logging.logError(
        MODULE,
        "Aws Access key or secret key or Region is missing in cli arguments"
      )
      throw new RuntimeException(
        "Aws Access key or secret key or Region is missing in cli arguments"
      )
    }
    else if (configFilePath != null && configFilePath.startsWith("s3a://")) {
      configFileReader = readFromS3(configFilePath)
    }
    else {
      // Read from local or HDFS fileSystem
      val configuration: Configuration = new Configuration()
      val path: Path = new Path(configFilePath)
      val fileSystem: FileSystem = FileSystem.get(path.toUri, configuration)
      configFileReader = new BufferedReader(
        new InputStreamReader(fileSystem.open(path))
      )
    }

    val configString: String = org.apache.commons.io.IOUtils.toString(configFileReader)

    println("[Read Config] Config File: \n " + configString)
    //var dataPipe: DataPipe = null
    val gson = new GsonBuilder()
      .registerTypeAdapter(new TypeToken[Map[String, Array[String]]]() {}.getType, new GenericMapDeserializer) // Register mutable.Map
      .registerTypeAdapter(new TypeToken[Map[String, String]]() {}.getType, new ImmutableMapDeserializer)
      .registerTypeAdapter(new TypeToken[mutable.Map[String, String]]() {}.getType, new MutableMapDeserializer)
      .registerTypeAdapter(classOf[Transformation], new TransformationDeserializer)
      .create()
    val data: Data = gson.fromJson(configString, classOf[Data])
    data
  }

  private def readFromS3(path: String): BufferedReader = {
    val awsS3Client = getAmazonS3Client(Resources.accessKey, Resources.secretKey, Resources.region)

    val s3filePath: String = path.split("://")(1)
    val bucketName: String = s3filePath.split("/")(0)
    val fileName: String = s3filePath.split("/", 2)(1)
    val obj = awsS3Client.getObject(bucketName, fileName)
    val reader = new BufferedReader(
      new InputStreamReader(obj.getObjectContent)
    )
    reader

  }

  private def getAmazonS3Client(accessKey: String,
                                secretKey: String,
                                region: String): AmazonS3 = {
    val awsCredentials: AWSCredentials =
      new BasicAWSCredentials(accessKey, secretKey)
    AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .withRegion(Regions.fromName(region))
      .build()
  }

}
