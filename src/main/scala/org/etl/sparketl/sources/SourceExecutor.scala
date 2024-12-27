package org.etl.sparketl.sources

import org.apache.spark.sql.DataFrame
import org.etl.sparketl.common.{Logging, Resources}
import org.etl.sparketl.conf.Source

class SourceExecutor {

  private val MODULE: String = "SourceExecutor"

  def execute(sources: Array[Source]): Unit = {
    sources.foreach { source =>

      try {
        if (source.getSourceType.equalsIgnoreCase("S3")) {
          val s3Reader: S3Reader = new S3Reader()
          val dataFrame = s3Reader.readDataframe(source)
          Resources.getDataFrameManger.setDataFrame(source.getDataframeName, dataFrame)
        }
        else {
          Logging.logInfo(MODULE, source.getSourceType.concat(" is not Supported."))
        }
      }
      catch {
        case exception: Exception =>
          Logging.logError(
            MODULE,
            "[Read Input] [ERROR] Error in input Name: "
              .concat(source.getSourceName)
              .concat(" with input Type: ")
              .concat(source.getSourceType)
          )
          exception.printStackTrace()
          throw new RuntimeException(exception)
      }
    }

  }
}
