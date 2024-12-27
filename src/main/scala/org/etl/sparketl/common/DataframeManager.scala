package org.etl.sparketl.common

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class DataframeManager {
  val MODULE: String = "DataframeManager"
  private val dataFrameList: mutable.HashMap[String, DataFrame] =
    new mutable.HashMap[String, DataFrame]

  def getDataFrame(dataFrameName: String): DataFrame = {
    try {
      val dataFrame: DataFrame = dataFrameList(dataFrameName)
      dataFrame
    } catch {
      case exception: Exception =>
        Logging.logError(
          MODULE,
          "[Dataframe Manager] [ERROR] Dataframe named: "
            .concat(dataFrameName)
            .concat(" is not present. Please check the config file")
        )
        exception.printStackTrace()
        throw new RuntimeException()
    }
  }

  def setDataFrame(dataFrameName: String, dataFrame: DataFrame): Unit = {
    dataFrameList.put(dataFrameName, dataFrame)
  }

  def updateDataFrame(dataFrameName: String, dataFrame: DataFrame): Unit = {
    dataFrameList.update(dataFrameName, dataFrame)
  }

  def isDataFrame(dataFrameName: String): Boolean = {
    dataFrameList.contains(dataFrameName)
  }

}

