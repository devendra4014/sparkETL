package org.etl.sparketl.targets

import org.apache.spark.sql.SaveMode
import org.etl.sparketl.conf.Target
import org.etl.sparketl.common.{Constant, Resources}

import scala.collection.mutable

class FileWriter {
  private val MODULE: String = "FileWriter"
  private var format: String = _
  private var fileUrl: String = "file:///"
  private var path: String = _
  private var mode: String = _
  private val optionHashMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String].empty

  def writeDataframe(store: Target): Unit = {

    store.getParams.foreach { x =>
      if (x.key.equalsIgnoreCase(Constant.FORMAT)) {
        this.format = x.value
      } else if (x.key.equalsIgnoreCase(Constant.PATH)) {
        path = x.value
      } else if (x.key.equalsIgnoreCase(Constant.MODE)) {
        path = x.value
      }
    }

    // Append is default
    var saveMode: SaveMode = SaveMode.Append
    if (mode != null && mode.equalsIgnoreCase(Constant.OVERWRITE)) {
      saveMode = SaveMode.Overwrite
    }

    val filePath: String = fileUrl + path

    val df = Resources.getDataFrameManger.getDataFrame(store.getDataframe)

    var query = df.write

    df.coalesce(1).write
      .mode(saveMode)
      .format(format)
      .options(optionHashMap)
      .save(filePath)

  }
}
