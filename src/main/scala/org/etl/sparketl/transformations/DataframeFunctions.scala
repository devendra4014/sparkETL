package org.etl.sparketl.transformations

import org.apache.spark.sql.DataFrame

object DataframeFunctions {
  private val MODULE: String = "DataframeFunctions"


  val applyCountRowFunction: DataframeFunction = new DataframeFunction {
    override val argType: String = "DataframeInput"

    override def apply(args: FunctionArgs[_]): DataFrame =
      args match {
        case DataframeInput(dataframeMap, parameter) =>
          dataframeMap("df").selectExpr("count(*) as rowCount")
      }
  }

  val applyFilterFunction: DataframeFunction = new DataframeFunction {
    override val argType: String = "DataframeInput"

    override def apply(args: FunctionArgs[_]): DataFrame =
      args match {
        case DataframeInput(dataframeMap, parameter) =>
          val df: DataFrame = dataframeMap("filterDF")
          val params: Map[String, String] = parameter.map(x => x.key -> x.value).toMap
          val condition = params("condition")

          df.filter(condition)
      }
  }
}
