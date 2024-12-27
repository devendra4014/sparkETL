package org.etl.sparketl.transformations

object DataframeFunctionRegistry {


  private val functions: Map[String, DataframeFunction] = Map(
    "ROW_COUNT" -> DataframeFunctions.applyCountRowFunction,
    "filterFunction" -> DataframeFunctions.applyFilterFunction
  )

  def getFunction(funName: String): Option[DataframeFunction] = functions.get(funName).map(identity)

}
