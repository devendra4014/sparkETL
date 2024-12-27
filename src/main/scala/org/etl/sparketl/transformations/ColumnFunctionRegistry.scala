package org.etl.sparketl.transformations




object ColumnFunctionRegistry {

  private val functions : Map[String, ColumnFunction] = Map(
    "tokenCount" -> ColumnLevelFunctions.applyCountTokenFunction,
    "lower" -> ColumnLevelFunctions.applyLowerCase,
    "upper" -> ColumnLevelFunctions.applyUpperCase
  )

  def getFunction(name: String): Option[ColumnFunction] = functions.get(name).map(identity)
}
