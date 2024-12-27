package org.etl.sparketl.transformations

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.etl.sparketl.conf.Parameter

sealed trait FunctionArgs[P] {
  val args: P
}

trait ColumnFunction {
  val argType: String

  def apply(args:FunctionArgs[_]): Column
}

case class SingleColumn(args: (Column), parameter: Array[Parameter]) extends FunctionArgs[(Column)]


trait DataframeFunction {
  val argType: String

  def apply(args: FunctionArgs[_]): DataFrame
}

case class DataframeInput(args: Map[String, DataFrame], parameter: Array[Parameter]) extends FunctionArgs[Map[String, DataFrame]]


/*
// this DataStructure can hold both "DataFrame" and "RelationalGroupedDataset" at the same time
sealed trait SparkDataFrame[T] {
  val data: T
}
case class NormalDF(data:DataFrame) extends SparkDataFrame[DataFrame]
case class RelationalDF(data: RelationalGroupedDataset) extends SparkDataFrame[RelationalGroupedDataset]
*/
