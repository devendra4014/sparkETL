package org.etl.sparketl.conf

import scala.collection.mutable


// Base trait Transformation (sealed for pattern matching exhaustiveness)
sealed trait Transformation {
  def transformationType: String
}

case class Operation
(
  function: String,
  parameters: Array[Parameter]
)

// Case class for DataFrame transformations
case class DataFrameTransformation(
                                    transformationType: String = "DataframeLevel",
                                    function: String,
                                    params: Array[Parameter],
                                    functionDataFrames: Map[String, String]
                                  ) extends Transformation

// Case class for Column transformations
case class ColumnTransformation(
                                 transformationType: String = "ColumnLevel",
                                 inputDataframe: String = "previousDF",
                                 operations: Array[Operation],
                                 columnName: String,
                                 outputColumn: String
                               ) extends Transformation


// Pipeline case class
case class Pipeline(
                     pipelineName: String,
                     inputDataFrames: Array[String], // Assuming this is an array of DataFrame names
                     retainColumns: Map[String, Array[String]] , // Columns to retain after transformations
                     outputDataFrameName: String, // Fixed camelCase spelling
                     transformations: Array[Transformation]
                   )


