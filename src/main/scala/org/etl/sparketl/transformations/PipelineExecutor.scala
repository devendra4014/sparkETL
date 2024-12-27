package org.etl.sparketl.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.etl.sparketl.common.{Logging, Resources}
import org.etl.sparketl.conf.{ColumnTransformation, DataFrameTransformation, Parameter, Pipeline}

import scala.collection.mutable

class PipelineExecutor {
  private val MODULE: String = "PipelineExecutor"
  private var finalDataframe: DataFrame = _

  private def createColumnInput(argType: String, colName: String, params: Array[Parameter]): FunctionArgs[_] = {
    argType match {
      case "SingleColumn" =>
        SingleColumn(col(colName), params)

      case _ => throw new IllegalArgumentException(s"Unsupported argument type: $argType")

    }
  }

  private def createDataframeInput(argType: String, dataframeMap: Map[String, DataFrame], params: Array[Parameter]): FunctionArgs[_] = {
    argType match {
      case "DataframeInput" =>
        DataframeInput(dataframeMap, params)

      case _ => throw new IllegalArgumentException(s"Unsupported argument type: $argType")

    }
  }

  def execute(pipelines: Array[Pipeline]): Unit = {
    pipelines.foreach { pipeline =>

      try {

        println(s"Executing Pipeline : ${pipeline.pipelineName}")

        val dataframeMap: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

        // get the input dataframes from the Dataframe Manager
        pipeline.inputDataFrames.foreach { dfName =>
          if (Resources.getDataFrameManger.isDataFrame(dfName)) {
            dataframeMap.put(dfName, Resources.getDataFrameManger.getDataFrame(dfName))
          }
          else {
            Logging.logError(MODULE, s"Dataframe with name : ${dfName}  is not found. Please check Dataframe Name.")
            throw new RuntimeException(s"Dataframe '$dfName' not found in the DataFrame Manager.")
          }
        }

        // select the required columns only for each input dataframe
        pipeline.retainColumns.foreach { case (dfName, cols) =>
          if (dataframeMap.contains(dfName)) {
            val tempDF = dataframeMap(dfName).select(cols.map(col): _*)
            dataframeMap.update(dfName, tempDF)
          } else {
            Logging.logInfo(MODULE, "Dataframe not found. Dataframe name mismatch in the retainColumns.")
          }
        }

        // apply each transformation one by one whether they are column level or dataframe level
        finalDataframe = pipeline.transformations.foldLeft(Resources.getSparkSession.emptyDataFrame) { (accDF, transformation) =>
          transformation match {
            // column level transformation
            case colTransformer: ColumnTransformation =>
              /*
                 get the working dataframe if inputDataframe == "previousDF" means want to apply column level transformation on previously accumulated dataframe i.e. accDF
                 else means we are apply function for the first time, Hence take working df from Dataframe-manager
                 */
              val workingDF: DataFrame = if (colTransformer.inputDataframe.equalsIgnoreCase("previousDF")) {
                accDF
              } else dataframeMap(colTransformer.inputDataframe)

              val transformedDF: DataFrame = colTransformer.operations.foldLeft(workingDF) { (accDF1, operation) =>
                val columnName: String = if (colTransformer.operations.head.function == operation.function) {
                  colTransformer.columnName
                } else colTransformer.outputColumn
                val outputColumn: String = colTransformer.outputColumn

                val function = ColumnFunctionRegistry.getFunction(operation.function)

                function match {
                  case Some(udfFunction) =>
                    val functionArgs: FunctionArgs[_] = createColumnInput(udfFunction.argType, columnName, operation.parameters)
                    val transformedColumn = udfFunction(functionArgs)

                    println("output for function:" + operation + " is : " + transformedColumn)
                    // Replace the outputCol with the transformed column in the DataFrame
                    accDF1.withColumn(outputColumn, transformedColumn)
                }
              }

              transformedDF

            case dfTransformer: DataFrameTransformation =>
              val dfMap: Map[String, DataFrame] = dfTransformer.functionDataFrames.map { x =>
                x._1 -> (if (x._2.equalsIgnoreCase("previousDF")) accDF else dataframeMap(x._2))
              }

              val dfFunction = DataframeFunctionRegistry.getFunction(dfTransformer.function)

              dfFunction match {
                case Some(function) =>

                  val functionArgs: FunctionArgs[_] = createDataframeInput(function.argType, dfMap, dfTransformer.params)
                  function(functionArgs)
                case _ => throw new IllegalArgumentException(s"Dataframe level function : `${dfTransformer.function}` not found. Please check function name Again.")
              }

          }

        }
        finalDataframe.show()
        finalDataframe.printSchema()
        Resources.getDataFrameManger.setDataFrame(pipeline.outputDataFrameName, finalDataframe)

      } catch {
        case exception: Exception =>
          Logging.logError(MODULE, "[Exception] : ".concat(exception.getMessage))
          exception.printStackTrace()
          throw new RuntimeException(exception)
      }
    }
  }

}
