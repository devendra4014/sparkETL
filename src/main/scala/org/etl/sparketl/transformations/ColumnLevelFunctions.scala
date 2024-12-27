package org.etl.sparketl.transformations

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{count, lower, size, split, upper}

object ColumnLevelFunctions {
  private val MODULE: String = "ColumnLevelFunctions"

  val applyCountTokenFunction: ColumnFunction = new ColumnFunction {
    override val argType: String = "SingleColumn"

    override def apply(args: FunctionArgs[_]): Column = {
      args match {
        case SingleColumn(column, parameter) => size(split(column, " "))

        case _ => throw new IllegalArgumentException("Invalid arguments")

      }
    }
  }

  val applyUpperCase: ColumnFunction = new ColumnFunction {
    override val argType: String = "SingleColumn"

    override def apply(args: FunctionArgs[_]): Column = {
      args match {
        case SingleColumn(column, parameter) => upper(column)

        case _ => throw new IllegalArgumentException("Invalid arguments")

      }
    }
  }

  val applyLowerCase: ColumnFunction = new ColumnFunction {
    override val argType: String = "SingleColumn"

    override def apply(args: FunctionArgs[_]): Column = {
      args match {
        case SingleColumn(column, parameter) => lower(column)

        case _ => throw new IllegalArgumentException("Invalid arguments")

      }
    }
  }

}
