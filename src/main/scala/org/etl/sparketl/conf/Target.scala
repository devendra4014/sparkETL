package org.etl.sparketl.conf

class Target {
  private val MODULE: String = "Target"
  private var targetName: String = _
  private var targetType: String = _
  private var dataframe: String = _
  private var connectionId: String = _
  private var params: Array[Parameter] = _
  private var options: Array[Parameter] = _


  def getTargetName: String = targetName

  def getTargetType: String = targetType

  def getConnectionID: String = connectionId

  def getDataframe: String = dataframe

  def getParams: Array[Parameter] = params

  def getOptions: Array[Parameter] = options

  def setTargetName(name: String): Unit = {
    targetName = name
  }

  def setDataframe(df: String): Unit = {
    dataframe = df
  }

  def setTargetType(targetType: String): Unit = {
    this.targetType = targetType
  }

  def setConnectionID(conn: String): Unit = {
    connectionId = conn
  }

  def setParams(parameter: Array[Parameter]): Unit = {
    params = parameter
  }

  def setOptions(options: Array[Parameter]): Unit = {
    this.options = options
  }

}
