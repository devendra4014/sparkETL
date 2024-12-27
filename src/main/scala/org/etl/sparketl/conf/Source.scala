package org.etl.sparketl.conf

class Source {
  private val MODULE: String = "Source"
  private var sourceName: String = _
  private var sourceType: String = _
  private var dataFrameName: String = _
  private var connectionId: String = _
  private var params: Array[Parameter] = _
  private var options: Array[Parameter] = _
  private var schemaFilePath: String = _
  private var schema: String = _

  def getSourceName: String = sourceName

  def getSourceType: String = sourceType

  def getDataframeName: String = dataFrameName

  def getConnectionId: String = connectionId

  def getParamsMap: Map[String, String] = {
    params.map(x => x.key -> x.value).toMap
  }

  def getOptions: Array[Parameter] = options

  def getSchemaFilePath: String = schemaFilePath

  def getSchema: String = schemaFilePath

  def setSchemaFilePath(path: String): Unit = {
    schemaFilePath = path
  }

  def setSchema(schema: String): Unit = {
    this.schema = schema
  }

  def setOptions(ops: Array[Parameter]): Unit = {
    options = ops
  }

  def setSourceName(name: String): Unit = {
    sourceName = name
  }

  def setSourceType(sourceType: String): Unit = {
    this.sourceType = sourceType
  }

  def setDataframeName(dataframe: String): Unit = {
    dataFrameName = dataframe
  }

  def setConnectionId(id: String): Unit = {
    connectionId = id
  }

  def setParamsMap(params: Array[Parameter]): Unit = {
    this.params = params
  }


}
