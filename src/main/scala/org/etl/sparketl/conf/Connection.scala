package org.etl.sparketl.conf


class Connection {

  private val MODULE: String = "Connection"
  private var connectionId: String = _
  private var connectionName: String = _
  private var params: Array[Parameter] = _

  def getConnectionId: String = this.connectionId

  def getParams: Array[Parameter] = this.params

  def setConnectionId(id: String): Unit = this.connectionId = id

  def setParams(params: Array[Parameter]): Unit = this.params = params
}
