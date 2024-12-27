package org.etl.sparketl.conf

class Data {

  private val MODULE: String = "Data"
  private var projectId: String = _
  private var projectName: String = _
  private var componentId: String = _
  private var componentName: String = _
  private var connections: Array[Connection] = _
  private var sources: Array[Source] = _
  private var pipelines: Array[Pipeline] = _
  private var targets: Array[Target] = _

  def getPipelines: Array[Pipeline] = pipelines

  def getComponentName: String = this.componentName

  def getProjectName: String = this.projectName

  def getProjectId: String = this.projectId

  def getComponentId: String = this.componentId

  def getSources: Array[Source] = this.sources

  def getConnection: Array[Connection] = this.connections

  def getTargets: Array[Target] = targets

  def setTargets(targets: Array[Target]): Unit = {
    this.targets = targets
  }

  def setComponentName(componentName: String): Unit = {
    this.componentName = componentName
  }

  def setProjectName(projectName: String): Unit = {
    this.projectName = projectName
  }

  def setProjectId(projectId: String): Unit = {
    this.projectId = projectId
  }

  def setComponentId(componentId: String): Unit = {
    this.componentId = componentId
  }

  def setConnections(conns: Array[Connection]): Unit = {
    connections = conns
  }

  def setPipelines(pipelines: Array[Pipeline]): Unit = {
    this.pipelines = pipelines
  }

}
