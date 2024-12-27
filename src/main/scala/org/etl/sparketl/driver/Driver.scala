package org.etl.sparketl.driver

import org.etl.sparketl.common.{ConfigFileReader, Resources}
import org.etl.sparketl.conf.Data
import org.etl.sparketl.sources.SourceExecutor
import org.etl.sparketl.targets.TargetExecutor
import org.etl.sparketl.transformations.PipelineExecutor


object Driver {


  def main(args: Array[String]): Unit = {

    // set command line arguments
    Resources.setArgVar(args)

    val readConfigFile: ConfigFileReader = new ConfigFileReader()
    val dataPipe: Data = readConfigFile.readConfiguration()

    Resources.setConnections(dataPipe.getConnection)

    val sourceExecutor: SourceExecutor = new SourceExecutor()
    sourceExecutor.execute(dataPipe.getSources)

    val pipelineExecutor: PipelineExecutor = new PipelineExecutor()
    pipelineExecutor.execute(dataPipe.getPipelines)

    val targetExecutor: TargetExecutor = new TargetExecutor()
    targetExecutor.execute(dataPipe.getTargets)
  }

}
