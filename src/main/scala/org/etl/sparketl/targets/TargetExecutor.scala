package org.etl.sparketl.targets

import org.etl.sparketl.common.Logging
import org.etl.sparketl.conf.Target

class TargetExecutor {

  private val MODULE: String = "TargetExecutor"

  def execute(targets: Array[Target]): Unit = {
    targets.foreach { target =>

      try {
        if (target.getTargetType.equalsIgnoreCase("file")) {
          val fileWriter: FileWriter = new FileWriter()
          fileWriter.writeDataframe(target)
        }
        else if (target.getTargetType.equalsIgnoreCase("S3")) {
          val s3Writer: S3Writer = new S3Writer()
          s3Writer.loadDataframe(target)
        }
        else {
          println(s"Target Type : {${target.getTargetType}} is not supported.")
        }
      }
      catch {
        case exception: Exception =>
          Logging.logError(
            MODULE,
            "[Read Input] [ERROR] Error in input Name: "
              .concat(target.getTargetName)
              .concat(" with input Type: ")
              .concat(target.getTargetType)
          )
          exception.printStackTrace()
          throw new RuntimeException(exception)


      }
    }

  }
}
