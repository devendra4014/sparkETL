package org.etl.sparketl.common

import org.apache.log4j.{Level, LogManager, Logger, PropertyConfigurator}

object Logging {

  // private var logger: org.apache.logging.log4j.Logger =
  // org.apache.logging.log4j.Logger
  LogManager.getRootLogger.setLevel(Level.INFO)
  private val logger: Logger = LogManager.getLogger(classOf[Logger])

  private val log4jFilePath: String = Resources.log4j

  if (log4jFilePath != "") {
    PropertyConfigurator.configure(log4jFilePath)
  }
  /** The Constant MESSAGE_PREFIX. */
  private val MESSAGE_PREFIX = "[ "

  /** The Constant MESSAGE_CONCATOR. */
  private val MESSAGE_CONCATOR = " ] : "


  def logDebug(moduleName: String, strMessage: String): Unit = {
    logger.debug(MESSAGE_PREFIX + moduleName + MESSAGE_CONCATOR + strMessage)
  }

  def logInfo(moduleName: String, strMessage: String): Unit = {
    logger.info(MESSAGE_PREFIX + moduleName + MESSAGE_CONCATOR + strMessage)
  }

  def logError(moduleName: String, strMessage: String): Unit = {
    logger.debug(MESSAGE_PREFIX + moduleName + MESSAGE_CONCATOR + strMessage)
  }

}
