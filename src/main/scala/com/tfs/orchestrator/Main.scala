package com.tfs.orchestrator

import com.tfs.orchestrator.catalog.polling.CatalogInitializer
import com.tfs.orchestrator.exceptions.InitializationException
import com.tfs.orchestrator.hibernate.utils.HibernateUtils
import com.tfs.orchestrator.jobs.runner.JobRunner
import com.tfs.orchestrator.managers.UndertowManager
import com.tfs.orchestrator.properties.SystemPropertiesObject
import com.tfs.orchestrator.utils.quartz.JobInitializer
import com.tfs.orchestrator.utils.{Constants, PropertyReader}
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging

/**
  * Main class that initializes all the services.
  */
object Main extends Logging {
  def main(args: Array[String]): Unit = {
    ThreadContext.put("service", "Orchestrator")

    try {
      initializeRuntimeHook()
      PropertyReader.initialize(false, System.getProperty(Constants.COMMAND_PROPS_DIR_KEY))
      HibernateUtils.initialize()
      JobRunner.initialize(SystemPropertiesObject())
      CatalogInitializer.initialize()
      JobInitializer.initialize()
      val undertow = new UndertowManager
      undertow.start()
      logger.info("System is successfully initialized.")
    } catch {
      case ex: InitializationException => {
        logger.error("Orchestrator initialization failed. Exiting.", ex)
        System.exit(1)
      }
      case ex: Exception => logger.error("Unexpected Exception while initializing.", ex)
    }
  }

  private def cleanUp = {
    try {
      JobInitializer.destroy()
    }
    catch {
      case ex: RuntimeException => logger.error("Unexpected Exception while cleanup.", ex)
    }
  }

  /**
    * Registers a runtimehook to cleanup at the time of shutting down.
    */
  def initializeRuntimeHook(): Unit = {
    logger.debug("Initializing runtime hook")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run = {
        logger.info("Shutdown hook handler invoked")
        cleanUp
      }
    })

  }
}
