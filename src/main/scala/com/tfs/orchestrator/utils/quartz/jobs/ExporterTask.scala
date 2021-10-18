package com.tfs.orchestrator.utils.quartz.jobs

import com.tfs.orchestrator.utils.handlers.ExporterJobHandler
import org.apache.logging.log4j.scala.Logging
import org.quartz.{DisallowConcurrentExecution, InterruptableJob, JobExecutionContext}

@DisallowConcurrentExecution
class ExporterTask extends InterruptableJob with Logging {

  private var isInterrupted = false
  private var exporterJobHandler: Option[ExporterJobHandler] = None

  initialize

  def initialize: Unit = {
    logger.info(s"${this.getClass.getSimpleName} is initialized.")
    try {
      exporterJobHandler = getExporterJobHandlerInstance
    }
    catch {
      case ex: Throwable => logger.error("Unable to initialize exporter job handler.", ex)
    }
  }

  protected def getExporterJobHandlerInstance = {
    Option(new ExporterJobHandler)
  }

  override def execute(context: JobExecutionContext): Unit = {
    if (exporterJobHandler.isDefined) {
      logger.info("Handling exporters.")
      try {
        while (!isInterrupted) {
          exporterJobHandler.get.handleExports()
        }
      }
      catch {
        case e: Exception => {
          logger.error("Unable to handle exports.", e)
        }
      }
      finally {
        logger.info("Pulling down exporter job handler.")
        exporterJobHandler.get.close()
      }
    }
  }

  override def interrupt(): Unit = {
    isInterrupted = true
    if (exporterJobHandler.isDefined)
      exporterJobHandler.get.interrupt
  }
}
