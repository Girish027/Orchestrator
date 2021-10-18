package com.tfs.orchestrator.utils.quartz.jobs

import com.tfs.orchestrator.utils.handlers.TerminationHandler
import org.apache.logging.log4j.scala.Logging
import org.quartz.{Job, JobExecutionContext}

class TerminatorTask extends Job with Logging {

  initialize

  def initialize: Unit = {
    logger.info(s"${this.getClass.getSimpleName} is initialized.")
  }

  override def execute(context: JobExecutionContext): Unit = {
    logger.info("Terminating applications.")
    TerminationHandler.terminateApplications()
  }
}
