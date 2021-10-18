package com.tfs.orchestrator.utils.quartz.jobs

import com.tfs.orchestrator.utils.handlers.RetryableJobHandler
import org.apache.logging.log4j.scala.Logging
import org.quartz.{Job, JobExecutionContext}

class RetryableTask extends Job with Logging {

  initialize

  def initialize: Unit = {
    logger.info(s"${this.getClass.getSimpleName} is initialized.")
  }

  override def execute(context: JobExecutionContext): Unit = {
    logger.info("Retrying failed applications.")
    RetryableJobHandler.retryFailedRetryableJobs()
  }

}
