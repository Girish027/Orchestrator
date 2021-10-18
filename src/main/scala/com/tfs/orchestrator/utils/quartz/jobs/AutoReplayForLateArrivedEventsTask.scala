package com.tfs.orchestrator.utils.quartz.jobs

import com.tfs.orchestrator.utils.handlers.LateArrivedEventsHandler
import org.apache.logging.log4j.scala.Logging
import org.quartz.{Job, JobExecutionContext}

/**
 * Created by Priyanka.N on 29-07-2019.
 */
class AutoReplayForLateArrivedEventsTask  extends Job with Logging {

  initialize

  def initialize: Unit = {
    logger.info(s"${this.getClass.getSimpleName} is initialized.")
  }

  override def execute(context: JobExecutionContext): Unit = {
    logger.info("Started Auto replay service for events arriving late.")
    (new LateArrivedEventsHandler).replayJobsForLateEvents
  }

}
