package com.tfs.orchestrator.utils.quartz

import com.tfs.orchestrator.utils.quartz.handlers.SchedulerHandler
import com.tfs.orchestrator.utils.{Constants, PropertyReader}
import com.tfs.orchestrator.utils.quartz.jobs.{AutoReplayForLateArrivedEventsTask, ExporterTask, RetryableTask, TerminatorTask}
import org.apache.logging.log4j.scala.Logging

object JobInitializer extends Logging {

  private val JOB_GROUP_NAME = "Dataplatform"
  val TERMINATOR_JOB_NAME = "Terminator"
  val RETYRABLE_JOB_NAME = "Retryable"
  val EXPORTER_JOB_NAME = "Exporter"
  val LATE_ARRIVED_EVENT_REPLAY_JOB_NAME = "Autoreplay"

  def initialize(): Unit = {
    scheduleTerminatorJob
    scheduleRetryableJob
    scheduleExporterJob
    scheduleLateArrivedEVentsReplayJob
  }

  private def scheduleTerminatorJob: Unit = {
    SchedulerHandler.triggerRepeatingJob(TERMINATOR_JOB_NAME, JOB_GROUP_NAME, PropertyReader.getApplicationPropertiesValue(Constants.SLA_TERMINATOR_INTERVAL_KEY, Constants.DEFAULT_TERMINATOR_INTERVAL), classOf[TerminatorTask])
  }

  private def scheduleRetryableJob: Unit = {
    SchedulerHandler.triggerRepeatingJob(RETYRABLE_JOB_NAME, JOB_GROUP_NAME, PropertyReader.getApplicationPropertiesValue(Constants.RETRY_INTERVAL, Constants.DEFAULT_RETRY_INTERVAL), classOf[RetryableTask])
  }

  private def scheduleExporterJob: Unit = {
    SchedulerHandler.triggerRepeatingJob(EXPORTER_JOB_NAME, JOB_GROUP_NAME, PropertyReader.getApplicationPropertiesValue(Constants.EXPORTER_JOB_INTERVAL, Constants.DEFAULT_EXPORTER_JOB_INTERVAL), classOf[ExporterTask])
  }

  private def scheduleLateArrivedEVentsReplayJob={
    SchedulerHandler.triggerRepeatingJob(LATE_ARRIVED_EVENT_REPLAY_JOB_NAME,JOB_GROUP_NAME,
      PropertyReader.getApplicationPropertiesValue(Constants.AUTOREPLAY_JOB_INTERVAL, Constants.DEFAULT_AUTOREPLAY_JOB_INTERVAL_SECONDS), classOf[AutoReplayForLateArrivedEventsTask])
  }

  def destroy(): Unit = {
    SchedulerHandler.destroy()
  }

}
