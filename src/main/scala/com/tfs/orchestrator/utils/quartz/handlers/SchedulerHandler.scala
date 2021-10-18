package com.tfs.orchestrator.utils.quartz.handlers

import java.util.Date

import org.apache.logging.log4j.scala.Logging
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import scala.util.{Failure, Success, Try}

object SchedulerHandler extends Logging {

  private lazy val scheduler: Option[Scheduler] = initialize()

  private def initialize(): Option[Scheduler] = {
    logger.info("Initializing scheduler handler.")
    try {
      val scheduler = new StdSchedulerFactory().getScheduler()
      scheduler.start()
      return Option(scheduler)
    }
    catch {
      case ex: Exception => {
        logger.error("Unable to initialize scheduler handler", ex)
      }
    }
    None
  }


  def triggerRepeatingJob[T <: Job](jobName: String, groupName: String, interval: Int, cls: Class[T]): Unit = {

    if (scheduler.isDefined) {
      val job = jobBuilder[T](jobName, groupName, cls)
      val trigger = triggerBuilder(jobName, groupName, interval)

      scheduleJob(job, trigger) match {
        case Success(_) => logger.info(s"Successfully triggered job for:$jobName")
        case Failure(ex) => logger.error(s"Unable to trigger job for:$jobName", ex)
      }
    } else {
      logger.error(s"Scheduler is not initialized. Unable to trigger job for:$jobName")
    }

  }

  def destroy():Unit={
    logger.info("Destroying schedulers")
    if (scheduler.isDefined) {
      try{
        scheduler.get.shutdown(true)
        logger.info("Successfully destroyed all schedulers")
      }
      catch {
        case ex:Exception => logger.error("Unable to destroy schedulers.")
      }
    }
    else{
      logger.error("Scheduler is not initialized. Unable to destroy scheduler")
    }
  }

  private def scheduleJob[T <: Job](job: JobDetail, trigger: SimpleTrigger): Try[Date] = {
    Try(scheduler.get.scheduleJob(job, trigger))
  }

  private def triggerBuilder[T <: Job](jobName: String, groupName: String, interval: Int): SimpleTrigger = {
    TriggerBuilder
      .newTrigger()
      .withIdentity(jobName, groupName)
      .withSchedule(
        SimpleScheduleBuilder.simpleSchedule()
          .withIntervalInSeconds(interval).repeatForever())
      .build()
  }

  private def jobBuilder[T <: Job](jobName: String, groupName: String, cls: Class[T]): JobDetail = {
    JobBuilder.newJob(cls).withIdentity(jobName, groupName).build
  }
}
