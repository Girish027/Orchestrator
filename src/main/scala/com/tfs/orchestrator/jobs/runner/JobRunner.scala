package com.tfs.orchestrator.jobs.runner

import java.util.{Date, Properties}

import com.tfs.orchestrator.jobs.runner.oozie.OozieJobRunner
import com.tfs.orchestrator.properties.{JobScheduleProperties, SystemProperties}

import scala.util.Try

/**
 * Abstraction of Runner utilities.
 */
trait JobRunner {
  /**
   * Initializes the Job runner context.
   */
  def initialize(systemProperties: SystemProperties): Unit


  /**
   * Schedules a given job as a cron.
   * @param jobProperties
   * @return the job Id.
   */
  def scheduleCron(jobProperties: JobScheduleProperties): Option[String]


  /**
   * Schedules a given job as a one time job workflow job.
   * @param jobProperties
   * @return the job Id.
   */
  def scheduleOnce(jobProperties: JobScheduleProperties): Option[String]


  /**
   * Kill an existing Job.
   * @param jobId the jobId that needs to be killed.
   * @return success of the kill.
   */
  def killJob(jobId: String): Boolean

  /**
   * Get the running status of a job.
   * @param jobId the jobId
   * @return whether the job is running or not.
   */
  def isJobAlive(jobId: String): Boolean

  /**
   * Update the job end time.
   * @param jobId the jobId
   * @param endTime end time
   * @return whether the job is running or not.
   */
  def updateJobEndTime(jobId: String, endTime: Date): Boolean


  def change(jobId: String, changeValue: String): Boolean

  /**
   * return an existing Job.
   * @param view_client the view and client combination whose Id is needed
   * @return the running job for view+client.
   */
  def getRunningJobId(view_client : String): Option[String]

  def rerun(wfId:String,conf:Properties): Try[Unit]

}

object JobRunner {

  private var runner: JobRunner = null

  def initialize(systemProperties: SystemProperties): JobRunner = {
    runner =  new OozieJobRunner(systemProperties)
    runner
  }

  /**
   * Factory method for getting a JobRunner instance.
   * Currently there can only be a single instance of JobRunner with which this is
   * initialized.
   * @return
   */
  def getJobRunner(): JobRunner = runner
}
