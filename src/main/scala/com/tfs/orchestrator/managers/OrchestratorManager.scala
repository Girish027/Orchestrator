package com.tfs.orchestrator.managers

import java.util.Date

import com.tfs.orchestrator.exceptions.{RestClientException, HibernateException, ScheduleException}
import com.tfs.orchestrator.hibernate.entities.ViewClientJobId
import com.tfs.orchestrator.hibernate.service.ViewClientJobIdService
import com.tfs.orchestrator.jobs.runner.JobRunner
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.utils.{Constants, DateUtils}
import org.apache.logging.log4j.scala.Logging

/**
  * The cache poller is expected to call this functions. This class extracts the necessary
  * information from cache/view definition and accordingly invokes the JobRunner
  * for schedules/modifying the workflows.
  */
class OrchestratorManager extends Logging {

  val jobRunner = JobRunner.getJobRunner()
  val viewClientJobIdService = new ViewClientJobIdService

  /**
    * Schedule the cron job for given config. Save the info the db.
    *
    * @param jobProperties
    * @return
    */
  def createCronJob(jobProperties: JobScheduleProperties): Boolean = {
    logger.info(s"Schedule a workflow for ${jobProperties.viewName} for client ${jobProperties.clientIdentifier}" +
      s" from start: ${jobProperties.jobStartTime} to end: ${jobProperties.jobEndTime}")
    var cronJobId: Option[String] = None
    var clientJobId: Option[ViewClientJobId] = None
    try {

      val viewClient: String = jobProperties.viewName + "_" + jobProperties.clientIdentifier;
      val coordJobId: Option[String] = getRunningJobId(viewClient)
      logger.info(s"Coord Job Id : ${coordJobId}")
      /*      if(coordJobId != None) {
              cronJobId = coordJobId
            }else{
              cronJobId = scheduleCron(jobProperties)
            }*/

      if (coordJobId != None) {
        logger.info(s"Job already running with coord id : $coordJobId")
      }

      cronJobId = scheduleCron(jobProperties)
      logger.info(s"Scheduled new job with coord id : $cronJobId")
      logger.info(s"Scheduled a workflow for ${jobProperties.viewName} for client ${jobProperties.clientIdentifier}" +
        s" from start: ${jobProperties.jobStartTime} to end: ${jobProperties.jobEndTime}")

    } catch {
      case e: RestClientException => {
        logger.error(s"Failed to get response from oozie for ${jobProperties.viewName} for client " +
          s"${jobProperties.clientIdentifier}", e)
        return false;
      }
      case e: Exception => {
        logger.error(s"Failed to schedule a workflow for ${jobProperties.viewName} for client " +
          s"${jobProperties.clientIdentifier} from start: ${jobProperties.jobStartTime} to " +
          s"end: ${jobProperties.jobEndTime}", e)
        return false;
      }
    }

    if (cronJobId.isDefined) {
      clientJobId = Some(createViewClientJobId(jobProperties, cronJobId.get))
      logger.debug(s"Client Job Id : ${clientJobId}")
    } else {
      return false
    }

    //Save the info to the db.
    try {
      saveViewClientJobId(clientJobId)
    } catch {
      //We could not save the job details to job. We should kill the job to avoid inconsistency in Orchestrator state
      // and job duplications in future
      case ex: HibernateException => {
        try {
          jobRunner.killJob(cronJobId.get)
          logger.warn(s"Failed to save the workflow info ${jobProperties.viewName} for client " +
            s"${jobProperties.clientIdentifier} from start: ${jobProperties.jobStartTime} to " +
            s"end: ${jobProperties.jobEndTime}", ex)
          return false
        } catch {
          case ex: ScheduleException => {
            logger.error(s"Job scheduled but information not saved ${jobProperties.viewName} for client " +
              s"${jobProperties.clientIdentifier} from start: ${jobProperties.jobStartTime} to " +
              s"end: ${jobProperties.jobEndTime}", ex)
            return true
          }
        }
      }
    }
    return true
  }

  def saveViewClientJobId(clientJobId: Option[ViewClientJobId]): Unit = {
    viewClientJobIdService.saveViewClientJobId(clientJobId.get)
  }

  def scheduleCron(jobProperties: JobScheduleProperties): Option[String] = {
    jobRunner.scheduleCron(jobProperties)
  }

  def scheduleOnce(jobProperties: JobScheduleProperties): Option[String] = {
    jobRunner.scheduleOnce(jobProperties)
  }

  def getRunningJobId(viewClient: String): Option[String] = {
    jobRunner.getRunningJobId(viewClient)
  }

  /**
    *
    * @param jobProperties
    * @return true indicates that no longer a job is running with this config,
    *         false indicates there exists a job with this config that still runs
    */
  def killCronJob(jobProperties: JobScheduleProperties): Boolean = {
    var jobId: Option[String] = null
    try {
      jobId = viewClientJobIdService.getJobId(jobProperties.viewName, jobProperties.clientIdentifier, jobProperties.jobCronExpression)
    } catch {
      case ex: HibernateException => {
        logger.error(s"Orchestrator manager! Job could not be killed as db is " +
          s"unavailable", ex)
        return false
      }

    }

    //DB does not have an entry for this job config.
    if (!jobId.isDefined) {
      return true
    }

    try {
      if (jobRunner.isJobAlive(jobId.get)) {
        jobRunner.killJob(jobId.get)
      }
    } catch {
      //As per the doc, oozie could not kill the Job.
      case ex: ScheduleException => {
        logger.error(s"OrchestratorManager! Job could not be killed for ${jobProperties.viewName}, " +
          s"${jobProperties.clientIdentifier}", ex);
        return false
      }
    }

    try {
      viewClientJobIdService.deleteByJobId(jobId.get)
    } catch {
      case ex: Exception => {
        logger.error(s"OrchestratorManager! Job is killed for ${jobProperties.viewName}, " +
          s"${jobProperties.clientIdentifier} but db could not modified.", ex)
        return false
      }
    }
    true
  }

  def killCronJobById(vwJobId: ViewClientJobId): Boolean = {
    logger.info(s"Kill schedule for job :$vwJobId")
    try {
      if (jobRunner.isJobAlive(vwJobId.jobId)) {
        jobRunner.killJob(vwJobId.jobId)
      }
    } catch {
      //As per the doc, oozie could not kill the Job.
      case ex: ScheduleException => {
        logger.error(s"OrchestratorManager! Job could not be killed for ${vwJobId.viewName}, " +
          s"${vwJobId.clientId}", ex);
        false
      }
    }

    try {
      viewClientJobIdService.deleteByJobId(vwJobId.jobId)
    } catch {
      case ex: Exception => {
        logger.error(s"OrchestratorManager! Job is killed for ${vwJobId.viewName}, " +
          s"${vwJobId.clientId} but db could not modified.", ex)
        return false
      }
    }
    true
  }

  /**
    * Returns the job id for the replay workflow created.
    *
    * @param jobProperties
    * @return
    */
  def createReplay(jobProperties: JobScheduleProperties): Option[String] = {
    //Trigger the cron job
    var cronJobId: Option[String] = None
    try {
      cronJobId = scheduleCron(jobProperties)
      logger.info(s"Scheduled a workflow for ${jobProperties.viewName} for client ${jobProperties.clientIdentifier}" +
        s" from start: ${jobProperties.jobStartTime} to end: ${jobProperties.jobEndTime}")
      return cronJobId
    } catch {
      case e: Exception => {
        logger.error(s"Failed to schedule the replay workflow for ${jobProperties.viewName} for client " +
          s"${jobProperties.clientIdentifier} from start: ${jobProperties.jobStartTime} to " +
          s"end: ${jobProperties.jobEndTime}", e)
        return None
      }
    }
  }


  def change(viewClientJobId: ViewClientJobId, endDate: Date): Boolean = {
    logger.info(s"change end time to :$endDate for view client job :$viewClientJobId")
    val oozieEndDate = DateUtils.dateToUTCString(Constants.OOZIE_DATE_FORMAT, endDate)
    val changeValue = s"endtime=$oozieEndDate"
    viewClientJobId.jobEndEpochTime = endDate.getTime
    try {
      if (jobRunner.isJobAlive(viewClientJobId.jobId) && jobRunner.change(viewClientJobId.jobId, changeValue)) {
        try {
          viewClientJobIdService.update(viewClientJobId)
        }
        catch {
          case ex: Exception => {
            logger.error(s"OrchestratorManager! Job updated for view :${viewClientJobId.viewName}, " +
              s"client :${viewClientJobId.clientId} but db could not modified.", ex)
            return false
          }
        }
      }
      true
    }
    catch {
      case ex: ScheduleException => {
        logger.error(s"Failed to change the job end time :$endDate for view :${viewClientJobId.viewName}, " +
          s"client :${viewClientJobId.clientId}", ex);
        return false
      }
    }
  }

  /**
    * Constructs the ViewClientJobId object with given details
    *
    * @param jobProperties
    * @param cronJobId
    * @return
    */
  protected def createViewClientJobId(jobProperties: JobScheduleProperties, cronJobId: String): ViewClientJobId = {
    val clientJobId = new ViewClientJobId
    clientJobId.clientId = jobProperties.clientIdentifier
    clientJobId.viewName = jobProperties.viewName
    clientJobId.jobId = cronJobId
    clientJobId.jobStartEpochTime = jobProperties.jobStartTime.getTime
    clientJobId.jobEndEpochTime = jobProperties.jobEndTime.getTime
    clientJobId.cronExpression = jobProperties.jobCronExpression
    return clientJobId
  }

}
