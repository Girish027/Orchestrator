package com.tfs.orchestrator.jobs.runner.oozie

import java.util.{Date, Properties}

import com.tfs.orchestrator.exceptions.{InitializationException, InvalidJsonException, RestClientException, ScheduleException}
import com.tfs.orchestrator.jobs.dag.oozie.OozieDagCreater
import com.tfs.orchestrator.jobs.runner.JobRunner
import com.tfs.orchestrator.properties.{JobScheduleProperties, SystemProperties}
import com.tfs.orchestrator.utils._
import net.liftweb.json._
import org.apache.logging.log4j.scala.Logging
import org.apache.oozie.client.{AuthOozieClient, OozieClient, OozieClientException, WorkflowJob}

import scala.util.control.NonFatal
import com.tfs.orchestrator.jobs.dag.oozie.DAGLocations

import scala.util.Try

/**
 * Schedules/Runs oozie jobs
 *
 * All the properties required to run oozie workflow will be populated here.
 */
class OozieJobRunner(val systemProperties: SystemProperties) extends JobRunner with Logging {

  protected var client: OozieClient = null

  private val oozieProperties = PropertyReader.getOozieProperties()

  val defaultQueueName = oozieProperties.getProperty(Constants.PROP_OOZIE_DEFAULT_QUEUE)
  val useSystemPath = oozieProperties.getProperty(Constants.KEY_OOZIE_USE_LIBPATH, "true")
  val oozieClientUrl = oozieProperties.getProperty(Constants.PROP_OOZIE_CLIENT_URL)
  val oozieLibpath = oozieProperties.getProperty(Constants.KEY_OOZIE_LIBPATH)

  initialize(systemProperties)

  /**
   * Initializes the Oozie runner code and the necessary Dags.
   */
  def initialize(systemProperties: SystemProperties) : Unit = {
    OozieDagCreater.initialize(systemProperties)
    client = new AuthOozieClient(oozieClientUrl, "simple")
    try {
      client.getServerBuildVersion
    } catch {
      case NonFatal(exc) => {
        logger.error(s"Failed to retrieve Oozie server host. Please check the host ${oozieClientUrl}")
        throw new InitializationException(exc, "")
      }
    }
  }

  /**
   * @see com.tfs.orchestrator.jobs.runner.JobRunner#scheduleCron
   * (Properties, Properties)
   */
  def scheduleCron(jobProperties: JobScheduleProperties): Option[String] = {
    val properties = fillInSystemProperties()
    properties.putAll(fillInCoordinatorProperties(jobProperties.wfType))
    properties.putAll(fillInJobProperties(jobProperties))
    var jobId: String = null
    try {
      logger.debug(s"Cron job is getting scheduled for $properties")
      jobId = client.run(properties)
    } catch {
      case ex: OozieClientException => throw new ScheduleException(ex, "Cron Job schedule failed.")
    }

    Option(jobId)
  }

  /**
   * @see com.tfs.orchestrator.jobs.runner.JobRunner#scheduleOnce
   * (Properties, Properties)
   */
  def scheduleOnce(jobProperties: JobScheduleProperties): Option[String] = {
    val properties = fillInSystemProperties()
    properties.putAll(fillInWorkflowProperties(jobProperties.wfType))
    properties.putAll(fillInJobProperties(jobProperties))

    var jobId: String = null
    try {
      jobId = client.run(properties)
    } catch {
      case ex: OozieClientException => throw new ScheduleException(ex, "Workflow Job schedule failed.")
    }

    Option(jobId)
  }

  /**
   * Kill a given job
   * @param jobId the jobId that needs to be killed.
   * @return success of the kill.
   */
  def killJob(jobId: String): Boolean = {
    var jobInfo: WorkflowJob = null
    try {
      client.kill(jobId)
      return true
    } catch {
      case ex: OozieClientException => throw new ScheduleException(ex, "Could not communicate with Oozie")
    }
  }

  /**
   * Running status of the given jobId or may be it is in suspended/paused/prepared states
   * @param jobId the jobId
   * @return whether the job is running or not.
   */
  def isJobAlive(jobId: String): Boolean = {
    var jobInfo: WorkflowJob = null
    try {
      jobInfo = client.getJobInfo(jobId)
      return !jobInfo.getStatus.equals(WorkflowJob.Status.KILLED)
    } catch {
      case ex: OozieClientException => {
        //If oozie is not able to find the given job Id, it implies job is not alive.
        if (!ex.getMessage.contains("E0604")) {
          return false;
        }
        throw new ScheduleException(ex, "Could not communicate with Oozie")
      }
      case ex: IllegalArgumentException => {
        if (ex.getMessage.contains("DONEWITHERROR")) {
          //TODO: For some odd reason, we are getting below exception even with same oozie server and client version
          //IllegalArgumentException: No enum constant org.apache.oozie.client.WorkflowJob.Status.DONEWITHERROR
        }
      }
    }
    return false;
  }

  /**
   * Update the job end time.
   * @param jobId the jobId
   * @param endTime end time
   * @return whether the job is running or not.
   */
  def updateJobEndTime(jobId: String, endTime: Date): Boolean = {
    logger.info(s"Job change id is ${jobId} and time " +
      s"is ${DateUtils.dateToUTCString(Constants.OOZIE_DATE_FORMAT, endTime)}")
    try {
      client.change(jobId, "endtime=" + DateUtils.dateToUTCString(Constants.OOZIE_DATE_FORMAT, endTime))
      return true
    } catch {
      case ex: OozieClientException => throw new ScheduleException(ex, "Could not communicate with Oozie")
    }
  }


  override def change(jobId: String, changeValue: String): Boolean = {
    logger.info(s"Job change id is ${jobId} and changeValue is ${changeValue} ")
    try {
      client.change(jobId, changeValue)
      return true
    } catch {
      case ex: OozieClientException => throw new ScheduleException(ex, "Could not communicate with Oozie")
    }
  }

    override def getRunningJobId(view_client : String): Option[String] ={
      var jobId : Option[String] = None
      var viewClientJob : ViewClientJob = null
      var cordinatorJobResponse : String = null
      val RunningCordinatorJobUrl: String = getRunningCordinatorJobUrl(view_client)
      try {
        cordinatorJobResponse = getRestResponse(RunningCordinatorJobUrl)
        if(cordinatorJobResponse != null){
          viewClientJob = createObjectFromJsonResponse(cordinatorJobResponse)
          var coordinatorJobs : CoordinatorJobs = null
          if(viewClientJob.coordinatorjobs.size == 1){
            coordinatorJobs = viewClientJob.coordinatorjobs.head
            jobId = Some(coordinatorJobs.coordJobId)
          }else if(viewClientJob.coordinatorjobs.size > 1){
            var endTime : Long = 0
            viewClientJob.coordinatorjobs.foreach(f = coordinatorJob => {
              val jobEndtime = DateUtils.utcStringToEpoch(Constants.OOZIE_RESPONSE_TIME_FORMAT, coordinatorJob.endTime)
              if (jobEndtime > endTime) {
                endTime = jobEndtime
                jobId = Some(coordinatorJob.coordJobId)
              }
            })
          }
        }
      } catch {
        case ex: RestClientException => {
          logger.error("Not able to retrieve response from Oozie server.", ex)
        }
      }
      jobId
    }

  def getRunningCordinatorJobUrl(view_client: String): String = {
    val RunningCordinatorJobUrlTemplate = PropertyReader.getOozieProperties().getProperty("oozie.job.description.url")
    val RunningCordinatorJobUrl = RunningCordinatorJobUrlTemplate.replace("VIEW_CLIENT", view_client)
    RunningCordinatorJobUrl
  }

  def getRestResponse(RunningCordinatorJobUrl: String): String = {
    RestClient.sendGetRequest(RunningCordinatorJobUrl)
  }

  /**
     * Parse the response from oozie for the running job and create ViewClientJob
     *
     * @param jsonStr
     * @return
     */
    def createObjectFromJsonResponse(jsonStr: String): ViewClientJob = {
      var viewClientJob: ViewClientJob = null
      logger.debug("Parsing snapshot to update orchestrator jobs")
      try {
        implicit val formats = net.liftweb.json.DefaultFormats
        logger.info("json String "+jsonStr)
        viewClientJob = parse(jsonStr).extract[ViewClientJob]
        return viewClientJob
      } catch {
        case e: Exception => {
          logger.error("There was error while parsing json", e)
          throw new InvalidJsonException(e.getMessage)
        }
      }
    }


  override def rerun(jobId: String, conf: Properties): Try[Unit] = {
    Try(client.reRun(jobId, conf))
  }

  /**
   * All the properties related to system and across all kinds of jobs, clients.
   * @return
   */
  private def fillInSystemProperties(): Properties = {
    val props = new Properties()
    props.setProperty(Constants.KEY_JOB_TRACKER, systemProperties.jobTracker)
    props.setProperty(Constants.KEY_NAME_NODE, systemProperties.nameNode)
    props.setProperty(Constants.KEY_INPUT_AVAIL_WAIT, PropertyReader.getSystemProperties().
      getProperty(Constants.PROP_INPUT_WAIT, "false"))
    props.setProperty(Constants.KEY_OUTPUT_CHECK, PropertyReader.getSystemProperties().
      getProperty(Constants.PROP_OUTPUT_CHECK, "false"))
    props.setProperty(Constants.KEY_OOZIE_LIBPATH, oozieLibpath)
    props.setProperty(Constants.KEY_OOZIE_USE_LIBPATH, oozieProperties.getProperty(Constants.KEY_OOZIE_USE_LIBPATH))
    props.setProperty(Constants.KEY_DEFAULT_PUBLISH, oozieProperties.getProperty("oozie.default.publish"))
    props.setProperty(Constants.KEY_HDFS_PROPS_DIR, PropertyReader.getSystemProperties().
      getProperty("hadoop.properties.path"))
    props.setProperty(Constants.KEY_HDFS_CONFIG_DIR, PropertyReader.getSystemProperties().
      getProperty("hadoop.config.location"))

    props
  }

  /**
   * All the properties pertinent to the Job. Usually the task related properties stay here.
   * @param jobProps
   * @return
   */
  private def fillInJobProperties(jobProps: JobScheduleProperties) = {
    val props = new Properties()

    //TODO: queue name may be deduced based on the job user.
    props.setProperty(Constants.KEY_QUEUE_NAME, defaultQueueName)

    props.setProperty(Constants.KEY_CLIENT_NAME, jobProps.clientIdentifier)
    props.setProperty(Constants.KEY_JOB_START_TIME, DateUtils.dateToUTCString(Constants.OOZIE_DATE_FORMAT,
      jobProps.jobStartTime))
    props.setProperty(Constants.KEY_JOB_END_TIME, DateUtils.dateToUTCString(Constants.OOZIE_DATE_FORMAT,
      jobProps.jobEndTime))
    props.setProperty(Constants.KEY_JOB_CRON_EXPR, jobProps.jobCronExpression)
    props.setProperty(Constants.KEY_VIEW_NAME, jobProps.viewName)
    props.setProperty(Constants.KEY_USER_NAME, jobProps.userName)
    props.setProperty(Constants.KEY_JOB_COMPLEXITY, jobProps.jobComplexity.toString)
    props.setProperty(Constants.KEY_OOZIE_USER_NAME, jobProps.userName)
    props.setProperty(Constants.KEY_REPLAY_TASK, jobProps.replay.toString)
    props.setProperty(Constants.KEY_SLEEP_TIME, "120")
    props.setProperty(Constants.INGESTION_TYPE, jobProps.ingestionType.toString)
    props.setProperty(Constants.EXTERNAL_ID, jobProps.externalId)
    val ucInstanceDate = jobProps.ucInstanceDate
    if(ucInstanceDate.isDefined){
      props.setProperty("ucInstanceDate",ucInstanceDate.get)
    }

    props
  }

  private def fillInCoordinatorProperties(wfType: WorkflowType.Value) = {
    val props = new Properties
    //TODO: If we go for dynamic DAGs. We will make a call to Oozie DAG create a DAG for us.
    val dagLocations= getDagLocations(wfType)
    props.setProperty(Constants.KEY_OOZIE_COORD_PATH, dagLocations.coordLocation)
    props.setProperty(Constants.KEY_OOZIE_WF_FOR_COORD, dagLocations.workflowLocation)
    props
  }

  private def getDagLocations(wfType: WorkflowType.Value): DAGLocations = {
    wfType match {
      case WorkflowType.DEFAULT => OozieDagCreater.getDefaultDagLocations()
      case WorkflowType.INGESTION => OozieDagCreater.getIngestDagLocations()
    }
  }

  private def fillInWorkflowProperties(wfType: WorkflowType.Value) = {
    val props = new Properties
    val dagLocations = getDagLocations(wfType)
    props.setProperty(Constants.KEY_OOZIE_WORKFLOW_PATH, dagLocations.workflowLocation)
    props
  }

}

case class ViewClientJob(val total: Int, val offset: Int, val len: Int, val coordinatorjobs: List[CoordinatorJobs])

case class CoordinatorJobs(val frequency : String, val startTime : String, val coordJobId : String, val endTime : String)