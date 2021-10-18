package com.tfs.orchestrator.properties

import java.util.Date

import com.tfs.orchestrator.exceptions.InvalidParameterException
import com.tfs.orchestrator.utils.{Constants, RESOURCE_DEGREE, WorkflowType}

/**
 * This class represents the properties associated with a job/workflow. Relevant to a single job/workflow.
 * Properties are mandatory.
 *
 * @param clientIdentifier client name.
 * @param viewName name of the view/application.
 * @param userName owner of the view/application as defined in the catalog.
 */
class JobScheduleProperties private(val clientIdentifier:String, val viewName: String, val userName: String) {

  //Cron expression of job.
  //Default: runs every 15 mins
  var jobCronExpression = "0 0 0/15/30/45 * *"

  //Start time of the job.
  //Default: time at which Orchestrator discovered the job.
  var jobStartTime: Date = new Date()

  //Time beyond which you do not want to have schedules of this job
  //Default: 1 day
  var jobEndTime: Date = new Date(jobStartTime.getTime + Constants.DEFAULT_JOB_END_DURATION)

  //Complexity of the job.
  //Default: LOW
  var jobComplexity = RESOURCE_DEGREE.LOW

  //Is the Job a replay.
  var replay = false
  
  var wfType = WorkflowType.DEFAULT

  var ingestionType = "ALL"

  var ucInstanceDate: Option[String] = None

  var externalId = ""

}

object JobScheduleProperties {
  def apply(clientIdentifier: String, viewName: String, userName: String): JobScheduleProperties = {
    if (clientIdentifier == null || viewName == null || userName == null) {
      throw new InvalidParameterException("Null value for a mandatory parameter: " +
        "clientIdentifier: " + clientIdentifier +
        ", viewName: " + viewName +
        ", userName: " + userName )
    }
    new JobScheduleProperties(clientIdentifier, viewName, userName)
  }

}
