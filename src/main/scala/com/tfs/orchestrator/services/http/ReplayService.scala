package com.tfs.orchestrator.services.http

import java.util.Date

import com.tfs.orchestrator.hibernate.entities.View
import com.tfs.orchestrator.hibernate.service.ViewService
import com.tfs.orchestrator.managers.OrchestratorManager
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.utils.{Constants, PropertyReader, RESOURCE_DEGREE}
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.util.Headers
import org.apache.logging.log4j.scala.Logging
import com.tfs.orchestrator.utils.WorkflowType

/**
 * Service that takes replay requests.
 */
class ReplayService extends HttpHandler with Logging {

  private val manager = new OrchestratorManager
  private val viewService = new ViewService()

  /**
   *
   * @param httpServerExchange
   */
  override def handleRequest(httpServerExchange: HttpServerExchange): Unit = {
    //MANDATORY parameters
    val (client, view, startTimeStr, endTimeStr) = fetchMandatoryParams(httpServerExchange) match {
      case Some(params) => params
      case None => return
    }

    //OPTIONAL parameters
    val (scheduleTime, queue) = fetchOptionalParams(httpServerExchange)

    val (dataStartDate, dataEndDate) = validateStartAndEndDate(startTimeStr, endTimeStr, httpServerExchange) match {
      case Some(value) => value
      case None => return
    }

    val existingViewSchedule: Option[View] = try {
      Option(getViewService().getViewByNameAndClientId(view, client))
    } catch {
      case ex: Exception => { logger.error("Error while fetching view schedule from database", ex); None}
    }

    if (!existingViewSchedule.isDefined) {
      sendResponse(httpServerExchange, Constants.HTTP_400, """{"status": "fail", "msg": "Invalid View."}""")
      return
    }

    logger.info(s"Replay request fired for: client: $client, view: $view, startTime: $dataStartDate, " +
      s"endTime: $dataEndDate, scheduleTime: $scheduleTime, queue: $queue")

    logger.debug(s"Existing view schedule details are : ${existingViewSchedule.get}")

    val jobScheduleProperties = getJobScheduleProperties(existingViewSchedule.get, client, dataStartDate, dataEndDate)

    getOrchestratorManager().createReplay(jobScheduleProperties) match {
      case Some(jobId) =>
        sendResponse(httpServerExchange, Constants.HTTP_200, s"""{"status": "success", "job": "${jobId}", "view":""" +
          s""" "$view", "client": "$client", "dataStartDate": "$dataStartDate", "dataEndDate": "$dataEndDate"}""")
      case None => sendResponse(httpServerExchange, Constants.HTTP_500,
        """{"status": "fail", "msg": "Replay cannot be scheduled."}""")
    }

    return
  }


  protected def getOrchestratorManager() = manager
  protected def getViewService() = viewService

  /**
   * Fetch the mandatory parameters from request.
   * @param httpServerExchange
   * @return
   */
  protected def fetchMandatoryParams(httpServerExchange: HttpServerExchange):
                                                        Option[(String, String, String, String)] = {
    val client = validateAndReturnValue("client", httpServerExchange, false) match {
      case Some(clientValue) => clientValue
      case None => return None
    }

    val view = validateAndReturnValue("view", httpServerExchange, false) match {
      case Some(viewName) => viewName
      case None => return None
    }

    val startTimeStr = validateAndReturnValue("datastarttime", httpServerExchange, false) match {
      case Some(startTimeValue) => startTimeValue
      case None => return None
    }

    val endTimeStr = validateAndReturnValue("dataendtime", httpServerExchange, false) match {
      case Some(endTimeValue) => endTimeValue
      case None => return None
    }

    Some(client, view, startTimeStr, endTimeStr)
  }

  /**
   * Fetch the optional parameters from request.
   * @param httpServerExchange
   * @return
   */
  protected def fetchOptionalParams(httpServerExchange: HttpServerExchange): (Date, String) = {
    val scheduleTime = validateAndReturnValue("scheduletime", httpServerExchange, true) match {
      case Some(value) => try { new Date(value.toLong) } catch { case ex: Exception => new Date()}
      case None => new Date()
    }

    val queue = validateAndReturnValue("queue", httpServerExchange, true) match {
      case Some(value) => value
      case None => PropertyReader.getOozieProperties().getProperty(Constants.PROP_OOZIE_DEFAULT_QUEUE)
    }

    return (scheduleTime, queue)
  }

  /**
   * Construct the JobScheduleProperties, meant to be used for scheduling the job.
   * @param viewSchedule
   * @param client
   * @param startDate
   * @param endDate
   * @return
   */
  def getJobScheduleProperties(viewSchedule: View, client: String, startDate:Date,
                               endDate: Date): JobScheduleProperties = {
    return getJobScheduleProperties(viewSchedule, client, startDate, endDate, WorkflowType.DEFAULT)
  }
  
  
   def getJobScheduleProperties(viewSchedule: View, client: String, startDate:Date,
                               endDate: Date, wfType: WorkflowType.Value): JobScheduleProperties = {
    val jobScheduleProps = JobScheduleProperties(client, viewSchedule.viewName, viewSchedule.userName)
    jobScheduleProps.jobComplexity = RESOURCE_DEGREE.MEDIUM
    jobScheduleProps.jobCronExpression = viewSchedule.cronExpression
    jobScheduleProps.jobStartTime = startDate
    jobScheduleProps.jobEndTime = endDate
    jobScheduleProps.replay = true
    jobScheduleProps.wfType = wfType
    return jobScheduleProps
  }


  /**
   *
   * @param key
   * @param httpServerExchange
   * @return
   */
  def validateAndReturnValue(key: String, httpServerExchange: HttpServerExchange, optional: Boolean): Option[String] = {
    val params = httpServerExchange.getQueryParameters().get(key)

    if (params == null || params.isEmpty) {
      if (!optional) {
        sendResponse(httpServerExchange, Constants.HTTP_400,
          s"""{"status": "fail", "msg": "$key value is null/empty."}""")
      }
      return None
    }

    if (params.size() > 1) {
      sendResponse(httpServerExchange, Constants.HTTP_400, s"""{"status": "fail", "msg": "$key value is duplicate."}""")
      return None
    }

    return Some(params.element())
  }

  /**
   *
   * @param startDateStr
   * @param endDateStr
   * @param httpServerExchange
   * @return
   */
  def validateStartAndEndDate(startDateStr: String, endDateStr: String,
                              httpServerExchange: HttpServerExchange): Option[(Date, Date)] = {
    try {
      val startDate = new Date(startDateStr.toLong)
      val endDate = new Date(endDateStr.toLong)
      val now = new Date()
      if (!now.after(startDate) || !now.after(endDate) || !endDate.after(startDate)) {
        sendResponse(httpServerExchange, Constants.HTTP_400,
          """{"status": "fail", "msg": "Either start/end date is in future or start is not before end date"}""")
        return None
      }
      return Some((startDate, endDate))
    } catch {
      case ex: Exception => {
        sendResponse(httpServerExchange, Constants.HTTP_400,
          """{"status": "fail", "msg": "Invalid data start/end time provided."}""")
        return None
      }
    }
  }

  /**
   *
   * @param httpServerExchange
   * @param httpCode
   * @param msg
   */
  def sendResponse(httpServerExchange: HttpServerExchange, httpCode: Int, msg: String): Unit = {
    httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, Constants.HTTP_APPLICATION_JSON)
    httpServerExchange.setStatusCode(httpCode)
    httpServerExchange.getResponseSender.send(msg)
  }

}
