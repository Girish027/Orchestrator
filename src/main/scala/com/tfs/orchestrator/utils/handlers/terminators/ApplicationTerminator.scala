package com.tfs.orchestrator.utils.handlers.terminators

import com.tfs.orchestrator.utils.{Constants, PropertyReader}
import com.tfs.orchestrator.utils.handlers.terminators.impl.Response
import net.liftweb.json._
import org.apache.logging.log4j.scala.Logging
import scalaj.http.{Http, HttpOptions, HttpResponse}
import org.apache.http.HttpStatus

import scala.collection.mutable.ListBuffer

abstract class ApplicationTerminator[A <: Response : Manifest] extends Logging {

  protected val YARN_URL: String = PropertyReader.getApplicationPropertiesValue("yarn.url", Constants.DEFAULT_YARN_URL)
  protected val OOZIE_URL: String = PropertyReader.getApplicationPropertiesValue("oozie.url", Constants.DEFAULT_OOZIE_URL)

  /*
    URL to be populated by extending classes.
   */
  protected val URL = ""

  var successor = None: Option[ApplicationTerminator[_ <: Response]]


  init()


  private def init() {
    logger.info(s"${this.getClass.getSimpleName} is being initialized.")
  }

  /**
    * Submit termination application for the list of workflows.
    * @param workflowIds
    */
  def terminate(workflowIds: ListBuffer[String]): Unit = {
    logger.info(s"List of applications to terminate:$workflowIds")
    try {
      killApplications(workflowIds, fetchYarnApplicationIdMappedToWorkflowIds(workflowIds))
    }
    catch {
      case exception: Exception => {
        logger.error(s"Unable to terminate applications for workflows:${workflowIds}, exception:${exception}")
      }
    }
    finally {
      if (successor.nonEmpty) {
        successor.get.terminate(workflowIds)
      }
    }

  }

  protected def killApplications(workflowIds: ListBuffer[String], wfAppIds: Map[String, String]) = {
    wfAppIds.foreach { case (workFlowId, applicationId) => {
      if (killApplication(applicationId)) {
        workflowIds.-=(workFlowId)
        logger.info(s"Application id:${applicationId} for workflow:${workFlowId} has been submitted to kill.")
      }
      else {
        logger.error(s"Unable to kill application:${applicationId} for workflow:${workFlowId}.")
      }
    }
    }
  }

  protected def killApplication(applicationId: String): Boolean = {
    val response = submitKillToYarn(applicationId)
    val code = response.code
    if (code.equals(HttpStatus.SC_ACCEPTED) || code.equals(HttpStatus.SC_OK)) {
      return true
    }
    logger.error(s"Unable to kill application.Error code:${code}, Message:${response.body}")
    false
  }


  protected def submitKillToYarn(applicationId: String): HttpResponse[String] = {
    Http(s"$YARN_URL/${applicationId}/state").put("{\"state\": \"KILLED\"}")
      .header("Content-Type", Constants.CONTENT_TYPE)
      .header("Charset", Constants.ENCODING)
      .option(HttpOptions.readTimeout(Constants.HTTP_TIMEOUT)).asString
  }

  /**
    * Returns a mapping of workflowIds to yarnApplicationIds.
    * @param workflowIds
    * @return a map containing workflowId -> yarnApplicationId
    */
  protected def fetchYarnApplicationIdMappedToWorkflowIds(workflowIds: ListBuffer[String]): Map[String, String] = {
    var wfAppIdMap: Map[String, String] = Map()
    workflowIds.foreach(workflowId => {
      val yarnAppId = fetchYarnApplicationId(workflowId, URL)
      if (yarnAppId.nonEmpty) {
        wfAppIdMap += (workflowId -> yarnAppId.get)
      }
    })
    wfAppIdMap
  }

  /**
    *
    * @param workflowId
    * @param url
    * @return yarn application ids
    */
  protected def fetchYarnApplicationId(workflowId: String, url: String): Option[String] = {
    val wfUrl = url.replace("$id", workflowId)
    val result: HttpResponse[String] = triggerHttpRequest(wfUrl)
    if (result.code.equals(HttpStatus.SC_OK)) {
      val response = result.body
      val json = parse(response)
      implicit val formats: DefaultFormats.type = DefaultFormats
      val responseObject = json.extract[A]
      return filterRunningApplications(responseObject)
    }
    logger.error(s"Unable to fetch yarn application details for workflow id:${workflowId}. Error code:${result.code}, message:${result.body}")
    Option.empty
  }

  private def triggerHttpRequest(wfUrl: String) = {
    Http(wfUrl)
      .header("Content-Type", Constants.CONTENT_TYPE)
      .header("Charset", Constants.ENCODING)
      .option(HttpOptions.readTimeout(Constants.HTTP_TIMEOUT)).asString
  }

  protected def filterRunningApplications(a: A): Option[String]

}