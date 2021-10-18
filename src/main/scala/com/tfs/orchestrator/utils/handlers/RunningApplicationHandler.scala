package com.tfs.orchestrator.utils.handlers

import java.io._

import com.tfs.orchestrator.cache.{CacheEntry, ExecutionCache}
import com.tfs.orchestrator.utils.handlers.elasticsearch.{ESRequestHandler, ESResponseHandler}
import com.tfs.orchestrator.utils.{Constants, PropertyReader, Utils}
import org.apache.http.HttpStatus
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.ListBuffer
import scalaj.http.HttpResponse

class RunningApplicationHandler extends Logging {


  private lazy val ES_URL = PropertyReader.getApplicationPropertiesValue("sla.es.ingest.url", Constants.DEFAULT_ES_URL)
  private lazy val DEFAULT_SLA_TIME: Int = PropertyReader.getApplicationPropertiesValue("sla.default.timeout", Constants.DEFAULT_SLA_TIMEOUT)
  private lazy val SLA_MULTIPLIER: Int = PropertyReader.getApplicationPropertiesValue("sla.multiplier", Constants.DEFAULT_SLA_MULTIPLIER)
  private lazy val RUNNING_FILTER = fetchRunningFilter()
  private lazy val esRequestHandler = new ESRequestHandler
  private lazy val esResponseHandler = new ESResponseHandler

  logClassProperties()

  /**
    * Fetch all running applications matched by the timeout tagged with each view-client job.
    * @return list of running applications
    */

  def fetchRunningApplications(): List[String] = {
    logger.debug("Fetching running applications.")
    val appLists = ListBuffer[String]()
    val entries = ExecutionCache.getSlaEntries()
    logger.debug(s"SLA entries count:${entries.size}")
    entries.foreach(entry => {
      fetchAndFilterRunningApplications(appLists, entry)
    })
    appLists.toList
  }

  protected def fetchAndFilterRunningApplications(appLists: ListBuffer[String], entry: CacheEntry): Unit = {
    val response: HttpResponse[String] = fetchRunningApplicationsFromES(entry.viewName, entry.clientName, s"${deriveSlaTime(entry)}m")
    addToRunningApplicationList(appLists, entry, response)
  }

  protected def addToRunningApplicationList(appLists: ListBuffer[String], entry: CacheEntry, response: HttpResponse[String]): Unit = {
    val code = response.code
    if (code.equals(HttpStatus.SC_OK)) {
      appLists ++= esResponseHandler.retrieveJobIds(response.body)
    }
    else {
      logger.error(s"Unable to fetch application status for view:${entry.viewName} and client:${entry.clientName}.Error code:$code, Message:${response.body}")
    }
  }

  protected def deriveSlaTime(entry: CacheEntry): Int = {
    val value = entry.value.toInt
    val slaTime = if (value == -1) DEFAULT_SLA_TIME else value
    slaTime * SLA_MULTIPLIER
  }



  protected def fetchRunningFilter(): String = {
    val file = new File(System.getProperty(Constants.COMMAND_PROPS_DIR_KEY) +
      System.getProperty("file.separator") + Constants.RUNNING_FILTER_FILE)
    Utils.readInputReader(new FileReader(file))
  }

  private def fetchRunningApplicationsFromES(viewName: String, clientName: String, slaTime: String):HttpResponse[String] = {
    val filter = RUNNING_FILTER.replace("$VIEW_NAME", viewName).replace("$CLIENT_NAME", clientName).replace("$SLA_TIME", slaTime)
    esRequestHandler.fetchJobDetailsFromES(Constants.JOB_INDEX,filter)
  }

  private def logClassProperties(): Unit = {
    logger.info(s"Properties=>ES_URL:$ES_URL, JOB_INDEX:${Constants.JOB_INDEX}, DEFAULT_SLA_TIME:$DEFAULT_SLA_TIME")
  }

}