package com.tfs.orchestrator.utils.retry.filters.impl

import java.io.{File, FileReader}

import com.tfs.orchestrator.utils.handlers.elasticsearch.{ESRequestHandler, ESResponseHandler}
import com.tfs.orchestrator.utils.retry.filters.RetryableJobs
import com.tfs.orchestrator.utils.{Constants, PropertyReader, Utils}
import org.apache.http.HttpStatus
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.ListBuffer
import scalaj.http.HttpResponse

class ESSourceRetryableJobs extends RetryableJobs with Logging {

  private lazy val ES_URL = PropertyReader.getApplicationPropertiesValue("sla.es.ingest.url", Constants.DEFAULT_ES_URL)
  private lazy val FAILED_FILTER = fetchFailedJobsFilter()
  private lazy val esRequestHandler = new ESRequestHandler
  private lazy val esResponseHandler = new ESResponseHandler

  override def identify(wfIds: ListBuffer[String]): Unit = {
    addToFailedJobLists(wfIds, fetchFailedApplicationsFromES(PropertyReader.getApplicationPropertiesValue(Constants.RETRY_WINDOW, Constants.DEFAULT_RETRY_WINDOW)))
  }


  protected def fetchFailedJobsFilter(): String = {
    val file = new File(System.getProperty(Constants.COMMAND_PROPS_DIR_KEY) +
      System.getProperty("file.separator") + Constants.RETRYABLE_FILTER_FILE)
    Utils.readInputReader(new FileReader(file))
  }


  private def fetchFailedApplicationsFromES(retryWindow: Int): HttpResponse[String] = {
    val filter = FAILED_FILTER.replace("$RETRYABLE_WINDOW",retryWindow+"m")
    esRequestHandler.fetchJobDetailsFromES(Constants.JOB_INDEX, s"size=${PropertyReader.getApplicationPropertiesValue(Constants.RETRY_FILTER_FETCHSIZE_KEY, Constants.DEFAULT_RETRY_FILTER_FETCHSIZE)}", filter)
  }

  protected def addToFailedJobLists(appLists: ListBuffer[String], response: HttpResponse[String]): Unit = {
    val code = response.code
    if (code.equals(HttpStatus.SC_OK)) {
      appLists ++= esResponseHandler.retrieveJobIds(response.body)
    }
    else {
      logger.error(s"Unable to fetch failed job list ")
    }
  }

}
