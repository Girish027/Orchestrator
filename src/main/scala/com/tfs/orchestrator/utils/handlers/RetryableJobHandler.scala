package com.tfs.orchestrator.utils.handlers

import java.util.Properties

import com.tfs.orchestrator.jobs.runner.JobRunner
import com.tfs.orchestrator.utils.retry.filters.RetryableJobs
import com.tfs.orchestrator.utils.retry.filters.impl.{ESSourceRetryableJobs, OozieFilteredRetryableJobs}
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success}

object RetryableJobHandler extends Logging {

  private lazy val retryableJobs = initializedRetryableJobRetriever

  private val RERUN_FAILNODES_KEY = "oozie.wf.rerun.failnodes"
  private val RERUN_FAILNODES = "false"

  def retryFailedRetryableJobs(): Unit = {
    try {
      val wfIds: List[String] = fetchAllRetryableJobs
      logger.info(s"Retryable job list:$wfIds")
      retryJobs(wfIds)
    }
    catch {
      case ex: Exception => logger.error("Unable to retry failed jobs.", ex)
    }
  }

  private def fetchAllRetryableJobs: List[String] = {
    retryableJobs.fetchAllRetryableJobs
  }

  def retryJobs(wfIds: List[String]): Unit = {
    val prop = new Properties()
    prop.setProperty(RERUN_FAILNODES_KEY,RERUN_FAILNODES)

    wfIds.foreach(wfId => {
        JobRunner.getJobRunner().rerun(wfId, prop) match {
          case Success(value) => logger.info(s"Triggered rerun for $wfId")
          case Failure(ex) => logger.error(s"Unable to rerun job:$wfId",ex)
        }
    })
  }

  private def initializedRetryableJobRetriever: RetryableJobs = {
    val esRetryableJobRetriever = new ESSourceRetryableJobs
    val oozieFilteredRetryableJobs = new OozieFilteredRetryableJobs
    esRetryableJobRetriever.successor = Option(oozieFilteredRetryableJobs)
    esRetryableJobRetriever
  }

}
