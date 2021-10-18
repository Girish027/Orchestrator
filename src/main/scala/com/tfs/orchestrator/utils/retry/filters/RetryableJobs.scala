package com.tfs.orchestrator.utils.retry.filters

import scala.collection.mutable.ListBuffer

abstract class RetryableJobs {

  protected def identify(wfIds: ListBuffer[String]): Unit

  var successor = None: Option[RetryableJobs]

  def fetchAllRetryableJobs: List[String] = {
    val retryableJobs = new ListBuffer[String]()
    identify(retryableJobs)
    if (successor.isDefined) {
      successor.get.identify(retryableJobs)
    }
    retryableJobs.toList
  }

}
