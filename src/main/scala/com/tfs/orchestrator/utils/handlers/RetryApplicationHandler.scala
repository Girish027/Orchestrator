package com.tfs.orchestrator.utils.handlers

class RetryApplicationHandler {

  def fetchAllRetryableApplications: List[String] = {
    List[String]()
  }

  def retryApplications(wfIds: List[String]): Unit = {

  }

  def retryFailedApplications():Unit ={
    val wfIds : List[String] = fetchAllRetryableApplications
    retryApplications(wfIds)
  }

}
