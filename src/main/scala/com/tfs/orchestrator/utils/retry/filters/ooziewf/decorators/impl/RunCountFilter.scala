package com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.impl

import com.tfs.orchestrator.cache.{Entry, ExecutionCache}
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.{OozieFilter, OozieResponse}

class RunCountFilter(filter: OozieFilter) extends OozieFilter {

  override def filter(wfId: String): Option[OozieResponse] = {
    val responseOption: Option[OozieResponse] = filter.filter(wfId)
    if (responseOption.isDefined) {
      val response = responseOption.get
      if (!retryCountExceeded(wfId,response)) {
        return responseOption
      }
    }
    None
  }


  protected def retryCountExceeded(wfId:String,response: OozieResponse): Boolean = {
    val runCount = response.run
    val entry = retrieveEntryKey(response)
    if (entry.isDefined) {
      val retryCount = getRetryCount(entry)
      if (retryCount.isDefined && runCount < retryCount.get.toInt) {
        return false
      }
    }
    logger.debug(s"Retry count exceeded for $wfId")
    true
  }


  protected def getRetryCount(entry: Option[Entry]): Option[String] = {
    ExecutionCache.getRetryCount(entry.get)
  }

  override def retrieveEntryKey(responseObj: OozieResponse): Option[Entry] = {
    filter.retrieveEntryKey(responseObj)
  }
}


