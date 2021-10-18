package com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.impl

import java.util.Date

import com.tfs.orchestrator.cache.{Entry, ExecutionCache}
import com.tfs.orchestrator.utils.{Constants, DateUtils, OozieUtils}
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.{OozieFilter, OozieResponse}

class TimeFilter(filter: OozieFilter) extends OozieFilter {

  override def filter(wfId: String): Option[OozieResponse] = {
    val responseOption: Option[OozieResponse] = filter.filter(wfId)
    if (responseOption.isDefined) {
      val response = responseOption.get
      if (timeForRetry(wfId,response)) {
        return responseOption
      }
    }
    None
  }


  protected def timeForRetry(wfId:String,response: OozieResponse): Boolean = {
    val entry = retrieveEntryKey(response)
    if (entry.isDefined) {
      val retryInterval = getRetryInterval(entry)
      if (retryInterval.isDefined) {
        val jobEndtime = DateUtils.utcStringToEpoch(Constants.OOZIE_RESPONSE_TIME_FORMAT, response.endTime)
        if ((jobEndtime + retryIntervalInMillis(retryInterval.get)) < new Date().getTime) {
          return true
        }
      }
    }
    logger.debug(s"$wfId is not yet timed for retry")
    false
  }

  protected def getRetryInterval(entry: Option[Entry]): Option[String] = {
    ExecutionCache.getRetryInterval(entry.get)
  }

  private def retryIntervalInMillis(retryInterval: String): Long = {
    retryInterval.toLong * 60 * 1000
  }

  override def retrieveEntryKey(responseObj: OozieResponse): Option[Entry] = {
    filter.retrieveEntryKey(responseObj)
  }
}


