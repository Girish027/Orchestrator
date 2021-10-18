package com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.impl

import com.tfs.orchestrator.cache.Entry
import com.tfs.orchestrator.utils.{Constants, OozieUtils}
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.{OozieFilter, OozieResponse}

class ReplayFilter(filter: OozieFilter) extends OozieFilter {

  override def filter(wfId: String): Option[OozieResponse] = {
    val response: Option[OozieResponse] = filter.filter(wfId)
    if (response.isDefined) {
      if (!isReplayTask(wfId,response.get)) {
        return response
      }
    }
    None
  }

  private def isReplayTask(wfId:String,response: OozieResponse): Boolean = {
    val value = OozieUtils.parse(response.conf, Constants.KEY_REPLAY_TASK)
    if (value.isDefined && value.get.equals("false")) {
      return false
    }
    logger.debug(s"$wfId is a replay task")
    true
  }

  override def retrieveEntryKey(responseObj: OozieResponse): Option[Entry] = {
    filter.retrieveEntryKey(responseObj)
  }
}
