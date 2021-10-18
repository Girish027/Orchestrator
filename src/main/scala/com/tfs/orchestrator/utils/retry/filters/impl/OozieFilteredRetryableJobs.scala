package com.tfs.orchestrator.utils.retry.filters.impl

import com.tfs.orchestrator.utils.retry.filters.RetryableJobs
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.OozieFilter
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.impl.{BaseFilter, ReplayFilter, RunCountFilter, TimeFilter}
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.ListBuffer

final class OozieFilteredRetryableJobs extends RetryableJobs with Logging {

  private lazy val filter: OozieFilter = initializeOozieFilters

  override def identify(wfIds: ListBuffer[String]): Unit = {
    val filteredOutLists = wfIds.filter(wfId => {
      val filteredResponse = filter.filter(wfId)
      filteredResponse.isEmpty
    })
    wfIds --= filteredOutLists
  }

  protected def initializeOozieFilters: OozieFilter = {
    logger.info("Initializing Oozie filters")
    new RunCountFilter(new TimeFilter(new ReplayFilter(new BaseFilter)))

  }
}
