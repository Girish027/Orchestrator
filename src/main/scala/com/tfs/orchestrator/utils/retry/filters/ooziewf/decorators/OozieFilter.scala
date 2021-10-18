package com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators

import com.tfs.orchestrator.cache.Entry
import org.apache.logging.log4j.scala.Logging

trait OozieFilter extends Logging {

  def filter(wfId: String): Option[OozieResponse]

  def retrieveEntryKey(responseObj: OozieResponse): Option[Entry]

  init()

  private def init(): Unit = {
    logger.info(s"${this.getClass.getSimpleName} is being initialized.")
  }

}

case class OozieResponse(
                          conf: String,
                          run: Double,
                          id: String,
                          status: String,
                          endTime: String
                        )

