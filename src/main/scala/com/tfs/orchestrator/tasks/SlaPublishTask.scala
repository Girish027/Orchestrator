package com.tfs.orchestrator.tasks

import com.tfs.orchestrator.utils.SlaRecordPublish._
import com.tfs.orchestrator.utils._
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging

/**
 * Dumps the information about the job run.
 */
object SlaPublishTask extends Logging {
  val propertiesDir = System.getProperty("properties.hdfs.dir")
  var clientName = ""
  var viewName = ""
  var scheduledTime = 0L
  var dataStartTime = 0L
  var dataEndTime = 0L
  var status = ""
  var externalId = ""

  def main(args: Array[String]) {
    clientName = args(0)
    viewName = args(1)
    scheduledTime = DateUtils.utcStringToDate(Constants.OOZIE_NOMINAL_DATE_FORMAT, args(2)).getTime
    dataStartTime = args(3).toLong
    dataEndTime = args(4).toLong
    status = args(5)
    externalId = args(6)

    val wfId = System.getProperty("oozie.job.id")

    ThreadContext.put("client", clientName)
    ThreadContext.put("view", viewName)
    ThreadContext.put("corelatId", wfId)
    ThreadContext.put("service", "SlaPublishTask")

    //Initialize the properties in this new container
    if (propertiesDir == null) {
      logger.error(s"Unable to fetch properties location: $propertiesDir")
      System.exit(1)
    }
    PropertyReader.initialize(true, propertiesDir)
    val oozieResponseStr = OozieUtils.fetchOozieWFStatus(wfId)

    if (!oozieResponseStr.isDefined) {
      System.exit(1)
    }

    SlaRecordPublish.publish(wfId, oozieResponseStr.get, JobMetadata(viewName, clientName, scheduledTime, dataStartTime,
      dataEndTime,externalId),status)

  }
}
