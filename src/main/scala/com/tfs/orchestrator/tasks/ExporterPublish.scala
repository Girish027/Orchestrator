package com.tfs.orchestrator.tasks

import com.tfs.orchestrator.tasks.SlaPublishTask.{clientName, viewName}
import com.tfs.orchestrator.utils.PropertyReader
import com.tfs.orchestrator.utils.kafka.KafkaWriter
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success, Try}

object ExporterPublish extends Logging {

  def main(args: Array[String]): Unit = {

    val clientName = System.getProperty("clientName")
    val viewName = System.getProperty("viewName")
    val dataStartTime = System.getProperty("dataStartTime").toLong
    val dataEndTime = System.getProperty("dataEndTime").toLong
    val queue = System.getProperty("queue")
    val ingestionType = System.getProperty("ingestionType")
    val userName = System.getProperty("userName")
    val exporterPluginJarLoc = System.getProperty("exporterPluginJarLoc")
    val ucInstanceDate = System.getProperty("ucInstanceDate")
    val replayTask = System.getProperty("replayTask").toBoolean

    val wfId = System.getProperty("oozie.job.id")

    initializeThreadContext(wfId)
    initializeProperties

    val exporterDetails: ExporterDetails = retrieveExporterDetails(clientName, viewName, dataStartTime, dataEndTime, queue, ingestionType, userName, exporterPluginJarLoc, ucInstanceDate, replayTask, wfId)
    val jsonString: String = mapToJson(exporterDetails)

    submitExporterDetails(wfId, jsonString) match {
      case Success(value) => logger.info(s"Submitted exporter details for :$wfId")
      case Failure(ex) => {
        val msg = s"Unable to submit exporter details for :$wfId"
        logger.error(msg, ex)
        throw new RuntimeException(msg)
      }
    }
  }

  private def mapToJson(exporterDetails: ExporterDetails) = {
    implicit val formats = DefaultFormats
    val jsonString = write(exporterDetails)
    logger.debug(s"Exporter details:$jsonString")
    jsonString
  }

  private def retrieveExporterDetails(clientName: String,
                                      viewName: String,
                                      dataStartTime: Long,
                                      dataEndTime: Long,
                                      queue: String,
                                      ingestionType: String,
                                      userName: String,
                                      exporterPluginJarLoc: String,
                                      ucInstanceDate: String,
                                      replayTask: Boolean,
                                      wfId: String
                                     ) = {
    val exporterDateDetails = ExporterDateDetails(dataStartTime, dataEndTime, ucInstanceDate)
    val exporterConfigs = ExporterConfigs(queue, ingestionType, exporterPluginJarLoc)
    val exporterDetails = ExporterDetails(clientName, viewName, wfId, userName, replayTask, exporterDateDetails, exporterConfigs)
    logger.info(s"Successfully configured exporter")
    logger.debug(s"Exporter details:$exporterDetails")
    exporterDetails
  }

  private def submitExporterDetails(wfId: String, jsonString: String): Try[Unit] = {
    Try(KafkaWriter.pushMetrics(PropertyReader.getApplicationProperties().getProperty("kafka.topic"), wfId, jsonString))
  }

  private def initializeProperties = {
    val propertiesDir = System.getProperty("properties.hdfs.dir")
    if (propertiesDir == null) {
      logger.error(s"Unable to fetch properties location: $propertiesDir")
      System.exit(1)
    }
    PropertyReader.initialize(true, propertiesDir)
  }

  private def initializeThreadContext(wfId: String) = {
    ThreadContext.put("client", clientName)
    ThreadContext.put("view", viewName)
    ThreadContext.put("corelatId", wfId)
    ThreadContext.put("service", "ExporterPublish")
  }

  case class ExporterDetails(val clientName: String, val viewName: String, val wfId: String, val userName: String, val replayTask: Boolean, val exporterDateDetails: ExporterDateDetails, val exporterConfigs: ExporterConfigs)

  case class ExporterDateDetails(val dataStartTime: Long, val dataEndTime: Long, val ucInstanceDate: String)

  case class ExporterConfigs(val queue: String, val ingestionType: String, val exporterPluginJarLoc: String)

}
