package com.tfs.orchestrator.utils.handlers

import com.tfs.orchestrator.managers.OrchestratorManager
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.tasks.ExporterPublish.ExporterDetails
import com.tfs.orchestrator.utils.kafka.KafkaReader
import com.tfs.orchestrator.utils.{RESOURCE_DEGREE, WorkflowType}
import net.liftweb.json._
import org.apache.logging.log4j.scala.Logging


class ExporterJobHandler extends Logging {

  private val kafkaReader = getKafkaReaderInstance

  private var isInterrupted = false

  def handleExports(): Unit = {

    while (!isInterrupted && kafkaReader.hasNext) {
      val kafkaMsg = kafkaReader.next()
      logger.debug(kafkaMsg)
      if (isInterrupted)
        kafkaReader.stopReading = true
      submitExporterJob(kafkaMsg)

    }
    kafkaReader.commit

  }

  protected def submitExporterJob(kafkaMsg: String):Unit = {
    val exporterDetails = retrieveExporterDetailsFromJson(kafkaMsg)
    if (exporterDetails.isDefined)
      submitWorkflow(exporterDetails.get)
    else
      logger.warn("Exporter details is not defined.")
  }

  def close(): Unit = {
    kafkaReader.close
  }

  protected def submitWorkflow(exporterDetails: ExporterDetails): Boolean = {
    try {
      val jobScheduleProperties = getJobScheduleProperties(exporterDetails.viewName, exporterDetails.clientName, exporterDetails.userName, exporterDetails.exporterDateDetails.ucInstanceDate, exporterDetails.replayTask, WorkflowType.INGESTION, exporterDetails.wfId)
      val jobId = getOrchestratorManagerInstance.scheduleOnce(jobScheduleProperties)
      logger.info(s"jobId:${jobId.getOrElse("Unknown!!!")}")
    }
    catch {
      case e: Exception => logger.info("Error while submitting workflow.!!!", e)
        return false
    }
    true
  }

  protected def getOrchestratorManagerInstance = {
    new OrchestratorManager()
  }

  private def retrieveExporterDetailsFromJson(inputJson: String): Option[ExporterDetails] = {
    implicit val formats = DefaultFormats
    try {
      val sourceRangeJson = parse(inputJson)
      val exporterDetails = sourceRangeJson.extract[ExporterDetails]
      logger.info(s"exporterDetails:$exporterDetails")
      return Option(exporterDetails)
    }
    catch {
      case ex: MappingException => {
        logger.error(s"Unable to map: $inputJson to ExporterDetails",ex)
      }
      case ex: JsonParser.ParseException => {
        logger.error(s"Unable to parse: $inputJson",ex)
      }

    }
    None
  }

  protected def getJobScheduleProperties(viewName: String, clientName: String, userName: String, ucInstanceDate: String, replayTask: Boolean, wfType: WorkflowType.Value, externalId: String): JobScheduleProperties = {
    logger.info("In getJobScheduleProperties")
    val jobScheduleProps = JobScheduleProperties(clientName, viewName, userName)
    jobScheduleProps.jobComplexity = RESOURCE_DEGREE.MEDIUM
    jobScheduleProps.replay = replayTask
    jobScheduleProps.wfType = wfType
    jobScheduleProps.ucInstanceDate = Option(ucInstanceDate)
    jobScheduleProps.externalId = externalId
    return jobScheduleProps
  }

  def interrupt: Unit = {
    logger.info("Being interrupted")
    isInterrupted = true
  }

  protected def getKafkaReaderInstance = {
    KafkaReader()
  }

}
