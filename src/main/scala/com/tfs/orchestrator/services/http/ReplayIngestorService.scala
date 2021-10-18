package com.tfs.orchestrator.services.http

import java.util.{Date, NoSuchElementException}

import com.tfs.orchestrator.exceptions.InvalidIngestionType
import com.tfs.orchestrator.hibernate.entities.View
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.utils.{Constants, IngestionType, WorkflowType}
import io.undertow.server.HttpServerExchange
import org.apache.logging.log4j.scala.Logging

class ReplayIngestorService extends ReplayService with Logging {

  var ingestionType:String = "ALL"

  override def handleRequest(httpServerExchange: HttpServerExchange): Unit = {
    retrieveAndSetIngestionType(httpServerExchange)
    parentRequestHandler(httpServerExchange)
  }

  def parentRequestHandler(httpServerExchange: HttpServerExchange): Unit = {
    super.handleRequest(httpServerExchange)
  }

  override def getJobScheduleProperties(viewSchedule: View, client: String, startDate: Date,
    endDate: Date): JobScheduleProperties = {
    val jobScheduleProps = super.getJobScheduleProperties(viewSchedule, client, startDate, endDate, WorkflowType.INGESTION)
    jobScheduleProps.ingestionType = ingestionType
    return jobScheduleProps
  }

  def retrieveAndSetIngestionType(httpServerExchange: HttpServerExchange) = {
    validateAndReturnValue ("datastore", httpServerExchange, true) match {
      case Some(exporterOverrides) =>
        logger.info(s"Ingestion type(s) - ${exporterOverrides}")
        ingestionType = exporterOverrides
      case None =>
        logger.info("Ingestion type is not provided")
        ingestionType = "ALL"
    }
  }

}