package com.tfs.orchestrator.catalog.polling

import java.util.{Date, UUID}

import com.tfs.orchestrator.cache.ExecutionCache
import com.tfs.orchestrator.exceptions.InvalidJsonException
import com.tfs.orchestrator.hibernate.entities.View
import com.tfs.orchestrator.hibernate.service.ViewService
import com.tfs.orchestrator.managers.ScheduleManager
import com.tfs.orchestrator.utils.{Constants, _}
import net.liftweb.json._
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.io.Source

/**
  * Method that periodically gets the snapshot of the views from catalogs service
  * and schedules the orchestrator jobs accordingly.
  *
  * @param url      - url for getting the snapshot from catalog service(GET)
  * @param interval - how frequent catalog service must be queried
  */
class CatalogReader(url: String, interval: Int) extends Thread with Logging {

  private val viewService = new ViewService()

  implicit val formats = DefaultFormats

  /**
    * Gets the catalog snapshot for orchestrator every specified interval and will take care of create, delete or modify
    * the orchestrator jobs
    */
  override def run(): Unit = {
    while (true) {
      var catalogSnapshot: String = ""
      /* for testing we can set a system variable env to dev and it can query the data from the static file instead from
      catalog service */
      if (Constants.DEV_MODE.equalsIgnoreCase(System.getProperty(Constants.RUN_ENV))) {
        catalogSnapshot = Source.fromFile(PropertyReader.getApplicationProperties().getProperty(Constants.SAMPLE_SOURCE_LOC)).getLines.mkString
        logger.debug(s"Deployment mode is set to DEV and the snapshot read from the sample file: ${catalogSnapshot}")
        updateOrchestrator(getViewSchedulesFromJson(catalogSnapshot))
      } else {
        try {
          catalogSnapshot = RestClient.sendGetRequest(url)
          logger.debug(s"Views snapshot from catalog service: ${catalogSnapshot}")
          updateOrchestrator(getViewSchedulesFromJson(catalogSnapshot))
        } catch {
          case e: Exception => {
            logger.error("Unable to make rest request to catalog service", e)
          }
        }
      }
      Thread.sleep(interval)
    }
  }

  /**
    * Method that will parse the output of the catalog snapshot and will take care of create, delete or update orchestrator
    * jobs
    *
    * @param viewSchedules - View schedules
    */
  def updateOrchestrator(viewSchedules: List[ViewSchedule]): Unit = {
    cacheExecutionProperties(viewSchedules)
    try {
      viewSchedules.foreach(f = viewSchedule => {
          val clientsCrons: List[ClientsCronExpression] = viewSchedule.clientCronExpressionList
          clientsCrons.foreach(clientsCron => {
            val catalogViewCronClientList = createViewEntity(viewSchedule, clientsCron)
            var filterClients = new FilterClientsByDataCenter
            val allClients = filterClients.applyFilter(clientsCron.clientExecProps, List[String]())
            val localViewCronClientList = getViewService.getViewByNameAndCron(viewSchedule.viewName, clientsCron.cronExpression)
            var existingClientIds = List[String]();

            if (localViewCronClientList != null) {
              localViewCronClientList.foreach(localView => {
                existingClientIds = existingClientIds ::: getExistingClientIds(localView)
              })
            }

            val deletedClients: List[String] = existingClientIds.filterNot(allClients.contains(_))
            val newlyAddedClients: List[String] = allClients.filterNot(existingClientIds.contains(_))

            logger.debug(s"Existing Clients for view : ${viewSchedule.viewName} and Cron : ${clientsCron.cronExpression} is ${existingClientIds}")

            catalogViewCronClientList.foreach(targetView => {
              logger.info(s"CATALOG READER : Received view details for view name: ${targetView.viewName} and" +
                s" cron expression: ${clientsCron.cronExpression} is: (${targetView})")

              if (localViewCronClientList != null) {
                logger.info(s"CATALOG READER : Existing view details for view name: ${targetView.viewName} and" +
                  s"cron expression: ${clientsCron.cronExpression} is: ${localViewCronClientList}")

                localViewCronClientList.foreach(localView => {
                  if (Utils.splitAndConvertToList(localView.clientIds).contains(targetView.clientIds)) {
                    getScheduleManager.updateJobEndTime(localView, targetView)
                  }

                  getScheduleManager.deScheduleDeletedClients(localView, deletedClients)
                })
              }
              getScheduleManager.scheduleNewClients(targetView, newlyAddedClients)
            })
          })
          getScheduleManager.descheduleNonExistentCrons(viewSchedule)
        }
    )
      getScheduleManager.deleteNonExistentViews(viewSchedules)
    } catch {
      case e: Exception => {
        logger.error(s"something went wrong while updating the orchestrator with the view", e)
      }
    }
  }


  private def cacheExecutionProperties(viewSchedules: List[ViewSchedule]) = {
    logger.info("Caching execution properties.")
    import scala.concurrent.ExecutionContext.Implicits.global
    try {
      Future {
        ExecutionCache.cacheExecutionProperties(viewSchedules)
      }
    }
    catch {
      case ex: RuntimeException => logger.error("Unable to cache execution properties.", ex)
    }
  }

  protected def getExistingClientIds(localView: View): List[String] = {
    localView.clientIds.split(",").toList
  }

  /**
    * Creates a view entity that from the json object
    *
    * @param viewSchedule - viewSchedule object
    * @return - view object
    */
  def createViewEntity(viewSchedule: ViewSchedule, clientsCron: ClientsCronExpression): List[View] = {
    var views = List.empty[View]
    clientsCron.clientExecProps foreach (client => {
      try {
        var view = new View;
        view.id = UUID.randomUUID().toString
        view.viewName = viewSchedule.viewName
        view.clientIds = client.name
        view.cronExpression = clientsCron.cronExpression
        view.userName = viewSchedule.userName
        if (client.jobStartTime == null) {
          view.jobStartTime = viewSchedule.jobStartTime
        }
        else {
          view.jobStartTime = client.jobStartTime
        }
        if (client.jobEndTime == null) {
          view.jobEndTime = viewSchedule.jobEndTime
        }
        else {
          view.jobEndTime = client.jobEndTime
        }
        view.jobComplexity = viewSchedule.complexity
        view.dateCreated = new Date()
        view.dateModified = view.dateCreated
        view.dataCenter = PropertyReader.getSystemProperties().getProperty(Constants.DATA_CENTER_NAME)
        if (view.userName == null) view.userName = "oozie"
        views ::= view
      } catch {
        case e: Exception =>
          logger.error("There was an error while creating view entity", e)
      }
    })

    views
  }


  /**
    * Parse the viewDefinition from catalog service and create list of ViewSchedules
    *
    * @param jsonStr
    * @return
    */
  def getViewSchedulesFromJson(jsonStr: String): List[ViewSchedule] = {
    var viewSchedules: List[ViewSchedule] = null
    logger.debug("Parsing snapshot to update orchestrator jobs")
    try {
      viewSchedules = parse(jsonStr).extract[List[ViewSchedule]]
      return viewSchedules
    } catch {
      case e: Exception => {
        logger.error("There was error while parsing json", e)
        throw new InvalidJsonException(e.getMessage)
      }
    }
  }

  protected def getScheduleManager = new ScheduleManager

  protected def getViewService: ViewService = viewService

}

/**
  * View Definition as received from Catalog.
  *
  * @param viewName
  * @param clientCronExpressionList
  * @param userName
  * @param jobStartTime
  * @param jobEndTime
  * @param complexity
  */
case class ViewSchedule(val viewName: String, val clientCronExpressionList: List[ClientsCronExpression], val userName: String,
                        val jobStartTime: String, val jobEndTime: String, val complexity: String)

case class ClientsCronExpression(val clientExecProps: List[ClientExecutionProperties], cronExpression: String)

case class ClientExecutionProperties(val name: String, val executionProperties: Map[String, String],
                                     val environmentProperties: Option[Map[String, String]], val jobStartTime: String, val jobEndTime: String)
