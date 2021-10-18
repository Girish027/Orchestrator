package com.tfs.orchestrator.managers

import java.util.Date

import com.tfs.orchestrator.catalog.polling.{ClientExecutionProperties, ClientsCronExpression, ViewSchedule}
import com.tfs.orchestrator.hibernate.entities.{View, ViewClientJobId}
import com.tfs.orchestrator.hibernate.service.{ViewClientJobIdService, ViewService}
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.utils.{Utils, DateUtils, RESOURCE_DEGREE}
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.ListBuffer

class ScheduleManager extends Logging {

  private val viewService = new ViewService()
  private val viewClientJobIdService: ViewClientJobIdService = new ViewClientJobIdService()
  private val orchestratorManager = new OrchestratorManager


  /**
    * Deschedules all view-cron-client jobs which doesn't exists in the new View Schedule.
    *
    * @param viewSchedule
    */
  def descheduleNonExistentCrons(viewSchedule: ViewSchedule): Unit = {
    logger.info("Delete non existent cron bucket clients")
    val allNewCronExprs = getCronExpressionList(viewSchedule)
    val deletedViews = getViewService.getViewsByName(viewSchedule.viewName).filterNot(savedView =>
      allNewCronExprs.contains(savedView.cronExpression))
    deletedViews.foreach(deletedView => deScheduledViewClientsAndUpdate(deletedView,
      deletedView.clientIds.split(",").toList))
  }


  def scheduleNewClients(targetView: View,newlyAddedClients: List[String]): Unit = {
    if (newlyAddedClients.size != 0) {
      logger.info(s"schedule new clients :(${newlyAddedClients.mkString(",")}) for view :$targetView")
      scheduleViewClientsAndUpdate(targetView, newlyAddedClients)
    }
  }

  def deScheduleDeletedClients(localView: View, deletedClients: List[String]): Unit = {
    if (deletedClients.size != 0 && deletedClients.exists(localView.clientIds.contains(_))) {
      logger.info(s"deschedule deleted clients :(${deletedClients.mkString(",")}) for view :$localView")
      deScheduledViewClientsAndUpdate(localView, deletedClients)
    }
  }

  def updateJobEndTime(localView: View, targetView: View): Unit = {
    if (!localView.compareJobEndWith(targetView)) {
      logger.info(s"update job end time for view: $localView")
      var jobChangeStatus = true
      val newClientIds = targetView.clientIds.split(",")
      val existingClientIds = localView.clientIds.split(",")
      val commonClientIds: List[String] = getCommonClients(newClientIds, existingClientIds)
      val viewClientJobIds = getViewClientJobIDService.getViewClientJobIds(targetView.viewName, commonClientIds, targetView.cronExpression)
      if (viewClientJobIds.isDefined) {
        val currentEpochTime = System.currentTimeMillis()
        viewClientJobIds.get.foreach(jobId => {
          logger.info(s"job details :$jobId and current time :$currentEpochTime")
          if (jobId.jobEndEpochTime > currentEpochTime) {
            if (!getOrchestratorManager.change(jobId, new Date(targetView.jobEndTime.toLong))) {
              logger.error(s"Failed to update end time for job :${jobId.jobId} belonging to view :${targetView.viewName} and cron :${targetView.cronExpression}")
              jobChangeStatus = false
            }
          }

        })
      }
      if (jobChangeStatus) {
        logger.info(s"Job end date updated for the view :${targetView.viewName} and cron :${targetView.cronExpression}")
        localView.jobEndTime = targetView.jobEndTime
        getViewService.updateView(localView)
      }
    }
  }

  /**
    * Delete all the view cron jobs that are not in catalog.
    *
    * @param viewSchedules
    */
  def deleteNonExistentViews(viewSchedules: List[ViewSchedule]): Unit = {
    logger.info("Delete non existent view schedules")
    val newViewNames = viewSchedules.map(view => view.viewName)
    val existingViews = getViewService.getAllViews()
    val deletedViews = existingViews.filterNot(existingView => newViewNames.contains(existingView.viewName))

    deletedViews.foreach(deletedView => {
      deScheduledViewClientsAndUpdate(deletedView, deletedView.clientIds.split(",").toList)
    })

    if (deletedViews.size != 0) {
      logger.info(s"Some existing views are deleted from catalog service ${deletedViews.flatMap(view => view.viewName)}")
    }
  }


  /**
    * Deschedule jobs for a view and given clients
    *
    * @param localView
    * @param clientIds
    */
  protected def deScheduledViewClientsAndUpdate(localView: View, clientIds: List[String]): Unit = {
    clientIds.foreach(clientId => {
      if (Utils.splitAndConvertToList(localView.clientIds).contains(clientId)) {
        val succeeded = deScheduleJob(localView, clientId)
        if (succeeded) {
          logger.info(s"Successfully descheduled the job: ${localView.viewName} for the client ${clientId}")
          deleteClientFromView(localView, clientId)
        } else {
          logger.error(s"Error while killing scheduled job ${localView.viewName} for the client ${clientId}")
        }
      }
    })
  }

  def deScheduleJob(localView: View, clientId: String): Boolean = {
    logger.info(s"Deschedule job for view :$localView, client id :$clientId")
    val viewClientJobIds: Option[List[ViewClientJobId]] = getViewClientJobIDService.getViewClientJobIds(localView.viewName, clientId, localView.cronExpression)
    var deScheduleStatus = true;
    if (viewClientJobIds.isDefined) {
      var jobIds: List[ViewClientJobId] = viewClientJobIds.get
      val nextHourDate = getNextHourDate
      jobIds.foreach(jobId => {
        val currentEpochTime = System.currentTimeMillis()
        if (jobId.jobEndEpochTime > currentEpochTime) {
          if (jobId.jobStartEpochTime > currentEpochTime) {
            deScheduleStatus = getOrchestratorManager.killCronJobById(jobId)
          }
          else {
            deScheduleStatus = getOrchestratorManager.change(jobId, nextHourDate)
          }
        }
      })
    }
    deScheduleStatus
  }

  /**
    * Schedules job for the given view and for the list of clients.
    *
    * @param targetView
    * @param clientIds
    */
  def scheduleViewClientsAndUpdate(targetView: View, clientIds: List[String]): Unit = {
    val viewClientIDs: Option[List[String]] = getViewClientJobIDService.getClientIdsForExistingJobs(targetView.viewName, clientIds)
    logger.info(s"Existing client ids : ${viewClientIDs}")
    var existingClientIds: List[String] = List[String]()
    var jobsProperties: ListBuffer[JobScheduleProperties] = new ListBuffer[JobScheduleProperties]()

    if (viewClientIDs.isDefined) {
      existingClientIds = viewClientIDs.get.distinct
      logger.info(s"Existing distinct client ids : ${existingClientIds}")
      jobsProperties.appendAll(getJobsProperties(targetView, existingClientIds, getNextHourDate))
    }
    var clients = clientIds.filterNot(client => existingClientIds.contains(client))
    if (clients.size > 0) {
      jobsProperties.appendAll(getJobsProperties(targetView, clients))
    }

    jobsProperties.foreach(jp => {
      if (getOrchestratorManager.createCronJob(jp)) {
        updateViewWithClient(targetView, jp.clientIdentifier)
        logger.info(s"Successfully scheduled job: ${jp.viewName} for the client ${jp.clientIdentifier}")
      } else {
        logger.error(s"There was an error while scheduling job ${jp.viewName} for the client ${jp.clientIdentifier}")
      }
    }
    )
  }

  /**
    * Create JobScheduleProperties object to be used to create or update Orchestrator Schedulers
    *
    * @param view
    * @return
    */
  def getJobsProperties(view: View, clients: List[String]): List[JobScheduleProperties] = {
    getJobsProperties(view, clients, new Date((view.jobStartTime).toLong))
  }


  def getJobsProperties(view: View, clients: List[String], jobStartTime: Date): List[JobScheduleProperties] = {
    logger.info(s"Form job schedule properties for view :${view.viewName}, clients :${clients.mkString(",")} and job start time :$jobStartTime")
    val jobsProperties: ListBuffer[JobScheduleProperties] = new ListBuffer[JobScheduleProperties]()
    try {
      clients.foreach(client => {
        if (client.equals(view.clientIds)) {
          val jobProperties: JobScheduleProperties = JobScheduleProperties.apply(client, view.viewName, view.userName)
          enrichJobScheduleProperties(view, jobProperties, jobStartTime)
          jobsProperties += jobProperties
        }
      }
      )
    } catch {
      case e: Exception =>
        logger.error("Invalid data to create or update scheduler jobs")
    }
    jobsProperties.toList
  }


  private def enrichJobScheduleProperties(view: View, jobProperties: JobScheduleProperties, jobStartTime: Date) = {
    jobProperties.jobCronExpression = view.cronExpression
    jobProperties.jobStartTime = jobStartTime
    jobProperties.jobEndTime = new Date((view.jobEndTime).toLong)
    jobProperties.jobComplexity = RESOURCE_DEGREE.MEDIUM
  }

  /**
    * Deletes the clientId from the list of clients for the given viewName
    *
    * @param view
    * @param clientId
    */
  private def deleteClientFromView(view: View, clientId: String): Unit = {
    logger.info(s"delete client id :$clientId from view :$view")
    val viewService = getViewService
    val clientIds = view.clientIds.split(",").toList
    var newList = List[String]()
    if (clientIds.contains(clientId)) {
      newList = clientIds.filterNot(elm => elm == clientId)
      if (!newList.isEmpty) {
        view.clientIds = newList.mkString(",")
        viewService.updateView(view)
      } else {
        view.clientIds = ""
        viewService.deleteView(view)
      }
    }
  }

  /**
    * adds a clientId for the existing clientIds for the given viewName
    *
    * @param view
    * @param clientId
    */
  private def updateViewWithClient(view: View, clientId: String): Unit = {
    logger.info(s"Update view: $view with clientId :$clientId")
    val existingViewsList = getViewService.getViewByNameAndCron(view.viewName, view.cronExpression)
    val viewService = getViewService
    if (null == existingViewsList) {
      logger.info(s"Inserting new View: $view")
      viewService.saveView(createViewEntity(view, clientId))
    }
    else {
      val client = for (v1 <- existingViewsList if Utils.splitAndConvertToList(v1.clientIds).contains(clientId)) yield clientId
      logger.info(s"Existing clients already in the view as comma separated : ${client}")
      if (client == null || client == "" || client == Nil) {
        logger.debug(s"Inserting new client View: $view")
        viewService.saveView(createViewEntity(view, clientId))
      }
    }
  }

  private def createViewEntity(targetView: View, clientId: String): View = {
    val view: View = new View()
    view.id = targetView.id
    view.viewName = targetView.viewName
    view.clientIds = clientId
    view.cronExpression = targetView.cronExpression
    view.userName = targetView.userName
    view.jobStartTime = targetView.jobStartTime
    view.jobEndTime = targetView.jobEndTime
    view.jobComplexity = targetView.jobComplexity
    view.dateCreated = targetView.dateCreated
    view.dateModified = targetView.dateModified
    view.userName = targetView.userName
    view.dataCenter = targetView.dataCenter

    view
  }

  private def getCronExpressionList(viewSchedule: ViewSchedule) = {
    viewSchedule.clientCronExpressionList.map(clientsCron => clientsCron.cronExpression)
  }

  protected def getOrchestratorManager: OrchestratorManager = orchestratorManager

  protected def getViewService: ViewService = viewService

  protected def getViewClientJobIDService: ViewClientJobIdService = viewClientJobIdService

  protected def getNextHourDate: Date = DateUtils.convertToNextHourDate(new Date())

  protected def getCommonClients(newClientIds: Array[String], existingClientIds: Array[String]): List[String] = newClientIds.filter(clientId => {
    existingClientIds.contains(clientId)
  }).toList

}
