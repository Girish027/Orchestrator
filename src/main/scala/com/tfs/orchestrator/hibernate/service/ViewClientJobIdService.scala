package com.tfs.orchestrator.hibernate.service

import java.util

import com.tfs.orchestrator.hibernate.entities.ViewClientJobId
import com.tfs.orchestrator.hibernate.utils.HibernateUtils
import org.apache.logging.log4j.scala.Logging
import org.quartz.CronExpression

import scala.collection.JavaConversions._

/**
 * Data Access Service for ViewClientJobId
 */
class ViewClientJobIdService(hiberanateUtils: HibernateUtils = new HibernateUtils) extends Logging {

  /**
   * To save new entry for the given JobId in the table
   * @param viewClientJobId
   */
  def saveViewClientJobId(viewClientJobId: ViewClientJobId) {
    hiberanateUtils.executeCreate(session => session.save(viewClientJobId))
    logger.info(s"Successfully saved the ViewClientJobId for view : ${viewClientJobId.viewName}, " +
        s"client: ${viewClientJobId.clientId} and jobId ${viewClientJobId.jobId}")
  }

  /**
   * Get the already scheduled cron jobId for the given clientId and viewName.
   * @param viewName
   * @param clientId
   * @return
   */
  def getJobId(viewName: String, clientId: String, cronExpression: String) : Option[String] = {
    val viewClientJobId = getViewClientJobId(viewName, clientId, cronExpression)
    if (viewClientJobId.isDefined) {
      return Some(viewClientJobId.get.jobId)
    }
    None
  }

  /**
    * Returns a list of existing client ids given a list of client ids and a view name.
    * @param viewName
    * @param clientIds
    * @return
    */
  def getClientIdsForExistingJobs(viewName: String, clientIds: List[String]): Option[List[String]] = {
    logger.info(s"Fetch client ids having jobs defined for view name :$viewName and client ids :(${clientIds.mkString(",")})")
    val views: util.List[_] = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from ViewClientJobId where viewName = :viewName and clientId in (:clientIds)").setParameter("viewName", viewName).setParameterList("clientIds", clientIds).list()
    })

    logger.info(s"existing job size :${views.size()}")

    if (views.size() != 0) {
      val vcJobIds = views.toList.asInstanceOf[List[ViewClientJobId]]
      val clients: List[String] = vcJobIds.map(vcJobId => vcJobId.clientId)
      logger.info(s"Fetched client ids :${clients.mkString(",")}")
      return Option(clients)
    }
    None
  }

  /**
    * Returns a list of view client job details given a view,client id and a cron expression.
    *
    * @param viewName
    * @param clientId
    * @param cronExpression
    * @return
    */
  def getViewClientJobIds(viewName: String, clientId: String, cronExpression: String) : Option[List[ViewClientJobId]] = {
      logger.info(s"Fetch job ids defined for view name :$viewName,client id :$clientId and cron expression :$cronExpression ")
    val views: util.List[_] = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from ViewClientJobId where viewName = :viewName and clientId =:clientId and " +
        "cronExpression= :cronExpression").setParameter("viewName", viewName).setParameter("clientId", clientId).
        setParameter("cronExpression", cronExpression).list()
    })

    logger.info(s"existing job size :${views.size()}")

    if (views.size() != 0) {
      return Option(views.toList.asInstanceOf[List[ViewClientJobId]])
    }
    None
  }

  /**
    * Returns a list of view client job given a view, a list of client ids and a cron expression.
    * @param viewName
    * @param clientIds
    * @param cronExpression
    * @return
    */
  def getViewClientJobIds(viewName: String, clientIds: List[String], cronExpression: String) : Option[List[ViewClientJobId]] = {
    logger.info(s"Fetch job ids defined for view name :$viewName,client ids :(${clientIds.mkString(",")}) and cron expression :$cronExpression ")
    val views: util.List[_] = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from ViewClientJobId where viewName = :viewName and clientId in (:clientIds) and " +
        "cronExpression= :cronExpression").setParameter("viewName", viewName).setParameterList("clientIds", clientIds).
        setParameter("cronExpression", cronExpression).list()
    })

    logger.info(s"existing job size :${views.size()}")

    if (views.size() != 0) {
      return Option(views.toList.asInstanceOf[List[ViewClientJobId]])
    }
    None
  }

  /**
   * Get the already scheduled ViewClientJobId Details
   * @param viewName
   * @param clientId
   * @return
   */
  def getViewClientJobId(viewName: String, clientId: String, cronExpression: String) : Option[ViewClientJobId] = {
    val views: util.List[_] = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from ViewClientJobId where viewName = :viewName and clientId = :clientId and " +
        "cronExpression= :cronExpression").setParameter("viewName", viewName).setParameter("clientId", clientId).
        setParameter("cronExpression", cronExpression).list()
    })

    if(views.size() != 0) {
      Option(views.toList.asInstanceOf[List[ViewClientJobId]].head)
    } else {
      //For backward compatibility.
      val views: util.List[_] = hiberanateUtils.executeSelect((session) => {
        session.createQuery("from ViewClientJobId where viewName = :viewName and clientId = :clientId")
          .setParameter("viewName", viewName).setParameter("clientId", clientId).list()
      })
      views.toList.asInstanceOf[List[ViewClientJobId]].find(viewClientId => viewClientId.cronExpression == null)
    }
  }

  /**
   * Delete the scheduled cron jobId for the given clientId and viewName.
   * @param jobId
   * @return
   */
  def deleteByJobId(jobId: String): Unit = {
    logger.info(s"remove job schedule for job id :$jobId")
    hiberanateUtils.executeDelete((session) => {
      session.createQuery("delete from ViewClientJobId where jobId = :jobId").setParameter("jobId", jobId).
        executeUpdate()
    })
  }

  def update(viewClientJobId: ViewClientJobId): Unit = {
    logger.info(s"update view client job :$viewClientJobId")
    hiberanateUtils.executeUpdate((session) => session.update(viewClientJobId))
  }

}
