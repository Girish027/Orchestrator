package com.tfs.orchestrator.hibernate.service

import java.util.Date
import java.util
import com.tfs.orchestrator.hibernate.entities.View
import com.tfs.orchestrator.hibernate.utils.HibernateUtils
import com.tfs.orchestrator.utils.{Utils, Constants, PropertyReader}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._

/**
  * ViewService used for the db operations on view table
  */

class ViewService(hiberanateUtils: HibernateUtils = new HibernateUtils) extends Logging {

  /**
    * To save new view in the table
    *
    * @param view
    */
  def saveView(view: View) {
    hiberanateUtils.executeCreate(session => session.save(view))
    logger.info(s"Successfully saved the View for : ${view.viewName}, clientIds: ${view.clientIds}")
  }

  def updateView(view: View) {
    view.dateModified = new Date()
    hiberanateUtils.executeUpdate((session) => session.update(view))
    logger.info(s"Successfully updated the View for view : ${view.viewName}, clientIds: ${view.clientIds}")
  }

  def deleteView(view: View) {
    hiberanateUtils.executeDelete((session) => session.delete(view))
    logger.info(s"Successfully deleted view entry from view table ${view}")
  }


  def getAllViews(): List[View] = {
    val views = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from View").list()
    })
    if (null != views && views.size() != 0) {
      return (views.asScala).toList.asInstanceOf[List[View]]
    } else {
      return List()
    }
  }

  def getViewByNameAndCron(viewName: String, cronExpression: String):List[View] = {
    val views = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from View where viewName =:name and cronExpression =:cronExpression and dataCenter =:dataCenter").
        setParameter("name", viewName).setParameter("cronExpression", cronExpression).
        setParameter("dataCenter", PropertyReader.getSystemProperties().getProperty(Constants.DATA_CENTER_NAME)).list()
    })
    if (null != views && views.size() != 0) {
      logger.info(s"Retrieved view details by view name :$viewName and cron expression :$cronExpression")
      import scala.collection.JavaConverters._
      views.asScala.map(_.asInstanceOf[View]).toList
    } else {
      null
    }
  }

  def getViewByNameAndClientId(viewName: String, clientId: String): View = {
    val views = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from View where viewName =:name").
        setParameter("name", viewName).list()
    })
    if (null != views && views.size() != 0) {
      val relevantViews = for {
        view <- (views.asScala).toList.asInstanceOf[List[View]];
        if (Utils.splitAndConvertToList(view.clientIds).contains(clientId))
      } yield view
      return relevantViews.head
    } else {
      return null
    }
  }

  def getViewsByName(viewName: String): List[View] = {
    val views = hiberanateUtils.executeSelect((session) => {
      session.createQuery("from View where viewName =:name").
        setParameter("name", viewName).list()
    })
    if (null != views && views.size() != 0) {
      (views.asScala).toList.asInstanceOf[List[View]]
    } else {
      List()
    }
  }

}
