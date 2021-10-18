package com.tfs.orchestrator.hibernate.utils

import java.io.File
import java.util

import com.tfs.orchestrator.exceptions.{HibernateException, InitializationException}
import com.tfs.orchestrator.utils.Constants
import org.hibernate.cfg.Configuration
import org.hibernate.service.{ServiceRegistry, ServiceRegistryBuilder}
import org.hibernate.{Session, SessionFactory}

/**
 * Contains the boilerplate code for all the hibernate CRUD actions.
 */
class HibernateUtils {

  def getHibernateSession(): Session = {
    HibernateUtils.getSessionFactory.openSession()
  }

  /**
   * CREATE operation.
   * @param createFunction
   */
  def executeCreate(createFunction: (Session)=>Unit): Unit = {
    enclosed(getHibernateSession()) (session => Option(createFunction(session)))
  }

  /**
   * READ operation.
   * @param selectFunction
   */
  def executeSelect(selectFunction: (Session)=>util.List[_]): util.List[_] = {
    var views: util.List[_] = null
    enclosed(getHibernateSession()) (session => views = selectFunction(session))
    views
  }

  /**
   * UPDATE operation.
   * @param updateFunction
   */
  def executeUpdate(updateFunction: (Session)=>Unit) {
    enclosed(getHibernateSession()) (session => Option(updateFunction(session)))
  }

  /**
   * DELETE operation.
   * @param deleteFunction
   */
  def executeDelete(deleteFunction: (Session)=>Unit) {
    enclosed(getHibernateSession()) (session => Option(deleteFunction(session)))
  }

  /**
   * Utility function for enclosing the
   * @param session
   * @param f function to perform in
   * @tparam C type of Session
   * @return
   */
  def enclosed[C <: Session](session: C)(f: C => Unit) {
    try {
      val txn = session.beginTransaction()
      f(session)
      txn.commit()
    } catch {
      case e: Exception => throw new HibernateException(e, "There was an error while trying to CRUD the entity")
    }
    finally { session.close() }
  }

}

/**
 * Initializes the SessionFactory for use across various entity access layers.
 */
object HibernateUtils {
  private var sessionFactory: SessionFactory = null

  def initialize(): Unit = {
    try {
      val configuration: Configuration = new Configuration()
      configuration.configure(new File(System.getProperty(Constants.COMMAND_PROPS_DIR_KEY) +
        System.getProperty("file.separator") + Constants.FILE_HIBERNATE_CONFIG))
      val serviceRegistry: ServiceRegistry = new ServiceRegistryBuilder()
        .applySettings(configuration.getProperties).buildServiceRegistry
      sessionFactory = configuration.buildSessionFactory(serviceRegistry)
    } catch {
      case ex: Throwable => {
        throw new InitializationException(ex, "Initial SessionFactory creation failed.")
      }
    }
  }

  def getSessionFactory: SessionFactory = sessionFactory

  def shutdown {
    getSessionFactory.close()
  }

}
