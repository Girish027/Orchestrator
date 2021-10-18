package com.tfs.orchestrator.cache

import com.tfs.orchestrator.catalog.polling.{ClientExecutionProperties, ViewSchedule}
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable
import scala.collection.mutable.Map


object ExecutionCache extends Logging {

  val SLA_BREACH_KEY = "slaBreachTime"
  val RETRY_INTERVAL = "retryInterval"
  val RETRY_COUNT = "retryCount"
  private var entries = Map[String, Map[Entry, String]]()

  /**
    * Cache execution properties.
    *
    * @param viewSchedules
    */
  def cacheExecutionProperties(viewSchedules: List[ViewSchedule]): Unit = {
    logger.debug(s"Caching execution properties from view schedule of size:${viewSchedules.size}")
    try {
      viewSchedules.foreach(viewSchedule => {
        val clientExecProps: List[ClientExecutionProperties] = viewSchedule.clientCronExpressionList.flatMap(_.clientExecProps)
        clientExecProps.foreach(clientExecProp => {
          val entry = Entry(viewSchedule.viewName, clientExecProp.name)
          clientExecProp.executionProperties.foreach { case (k, v) => {
            val entryList: Option[Map[Entry, String]] = entries.get(k)
            if (entryList.isEmpty) {
              entries += (k -> Map(entry -> v))
            }
            else {
              var value: Map[Entry, String] = entryList.get
              value += (entry -> v)
            }
          }
          }

        })

      })
    }
    catch {
      case e: Exception => {
        logger.error(s"Failed while caching execution properties.", e)
      }
    }
  }

  /**
    * Retrieve cached SLA entries.
    * @return
    */
  def getSlaEntries(): List[CacheEntry] = {
    fetchEntries(SLA_BREACH_KEY)
  }

  def getRetryInterval(entry: Entry): Option[String] = {
    getProperties(entry,RETRY_INTERVAL)
  }

  def getRetryCount(entry: Entry): Option[String] = {
    getProperties(entry,RETRY_COUNT)
  }

  private def getProperties(entry: Entry, cacheKey:String): Option[String] = {
    val cacheEntry: Option[mutable.Map[Entry, String]] = entries.get(cacheKey)
    if (cacheEntry.isDefined) {
      return cacheEntry.get.get(entry)
    }
    Option.empty
  }

  private def fetchEntries(key:String) :List[CacheEntry] = {
    entries.getOrElse(key, Map[Entry, String]()).map {
      case (k, v) => {
        CacheEntry(k.viewName, k.clientName, v)
      }
    }.toList
  }

  def invalidate(): Unit = {
    entries = Map.empty
  }

}

case class Entry(viewName: String, clientName: String)
case class CacheEntry(viewName: String, clientName: String, value: String)
