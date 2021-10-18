package com.tfs.orchestrator.utils

import java.io.{File, FileOutputStream}
import java.util.Properties

import com.tfs.orchestrator.exceptions.RestClientException
import org.apache.logging.log4j.scala.Logging

/**
  * The utilities to save the context data so that it may be queried by
  * other tasks in the oozie workflow.
  */
object OozieUtils extends Logging {


  private val END_TAG = "</value>"
  private val START_TAG = "<value>"

  /**
    * Saves the given key and value to the Oozie context.
    *
    * @param keyName property key.
    * @param exists  property value.
    */
  def saveToOozieContext(keyName: String, exists: String): Unit = {
    try {
      val props = new Properties()
      props.put(keyName, exists.toString)

      val file = new File(System.getProperty(Constants.PROP_OOZIE_ACTION_OUTPUT_CONTEXT))
      val os = new FileOutputStream(file)
      props.store(os, "")
      os.close()
    } catch {
      //Could not save the context. Exit the system to indicate the task failure.
      case e: Exception => logger.error("Could not save to the oozie context", e); System.exit(0)
    }
  }

  /**
    * Saves the given properties to the Oozie context.
    *
    * @param props Properties.
    */
  def saveToOozieContext(props: Properties): Unit = {
    try {
      val file = new File(System.getProperty(Constants.PROP_OOZIE_ACTION_OUTPUT_CONTEXT))
      val os = new FileOutputStream(file)
      props.store(os, "")
      os.flush()
      os.close()
    } catch {
      //Could not save the context. Exit the system to indicate the task failure.
      case e: Exception => logger.error("Could not save to the oozie context", e); System.exit(0)
    }
  }

  /**
    * Get the Oozie status URL template from the properties file located in hadoop.
    *
    * @return
    */
  def fetchOozieWFStatus(wfId: String): Option[String] = {
    val statusUrlTemplate = PropertyReader.getOozieProperties().getProperty("oozie.status.query.url")
    val statusUrl = statusUrlTemplate.replace("WFID", wfId)

    try {
      Option(RestClient.sendGetRequest(statusUrl))
    } catch {
      case ex: RestClientException => {
        logger.error("Not able to retrieve response from Oozie server.", ex)
        None
      }
    }
  }


  /**
    * Retrieve value for the key from Oozie conf.
    *
    * @param conf
    * @param key
    * @return
    */
  def parse(conf: String, key: String): Option[String] = {
    val replayTaskIndex = conf.indexOf(key)
    if (replayTaskIndex != -1) {
      val valueStartIndex = conf.indexOf(START_TAG, replayTaskIndex)
      val valueEndIndex = conf.indexOf(END_TAG, valueStartIndex)
      val value = conf.substring(valueStartIndex + START_TAG.length, valueEndIndex)
      return Option(value)
    }
    None
  }

}
