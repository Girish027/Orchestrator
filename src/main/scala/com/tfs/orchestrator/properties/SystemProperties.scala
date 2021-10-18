package com.tfs.orchestrator.properties

import com.tfs.orchestrator.exceptions.InitializationException
import com.tfs.orchestrator.utils.Constants._
import com.tfs.orchestrator.utils.RESOURCE_DEGREE.RESOURCE_DEGREE
import com.tfs.orchestrator.utils.{HadoopUtils, Constants, PropertyReader, RESOURCE_DEGREE}
import org.apache.logging.log4j.scala.Logging

/**
 * The class represents the properties associated across the system.
 * The information is constant across all the job/workflows.
 */
trait SystemProperties extends Logging {

  protected val properties = PropertyReader.getSystemProperties()

  //The properties exposed for the external use.
  val jobTracker = properties.getProperty(PROP_SYSTEM_JOB_TRACKER)
  val nameNode = properties.getProperty(PROP_SYSTEM_NAME_NODE)
  val idmPath = properties.getProperty(PROP_SYSTEM_IDM_PATH)
  val speechPath = properties.getProperty(PROP_SYSTEM_SPEECH_PATH)

  /**
   * Get the resource profile for this task.c
   * @param complexity complexity of the task as defined in the catalog.
   * @param size size of the input data as found in HDFS.
   * @return a tuple containing number of vcores and the amount of memory in GB.
   */
  def getResourceProfile(complexity: RESOURCE_DEGREE, size:RESOURCE_DEGREE): Option[(Int, Int)] = {
    val resourceValue = properties.getProperty(SYSTEM_RESOURCE_DEF_PREFIX + SYSTEM_RESOURCE_DEF_DELIMITER +
                                               complexity + SYSTEM_RESOURCE_DEF_DELIMITER + size)
    if (resourceValue != null) {
      val resources = resourceValue.split(":")
      Some(Integer.parseInt(resources(0)), Integer.parseInt(resources(1)))
    } else {
      None
    }
  }

  val lowSize = properties.getProperty(SYSTEM_RESOURCE_DEF_PREFIX + SYSTEM_RESOURCE_DEF_DELIMITER +
    SYSTEM_RESOURCE_SIZE + SYSTEM_RESOURCE_DEF_DELIMITER + SYSTEM_RESOURCE_DEGREE_LOW).toLong
  val mediumSize = properties.getProperty(SYSTEM_RESOURCE_DEF_PREFIX + SYSTEM_RESOURCE_DEF_DELIMITER +
    SYSTEM_RESOURCE_SIZE + SYSTEM_RESOURCE_DEF_DELIMITER + SYSTEM_RESOURCE_DEGREE_MEDIUM).toLong
  val highSize = properties.getProperty(SYSTEM_RESOURCE_DEF_PREFIX + SYSTEM_RESOURCE_DEF_DELIMITER +
    SYSTEM_RESOURCE_SIZE + SYSTEM_RESOURCE_DEF_DELIMITER + SYSTEM_RESOURCE_DEGREE_HIGH).toLong

  def getDataSizeDegree(size:Long=0): RESOURCE_DEGREE = {
    if (size < lowSize) {
      RESOURCE_DEGREE.LOW
    } else if (size < mediumSize) {
      RESOURCE_DEGREE.MEDIUM
    } else {
      RESOURCE_DEGREE.HIGH
    }
  }

  def getProperty(key: String): String = {
    properties.getProperty(key)
  }
}

/**
 * System Properties singleton with validations included.
 */
object SystemPropertiesObject extends SystemProperties {

  /**
   * Validates whether the mandatory properties are present or not.
   */
  private def validate() = {
    if (jobTracker == null) {
      logger.error("MISSING! Necessary System properties are missing")
      throw new InitializationException("Necessary System properties are missing")
    }
    if (properties.getProperty(Constants.PROP_SYSTEM_VALIDATE_HADOOP, "false").toBoolean &&
      !HadoopUtils.hadoopFileExists(properties.getProperty(Constants.PROP_SYSTEM_HADOOP_LOCATION, "/"))) {
      throw new InitializationException("Invalid name node!")
    }
  }

  validate()
  /**
   * Singleton object of system properties.
   */
  def apply(): SystemProperties = SystemPropertiesObject
}
