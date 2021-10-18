package com.tfs.orchestrator.utils

import java.io.{File, FileInputStream, IOException}
import java.util.Properties

import reflect.runtime.universe._
import com.tfs.orchestrator.exceptions.InitializationException
import org.apache.logging.log4j.scala.{Logger, Logging}

/**
 * A utility function to initialize the Properties in the application.
 */
object PropertyReader extends Logging {

  private var systemProperties: Properties = null
  private var oozieProperties: Properties = null
  private var applicationProperties: Properties = null

  def initialize(onHadoop: Boolean, propertiesDir: String): Unit = {
    try {
      if (onHadoop) {
        systemProperties = getPropertiesFromHDFS(propertiesDir, Constants.FILE_SYSTEM_PROPERTIES)
        oozieProperties = getPropertiesFromHDFS(propertiesDir, Constants.FILE_OOZIE_PROPERTIES)
        applicationProperties = getPropertiesFromHDFS(propertiesDir, Constants.FILE_APPLICATION_PROPERTIES)
      } else {
        systemProperties = getProperties(new File(propertiesDir, Constants.FILE_SYSTEM_PROPERTIES))
        oozieProperties = getProperties(new File(propertiesDir, Constants.FILE_OOZIE_PROPERTIES))
        applicationProperties = getProperties(new File(propertiesDir, Constants.FILE_APPLICATION_PROPERTIES))
      }
    } catch {
      case ex:IOException => {
        throw new InitializationException(ex, "Property files missing in the set path.")
      }
    }
  }

  def getSystemProperties(): Properties = systemProperties
  def getOozieProperties(): Properties = oozieProperties
  def getApplicationProperties(): Properties = applicationProperties

  /**
   * Get the properties.
   * @param file properties file name
   * @return
   */
  def getProperties(file: File): Properties = {
    val properties = new Properties
    val fis = new FileInputStream(file)
    properties.load(fis)
    fis.close()
    return properties
  }

  /**
   *
   * @param propertiesDir
   * @param fileName
   * @return
   */
  def getPropertiesFromHDFS(propertiesDir: String, fileName: String): Properties = {
    val inputStream = HadoopUtils.getInputStream(propertiesDir, fileName)
    val properties = new Properties
    properties.load(inputStream)
    inputStream.close()
    return properties
  }


  def getApplicationPropertiesValue[T: TypeTag](key: String, defaultValue: T): T = {
    val value = applicationProperties.getProperty(key)
    if (value == null) {
      logger.warn(s"Value for key:$key doesn't exists. Will return default value :$defaultValue")
      return defaultValue
    }

    try {
      defaultValue match {
        case _: Long =>
          return value.toLong.asInstanceOf[T]
        case _: Boolean =>
          return value.toBoolean.asInstanceOf[T]
        case _: Int =>
          val t = value.toInt.asInstanceOf[T]
          return t
        case _: Byte =>
          return value.toByte.asInstanceOf[T]
        case _: Float =>
          return value.toFloat.asInstanceOf[T]
        case _: Double =>
          return value.toDouble.asInstanceOf[T]
        case _: String =>
          return value.asInstanceOf[T]
        case _ => {
          logger.warn(s"Class type:${typeOf[T]}, is not supported. Will return default value :$defaultValue")
        }
      }
    }
    catch {
      case ex: Exception => logger.error(s"Exception incurred while fetching value for key:$key. Will return default value :$defaultValue", ex)
    }
    defaultValue
  }


}
