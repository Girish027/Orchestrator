package com.tfs.orchestrator.tasks

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.{Date, Properties}

import com.tfs.orchestrator.utils.RESOURCE_DEGREE.RESOURCE_DEGREE
import com.tfs.orchestrator.utils.SlaRecordPublish._
import com.tfs.orchestrator.utils._
import net.liftweb.json._
import org.apache.logging.log4j.ThreadContext
import org.apache.logging.log4j.scala.Logging

import scala.util.Try

/**
 * This service is called before calling the Processing task.
 *
 * 1. Calculates the size of the input data from the directories.
 * 2. The input data size coupled with complexity defined in the view definition, an appropriate resource profile
 * is selected. Details here.
 * 3. Figures the expected output directory based on mappings so that
 *
 */
class PreprocessingTask extends Logging {

  protected var viewName: String = null
  protected var clientName: String = null
  protected var instance_time: Date = null
  protected var processComplexity = RESOURCE_DEGREE.LOW
  protected var externalId = ""
  val propertiesDir = System.getProperty("properties.hdfs.dir")
  protected val RUNNING_STATUS = "RUNNING"

  implicit val formats = DefaultFormats

  /**
    * Fetch and parse the source range information.
    *
    * @param processComplexity
    * @return
    */
  def getProcessingProperties(processComplexity: RESOURCE_DEGREE, instanceTime: Date): Properties = {
    val sourceRangeJson = parse(fetchCatalogSourceRangeInfo())
    val sourceRangeObj = sourceRangeJson.extract[CatalogResponse]

    val stime = sourceRangeObj.scheduleStartTime
    val etime = sourceRangeObj.scheduleEndTime

    val userName = System.getProperty(Constants.KEY_OOZIE_USER_NAME)
    val jobExecutionExpression = sourceRangeObj.jobExecutionExpression

    val startTime = DateUtils.epochToUTCString(Constants.SPARTAN_DATE_FORMAT, stime.toLong)
    val endTime = DateUtils.epochToUTCString(Constants.SPARTAN_DATE_FORMAT, etime.toLong)
    val pathsList = sourceRangeObj.sourceTimeRangeList.flatMap(sourceTimeRange => sourceTimeRange.pathList)

    var queueName = Try(sourceRangeObj.executionProperties.get(executionPropertyName.queue.toString)).getOrElse("")
    var driverMemory = Try(sourceRangeObj.executionProperties.get(executionPropertyName.driverMemory.toString)).getOrElse("default")
    var numExecutors = Try(sourceRangeObj.executionProperties.get(executionPropertyName.executors.toString)).getOrElse("default")
    var executorCore = Try(sourceRangeObj.executionProperties.get(executionPropertyName.executorcores.toString)).getOrElse("default")
    var execMemory = Try(sourceRangeObj.executionProperties.get(executionPropertyName.executorMemory.toString)).getOrElse("default")
    var sparkConfigs = Try(sourceRangeObj.executionProperties.get(executionPropertyName.sparkConfigs.toString)).getOrElse("")
    var isPostProcessingEnabled: Boolean = Try(sourceRangeObj.executionProperties.get(executionPropertyName.isPostProcessingEnabled.toString).toBoolean).getOrElse(false)

    val granularity = sourceRangeObj.granularity
    val timezone = sourceRangeObj.timezone

    val props = new Properties()
    props.setProperty("process_memory", "-Xmx4g")
    props.setProperty("START_TIME", startTime)
    props.setProperty("END_TIME", endTime)
    props.setProperty("START_EPOCH_TIME", stime)
    props.setProperty("END_EPOCH_TIME", etime)
    props.setProperty("INPUT_AVAILABLE", checkInputAvailability(pathsList).toString)
    props.setProperty("JOB_START_EPOCH_TIME",instanceTime.getTime.toString)
    props.setProperty("VIEW_NAME", sourceRangeObj.viewName)
    props.setProperty("QUEUE_NAME", queueName)
    props.setProperty("num-executors", numExecutors)
    props.setProperty("executor-cores", executorCore)
    props.setProperty("driver-memory", driverMemory)
    props.setProperty("executor-memory", execMemory)
    props.setProperty("spark-configurations", sparkConfigs)
    props.setProperty("CLIENT_NAME", sourceRangeObj.clientName)
    props.setProperty("USER_NAME", userName)
    props.setProperty("IS_JOB_VALID", validateJobExpression(jobExecutionExpression, props).toString)
    props.setProperty("IS_POST_PROCESSING_ENABLED",isPostProcessingEnabled.toString)

    sourceRangeObj.granularity match{
      case Some(granularity) =>
        props.setProperty("GRANULARITY", granularity)
      case None =>
        logger.debug("granularity is null")
    }

    sourceRangeObj.timezone match{
      case Some(timezone) =>
        val sftpStartTime = DateUtils.fetchClientZoneTime(stime.toLong, timezone, Constants.SFTP_TIME_FORMAT)
        props.setProperty("SFTP_START_TIME", sftpStartTime)
      case None =>
        logger.info("client timezone is null")
    }

    sourceRangeObj.processorPlugInJarLoc match {
      case Some(jarLocation1:List[String]) =>
        props.setProperty("PROCESSOR_PLUGIN_JAR_LOC", jarLocation1.mkString(","))
      case None =>
        logger.debug("processor plugin jar location is null")
    }

    if(sourceRangeObj.exporterPluginJarLocList.nonEmpty)
      props.setProperty("EXPORTER_PLUGIN_JAR_LOC", ","+sourceRangeObj.exporterPluginJarLocList.mkString(","))

    sourceRangeObj.executionProperties match{
      case Some(execProps) =>
        populateExecutionProperties(props,execProps)
      case None =>
        logger.debug("execution properties is null")
    }
    props
  }

  protected def validateJobExpression(jobExecutionExpression:Option[String], props: Properties): Boolean = {

    if (jobExecutionExpression == None || jobExecutionExpression.get.isEmpty) {
      logger.info("jobExecutionExpression is not defined, skipping validation!")
      return true
    }

    val expression = jobExecutionExpression.get
      .replace("${VIEW_NAME}", props.getProperty("VIEW_NAME"))
      .replace("${CLIENT_NAME}", props.getProperty("CLIENT_NAME"))
      .replace("${USER_NAME}", props.getProperty("USER_NAME"))
      .replace("${QUEUE_NAME}", props.getProperty("QUEUE_NAME"))
      .replace("${START_EPOCH_TIME}", props.getProperty("START_EPOCH_TIME"))
      .replace("${END_EPOCH_TIME}", props.getProperty("END_EPOCH_TIME"))
      .replace("${JOB_START_EPOCH_TIME}", props.getProperty("JOB_START_EPOCH_TIME"))
    logger.info(s"job Execution Expression is : $expression")
    val sql = s"SELECT $expression"
    H2DBConnectionUtil.evaluateSqlExpression(sql)
}


  /**
    * enum class for valid values as execution properties key values
    */
  object executionPropertyName extends Enumeration {
    type executionPropertyName = Value

    val queue = Value("queue")
    val executors = Value("num-executors")
    val executorcores = Value("executor-cores")
    val driverMemory = Value("driver-memory")
    val executorMemory = Value("executor-memory")
    val isPostProcessingEnabled = Value("isPostProcessingEnabled")
    val sparkConfigs = Value("spark-configurations")
  }

  def populateExecutionProperties(props:Properties, executionProperties:Map[String,String]) : Unit =
  {
      executionPropertyName.values foreach (propName =>
        executionProperties.get(propName.toString) match {
          case Some(prop1) =>  props.setProperty(propName.toString,executionProperties.get(propName.toString).get)
          case None => ;
        }
        )

  }

  /**
   *
   * @param pathsList
   * @return
   */
  protected def checkInputAvailability(pathsList: List[String]): Boolean = {
    HadoopUtils.hadoopFileExists("_SUCCESS", pathsList);
  }

  /**
   * fetches the catalog source range details.
   * @return
   */
  def fetchCatalogSourceRangeInfo(): String = {
    var url = PropertyReader.getApplicationProperties().getProperty("catalog.source.range.url")
    url = url.replaceAll("VIEW", viewName)
    url = url.replaceAll("CLIENT", clientName)
    url = url.replaceAll("TIME", instance_time.getTime.toString)
    url = url.replaceAll("REPLAY", System.getProperty("replay.task", "false"))

    val sourceTimeRange = RestClient.sendGetRequest(url)
    logger.info("Catalog source range details: " + sourceTimeRange)
    return sourceTimeRange
  }

  def encodeValue(value: String): String = {
    return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
  }

}

object PreprocessingTask extends PreprocessingTask() {
  /**
   * The following params should be passed to this.
   * @param args viewName, clientName, instance_time, complexity
   */
  def main (args: Array[String]): Unit ={
    val wfId = System.getProperty("oozie.job.id")
    var props = new Properties()
      try {
      this.clientName = args(0)
      this.viewName = args(1)
      this.instance_time = DateUtils.utcStringToDate(Constants.OOZIE_NOMINAL_DATE_FORMAT, args(2))
      this.processComplexity = RESOURCE_DEGREE.withName(args(3))
      this.externalId = args(4)

      ThreadContext.put("client", clientName)
      ThreadContext.put("view", viewName)
      ThreadContext.put("corelatId", wfId)
      ThreadContext.put("service", "PreProcessingTask")

      //Initialize the properties in this new container
      if (propertiesDir == null) {
        logger.error(s"Unable to fetch namenode and/or properties location: $propertiesDir")
        System.exit(1)
      }
      PropertyReader.initialize(true, propertiesDir)

      props = getProcessingProperties(processComplexity, this.instance_time)
      OozieUtils.saveToOozieContext(props)

    } catch {
      case e: Exception => logger.error("Unexpected exception", e); System.exit(1)
    }

    OozieUtils.fetchOozieWFStatus(wfId) match {
      case Some(response) =>
        SlaRecordPublish.publish(wfId, response, JobMetadata(viewName, clientName,
        instance_time.getTime, props.getProperty("START_EPOCH_TIME", "0").toLong,
        props.getProperty("END_EPOCH_TIME", "0").toLong,externalId),RUNNING_STATUS)

      case None => logger.error(s"Unable to fetch oozie response for this workflowId: $propertiesDir")
    }
  }
}



case class CatalogResponse(val viewName: String, val clientName: String, val scheduleStartTime:String,
                           val scheduleEndTime:String, val sourceTimeRangeList:List[SourceTimeRange],
                           val executionProperties:Option[Map[String,String]], val processorPlugInJarLoc: Option[List[String]],
                           val exporterPluginJarLocList:Seq[String],
                           val jobExecutionExpression:Option[String],val granularity: Option[String],val timezone: Option[String])


case class SourceTimeRange(val hdfsSourceViewID: String, val startTime: String, val endTime: String,
                           val pathList: List[String])
