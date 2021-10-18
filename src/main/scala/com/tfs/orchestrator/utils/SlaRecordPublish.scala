package com.tfs.orchestrator.utils

import java.text.SimpleDateFormat
import java.util.Date
import com.tfs.orchestrator.exceptions.RestClientException
import com.tfs.orchestrator.utils.kafka.KafkaWriter
import com.tfs.orchestrator.utils.kafka.KafkaWriter.{PlatformStats, PlatformDetailsStats}
import net.liftweb.json._
import org.apache.logging.log4j.scala.Logging

object SlaRecordPublish extends Logging {

  implicit val formats = DefaultFormats
  val propertiesDir = System.getProperty("properties.hdfs.dir")

  /**
   * Takes the Oozie API response for the given workflow, converts it into SlaDashboard friendly versio
   * and ingests into the ES against the given unique id.
   * @param recordId
   * @param oozieResponseStr
   * @param jobMetadata
   */
  def publish(recordId: String, oozieResponseStr: String, jobMetadata: JobMetadata, status:String): Unit = {

    val dqStatus = fetchDQStatus(recordId)
    val kafkaWriter = KafkaWriter
    val topicName = getTopicName
    val stats = getJobMetrics(recordId, oozieResponseStr, jobMetadata, status, dqStatus)
    val key = recordId
    kafkaWriter.pushMetrics(topicName, key, stats)

  }

  /**
   *
   * @param wfId
   * @param oozieResponseStr
   * @param jobMetadata
   * @param status
   * @param dqStatus
   * @return
   */
  def getJobMetrics(wfId: String, oozieResponseStr: String, jobMetadata: JobMetadata, status:String, dqStatus:String) : scala.Predef.String = {

    val slaRecordPublish = SlaRecordPublish
    val oozieResponse = parse(oozieResponseStr).asInstanceOf[JObject]
    val actualStart_time = getUTCToEpochTime(compactRender(oozieResponse \ "startTime").replace("\"", ""))
    val actualEnd_time = getUTCToEpochTime(fetchEndTime(status))
    val duration = fetchDuration(actualStart_time, actualEnd_time, status)
    val actions = slaRecordPublish.constructOozieActionStats((oozieResponse \ "actions").asInstanceOf[JArray])
    val retry_count = compactRender(oozieResponse \ "run").toInt
    val id = wfId
    val eventSource = Constants.EVENT_SOURCE
    val eventTime= System.currentTimeMillis()

    implicit val formats = net.liftweb.json.DefaultFormats
    compactRender(Extraction.decompose(PlatformDetailsStats(id, eventSource, eventTime, PlatformStats(jobMetadata.viewName, jobMetadata.clientName, jobMetadata.scheduledTime,
      jobMetadata.dataStartTime, jobMetadata.dataEndTime, actualStart_time, actualEnd_time, duration, actions, retry_count, status, dqStatus,jobMetadata.externalId))))
  }

  /**
   *
   * @param utcTime
   * @return
   */
  def getUTCToEpochTime(utcTime : String) : Long = {
    var epoch: Long = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz").parse(utcTime).getTime
    epoch
  }

  /**
   *
   * @param actualStart_time
   * @param actualEnd_time
   * @param status
   * @return
   */
  def fetchDuration(actualStart_time: Long, actualEnd_time: Long, status: String): Long = {
    if (status.equals("RUNNING")) {
      return (System.currentTimeMillis() - actualStart_time)
    }
    (actualEnd_time - actualStart_time)
  }

  /**
   *
   * @return
   */
  def getTopicName : String = {
    PropertyReader.getApplicationProperties().getProperty("dp2.metrics.topic")
  }

  /**
   * Parses the Oozie response into a OozieWorkflowStats object that is compliant for dumping the stats
   * to the ElasticSearch for SLA dashboard consumption.
   * @param oozieResponseStr
   */
  def constructOozieWFStats(oozieResponseStr: String, jobMetadata: JobMetadata, dqStatus:String, status:String): OozieWorkflowStats = {
    val oozieResponse = parse(oozieResponseStr).asInstanceOf[JObject]
    val actualStartTime = compactRender(oozieResponse \ "startTime").replace("\"", "")
    val actualEndTime = fetchEndTime(status)
    val oozieWfId = compactRender(oozieResponse \ "id").replace("\"", "")
    val actions = constructOozieActionStats((oozieResponse \ "actions").asInstanceOf[JArray])
    val retryCount = compactRender(oozieResponse \ "run").toInt
    val workflowStatus = status
    return OozieWorkflowStats(jobMetadata.viewName, jobMetadata.clientName, jobMetadata.scheduledTime, actualStartTime,actualEndTime,      jobMetadata.dataStartTime, jobMetadata.dataEndTime, oozieWfId, retryCount, workflowStatus, actions, dqStatus)
  }

  /**
   *
   * @param oozieResponse
   * @return
   */
  private def fetchEndTime(oozieResponse: JObject):String = {
    val endTime = compactRender(oozieResponse \ "endTime").replace("\"", "")
    if(endTime.equals("null")){
      return "Fri, 31 Dec 3000 23:59:59 GMT"
    }
    endTime
  }

  /**
   *
   * @param status
   * @return
   */
  private def fetchEndTime(status: String): String = {
    if (status.equals("SUCCEEDED") || status.equals("FAILED")) {
      return DateUtils.dateToUTCString(Constants.OOZIE_RESPONSE_TIME_FORMAT, new Date())
    }
    "Fri, 31 Dec 3000 23:59:59 GMT"
  }

  /**
   * Parses the actions information from oozie response into an array of
    *
    * @param oozieResponseJson
   * @return
   */
  def constructOozieActionStats(oozieResponseJson: JArray): List[OozieActionStats] = {
    var counter = 0
    val actionsMap = oozieResponseJson.arr.map((res: JValue) =>
      compactRender(res.asInstanceOf[JObject] \ "name") -> res.asInstanceOf[JObject]).toMap

    //Get the start action info.
    var oozieResponseActionInfo = actionsMap.getOrElse("\":start:\"", null)
    val oozieActionStats = collection.mutable.MutableList[OozieActionStats]()

    while (oozieResponseActionInfo != null) {
      val workflowStatus = compactRender(oozieResponseActionInfo \ "status") match {
        case status if status matches "(?i)\"OK\"" => "SUCCESS"
        case status if status matches "(?i)\"RUNNING\""=> "RUNNING"
        case _ => "FAIL"
      }

      //Let's skip the adding the task details, if the task is in progress.
      if (!workflowStatus.equals("RUNNING")) {
        val actionName = compactRender(oozieResponseActionInfo \ "name").replace("\"", "")
        val startTime = compactRender(oozieResponseActionInfo \ "startTime").replace("\"", "")
        val endTime = fetchEndTime(oozieResponseActionInfo)
        val consoleUrl = compactRender(oozieResponseActionInfo \ "consoleUrl").replace("\"", "")
        oozieActionStats.+=(OozieActionStats(counter, actionName, startTime, endTime, workflowStatus, consoleUrl))
      }
      //Go the next task in execution order.
      val transitionTask = compactRender(oozieResponseActionInfo \ "transition")
      oozieResponseActionInfo = actionsMap.getOrElse(transitionTask, null)
      counter = counter + 1
    }

    return oozieActionStats.toList
  }

  /**
    * Get the DQ status URL template from the properties file located in hadoop.
    * @return
    */
  def fetchDQStatus(wfId: String): String = {
    var dqStatus = "NA"
    val statusUrlTemplate = PropertyReader.getApplicationProperties().getProperty("dq.es.status.url")

    val statusUrl = statusUrlTemplate.replace("WFID", wfId)
    logger.info("Status URL : "+statusUrl)
    val response: Option[String] =
      try {
        Some(RestClient.sendGetRequest(statusUrl))
      } catch {
        case ex: RestClientException => {
          logger.error("Not able to retrieve response from ES.", ex)
          None
        }
      }
    if (response.isDefined) {
      dqStatus = fetchDQStatusFromResponseJSON(response.get)
    }
    return dqStatus
  }

  /**
    * Parse ES response and return DQ Status
    * If no document is found then return NA otherwise actual value in ES.
    * @return
    */
  def fetchDQStatusFromResponseJSON(responseString:String):String = {
    val respObj = parse(responseString).asInstanceOf[JObject]

    val found = compactRender(respObj \ "found").toBoolean
    var dqStatus = "NA"
    if(found){
      val status = compactRender(respObj \ "_source" \ "DQStatus")
      if(!status.isEmpty){
        dqStatus = status
      }
    }
    dqStatus.replace("\"", "")
  }

  /**
   * REST endpoint for our index name.
   * @return
   */
  def getESIndexUrl(): String = {
    val oozieProps = PropertyReader.getApplicationProperties()
    var ingestUrl = oozieProps.getProperty("sla.es.ingest.url")
    ingestUrl = ingestUrl + "/" + oozieProps.getProperty("sla.es.indexName")
    return ingestUrl
  }

  /**
   * REST endpoint for the dataType in our index name.
   * @return
   */
  def getESDataTypeUrl(): String = {
    val appProps = PropertyReader.getApplicationProperties()
    var ingestUrl = appProps.getProperty("sla.es.ingest.url")
    val indexName = appProps.getProperty("sla.es.indexName")
    val dataType = appProps.getProperty("sla.es.dataType")
    ingestUrl = ingestUrl + "/" + indexName + "/" + "_mapping" + "/" + dataType
    return ingestUrl
  }

  /**
   * REST endpoint for the ingesting the data records.
   * @return
   */
  def getESIngestionUrl(uniqueId: Option[String]): String = {
    val appProps = PropertyReader.getApplicationProperties()
    var ingestUrl = appProps.getProperty("sla.es.ingest.url")
    val indexName = appProps.getProperty("sla.es.indexName")
    val dataType = appProps.getProperty("sla.es.dataType")

    ingestUrl = uniqueId match {
      case Some(id) => s"$ingestUrl/$indexName/$dataType/$id"
      case None => s"$ingestUrl/$indexName/$dataType"
    }
    return ingestUrl
  }
  case class OozieWorkflowStats(var viewName: String, clientName:String, jobScheduleTime:Long, actualStartTime:String,
                                actualEndTime:String,dataStartTime: Long, dataEndTime:Long, oozieWorkflowId: String, retryCount:Int, workflowStatus:String,
                                actions: List[OozieActionStats], dqStatus:String)
  case class OozieActionStats(action: Int, actionName: String, startTime: String, endTime: String, status: String,
                              yarnStatusUrl: String)

  case class JobMetadata(viewName: String, clientName:String, scheduledTime: Long, dataStartTime: Long, dataEndTime: Long, externalId: String ="")
}
