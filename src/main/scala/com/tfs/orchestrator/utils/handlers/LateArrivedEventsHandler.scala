package com.tfs.orchestrator.utils.handlers

import java.io.{File, FileReader}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.tfs.orchestrator.utils.handlers.elasticsearch._
import com.tfs.orchestrator.utils.{Constants, PropertyReader, Utils}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.http.HttpStatus
import org.apache.logging.log4j.scala.Logging
/**
 * Created by Priyanka.N on 29-07-2019.
 */
class LateArrivedEventsHandler extends Logging{

  var esRequestHandler = new ESRequestHandler
  var esResponseHandler = new ESResponseHandler
  var jobMetricsMap = scala.collection.mutable.Map[String,EventCounts]()
  case class EventCounts(executionTime:String,eventCountBefore:Long,eventCountAfter:Long)


  /**
   * reduce the jobs list to filter out any reruns/replays with different workflow id : only the latest run is preserved
   * @param jobMetricsList
   * @return
   */
  def getLatestJobRuns(jobMetricsList:Seq[FinalMetric])={
    logger.info("original jobs list is "+jobMetricsList.size)
    var mapWithUniqueJobruns = scala.collection.mutable.Map[String,FinalMetric]()
    for(metric <- jobMetricsList)
      {
        val key = metric.viewName_keyword+":"+metric.clientName_keyword+":"+metric.nominal_time
        val value = metric
        if(mapWithUniqueJobruns.contains(key)){
          val existingValue = mapWithUniqueJobruns.get(key).get
          logger.debug("found duplicate entries for key "+key+" : "+value.processingExecution_time+" and "+existingValue.processingExecution_time)
          if(parseLong(value.processingExecution_time).get > parseLong(existingValue.processingExecution_time).get)
            {
              mapWithUniqueJobruns += (key -> value)
            }
        }
       else{
          mapWithUniqueJobruns += (key -> value)
        }
      }
    logger.info("jobs list after removal of replays is "+mapWithUniqueJobruns.values.size)
    mapWithUniqueJobruns.values.toList
  }

  private def parseLong(s: String) = try { Some(s.toLong) } catch { case _ => None }

  def getMapWithRoundedTime(jobMetricsList:Seq[FinalMetric])={
    var bucketMap = Map[Long,Map[String,Seq[String]]]()
    for(metric <- jobMetricsList)
    {
      val next = roundOffProcessingTimeToLatestQuarterHour(metric.processingExecution_time)
      if(!bucketMap.contains(next))
      {
        bucketMap += (next -> Map[String,Seq[String]](metric.id -> metric.dataPaths))
      }
      else
      {
        var existingMap = bucketMap.get(next).get
        existingMap += (metric.id -> metric.dataPaths)
      }
    }
    bucketMap
  }

  /**
   * @param processingExecution_time
   * @return rounded off time to previous 15th minute.
   */
  private def roundOffProcessingTimeToLatestQuarterHour(processingExecution_time:String)= {
    var time = parseLong(processingExecution_time).get / 1000
    time  = time - (time % 900);
    time*1000
  }

  def getThresholdDifference(index:String,processingTime:Long,currentTime:Long,path:String): Float ={
    var esQuery = fetchJobMetricsFilterSpec(Constants.HINGE_METRICS_QUERY_FILE)
    esQuery = esQuery.replace("$DATA_PATH", path)
    var eventCountBefore:Long = 0
    var eventCountAfter:Long = 0
    var isReplayRequired = false
    val esQueryForHingeTillProcessingTime = esQuery.replace("$EXECUTION_TIME",processingTime.toString)
    val esQueryForHingeTillCurrentTime = esQuery.replace("$EXECUTION_TIME",currentTime.toString)

    var esResponse = esRequestHandler.fetchJobDetailsFromES(index+"/_search", esQueryForHingeTillProcessingTime)
    if (esResponse.code == HttpStatus.SC_OK) {
      logger.debug("es results from hinge metrics for path "+path+" total count before : " + esResponse.body)
      eventCountBefore = esResponseHandler.retrieveTotalEventCount(esResponse.body)
    }
    else
      {
        logger.info("ES request for hinge, with path "+path+" has returned unsuccessful code "+esResponse.code+" body: "+esResponse.body)
      }
    esResponse = esRequestHandler.fetchJobDetailsFromES(index+"/_search", esQueryForHingeTillCurrentTime)
    if (esResponse.code == HttpStatus.SC_OK) {
      logger.debug("es results from hinge metrics for path"+path+" total count after : " + esResponse.body)
      eventCountAfter = esResponseHandler.retrieveTotalEventCount(esResponse.body)
    }
    else
    {
      logger.info("ES request for hinge, with path "+path+" has returned unsuccessful code "+esResponse.code+" body: "+esResponse.body)
    }
    getDifferenceInPercentage(eventCountBefore,eventCountAfter)
  }

  def getDifferenceInPercentage(eventCountBefore:Float,eventCountAfter:Float): Float ={
    (((eventCountAfter-eventCountBefore)/eventCountBefore) * 100)
  }

  /**
   * @return list of job metrics since window duration specified , for example 7 days
   */
  def replayJobsForLateEvents:Unit = {
    logger.info("Starting replay job")
    var esQuery = fetchJobMetricsFilterSpec(Constants.JOB_METRICS_QUERY_FILE)
    esQuery = esQuery.replace("$WINDOW_DURATION",getFilterDate())
    val queryParam = "scroll=1m"
    val scrollQueryParam = "_search/scroll"
    var results = esRequestHandler.fetchJobDetailsFromES(Constants.JOB_INDEX,queryParam,esQuery)
    if (results.code == HttpStatus.SC_OK) {
      var json = parse(results.body)
      var scrollId = esResponseHandler.getESScroll(json)
      implicit val formats: DefaultFormats.type = DefaultFormats
      var hitsList = json.extract[JobMetrics].hits.hits
      while(hitsList != null && hitsList.length > 0)
        {
          val scrolledBatchesQuery = "{\n    \"scroll\" : \"1m\", \n    \"scroll_id\" : \""+scrollId+"\" \n}"
          triggerRetry(esResponseHandler.retrieveJobsForLateArrivedEvents(hitsList))
          val scrollResponse  = esRequestHandler.fetchJobDetailsFromESWithoutIndex(scrollQueryParam,scrolledBatchesQuery)
          val scrollResponseJson = parse(scrollResponse.body)
          scrollId = esResponseHandler.getESScroll(scrollResponseJson)
          hitsList = scrollResponseJson.extract[JobMetrics].hits.hits
        }
      results = esRequestHandler.deleteScrollId(scrollQueryParam + "/" +scrollId)
      if(results.code != HttpStatus.SC_OK)
        {
          logger.info("Delete scroll id in ES failed : "+results.body)
        }
    }
    else {
      logger.info("Es results to fetch DP2 jobs returned unsuccessful code"+results.code+" body: "+results.body)
    }
  }

  def triggerRetry(metricsList:Seq[FinalMetric]): Unit =
  {
    val reducedJobMetricsList = getLatestJobRuns(metricsList)
    logger.info("obtained unique job runs count : "+reducedJobMetricsList.size)
    checkAndTriggerRetry(getMapWithRoundedTime(reducedJobMetricsList))
  }
  private def fetchJobMetricsFilterSpec(queryFile:String): String = {
    val file = new File(System.getProperty(Constants.COMMAND_PROPS_DIR_KEY) +
      System.getProperty("file.separator") + queryFile)
    Utils.readInputReader(new FileReader(file))
  }

  /**
   * @return FROM date filter to query ES in format YYYY-MM-dd , 2019-08-01 : default window period is is 7 days
   */
  def getFilterDate(): String =
  {
    val windowInDays = PropertyReader.getApplicationPropertiesValue(Constants.AUTO_REPLAY_WINDOW_PROPERTY,Constants.AUTO_REPLAY_WINDOW_DAYS_DEFAULT)
    val df = new SimpleDateFormat("YYYY-MM-dd")
    var pastDate = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(windowInDays, TimeUnit.DAYS))
    df.format(pastDate)
  }

  def checkAndTriggerRetry(map:Map[Long,Map[String,Seq[String]]])={
    val currentTime = System.currentTimeMillis();
    val thresholdPercentage:Float  = PropertyReader.getApplicationPropertiesValue(Constants.AUTO_REPLAY_THRESHOLD_PROPERTY,Constants.AUTO_REPLAY_THRESHOLD_PERCENT_DEFAULT)
    logger.debug("defined threshold is : "+thresholdPercentage)
    for((key,value) <- map)
    {
      var retryableJobsList = scala.collection.mutable.Set[String]()
      for((jobId,pathList)<-value)
      {
        var lateEventsInPercentage:Float=0
        for (path <- pathList)
          {
            var replacedPath:String = ""
            if(path.contains("///")) {
              val index = path.indexOf("///")
              replacedPath = "hdfs:"+path.substring(index+2)
            }
            val index = fetchIndexForHingeMetrics(path)
            lateEventsInPercentage = lateEventsInPercentage  + getThresholdDifference(index,key,currentTime,replacedPath)
            if(lateEventsInPercentage >= thresholdPercentage)
            {
              logger.info("Late events arrival : Triggering a retry on jobID : "+jobId)
              retryableJobsList = retryableJobsList + jobId
            }
          }
      }
      if(retryableJobsList.size >0){
        logger.info("submitting jobs to retry : "+retryableJobsList.size)
        invokeRetry(retryableJobsList)
      }
    }
  }

  protected def invokeRetry(jobList:scala.collection.mutable.Set[String]): Unit =
  {
    RetryableJobHandler.retryJobs(jobList.toList)
  }

  /**
   * maps input paths with hinge job indices in ES
   * @param path
   * @return index name of corresponding event's hinge job
   */
  private def fetchIndexForHingeMetrics(path:String): String =
  {
    val index =
    if(path.contains("/pxassistv2"))
     PropertyReader.getApplicationPropertiesValue("pxassistv2",ESIndex_HingeMetrics.pxassistv2)
    else if(path.contains("/pxassist"))
      PropertyReader.getApplicationPropertiesValue("pxassist",ESIndex_HingeMetrics.pxassist)
    else if(path.contains("/rtdp"))
      PropertyReader.getApplicationPropertiesValue("rtdp",ESIndex_HingeMetrics.rtdp)
    else if(path.contains("/sessionized_binlog"))
      PropertyReader.getApplicationPropertiesValue("sessionized_binlog",ESIndex_HingeMetrics.sessionized_binlog)
    else if(path.contains("/rawbinlog"))
      PropertyReader.getApplicationPropertiesValue("rawbinlog",ESIndex_HingeMetrics.rawbinlog)
    else if(path.contains("/simod"))
      PropertyReader.getApplicationPropertiesValue("simod",ESIndex_HingeMetrics.simod)
    else if(path.contains("/pxoe_merged"))
      PropertyReader.getApplicationPropertiesValue("pxoe_merged",ESIndex_HingeMetrics.pxoe_merged)
    else if(path.contains("/pxoe"))
      PropertyReader.getApplicationPropertiesValue("pxoe",ESIndex_HingeMetrics.pxoe)
    else if(path.contains("/afv"))
      PropertyReader.getApplicationPropertiesValue("afv",ESIndex_HingeMetrics.afv)
    else if(path.contains("/ivr2chat"))
      PropertyReader.getApplicationPropertiesValue("ivr2chat",ESIndex_HingeMetrics.IVR2chatTicketing)

    index.toString
  }
    object ESIndex_HingeMetrics extends Enumeration {
    type ESIndex_HingeMetrics = Value
    val pxassist = Value("pxassist_event_dq_*")
    val rawbinlog = Value("raw_binlog_event_dq_*")
    val rtdp = Value("rtdp_idm_event_dq_*")
    val sessionized_binlog = Value("sessionized_binlog_event_dq_*")
    val simod = Value("simod_feeds_event_dq_*")
    val pxoe = Value("pxoe_event_dq_*")
    val pxoe_merged = Value("pxoe_merged_event_dq_*")
    val pxassistv2 = Value("pxassistv2_event_dq_*")
    val IVR2chatTicketing = Value("ivr2chat_ticketing_event_dq_*")
    val afv = Value("afv_event_dq_*")
  }

}
