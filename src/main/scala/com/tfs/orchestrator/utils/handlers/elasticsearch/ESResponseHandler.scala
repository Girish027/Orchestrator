package com.tfs.orchestrator.utils.handlers.elasticsearch

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.{DefaultFormats, parse}

import scala.collection.mutable.ListBuffer

class ESResponseHandler {

  def retrieveJobIds(response: String): List[String] = {
    val responseObject: JobIDsResponse = parseToResponseObject(response)
    retrieveWorkflowIds(responseObject)
  }

  private def parseToResponseObject(response: String): JobIDsResponse = {
    val json = parse(response)
    implicit val formats: DefaultFormats.type = DefaultFormats
    json.extract[JobIDsResponse]
  }

  private def retrieveWorkflowIds(responseObject: JobIDsResponse): List[String] = {
    var wfIds = ListBuffer[String]()
    responseObject.hits.hits.foreach(hit => {
      wfIds += hit._id
    })
    wfIds.toList
  }

  def getESScroll(json: JValue) = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    json.extract[JobMetrics]._scroll_id
  }

  def getESTotalHits(json: JValue) = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    json.extract[JobMetrics].hits.total
  }

  def retrieveJobsForLateArrivedEvents(metrics:List[MetricsWrapper]): Seq[FinalMetric] = {
    val metricsList = new ListBuffer[FinalMetric]
    metrics.foreach{ x =>
      val metric :FinalMetric = new FinalMetric(x._id,
      x._source.processingExecution_time.getOrElse("0"),
        x._source.clientName_keyword,
        x._source.viewName_keyword,
        x._source.nominal_time.getOrElse("0"),
        x._source.dataPaths.getOrElse(List[String]()))
      metricsList += metric
    }
    metricsList.seq
  }


  def retrieveTotalEventCount(outputJson: String): Long = {
    val json = parse(outputJson)
    implicit val formats: DefaultFormats.type = DefaultFormats
    json.extract[HingeMetricCount].aggregations.total_event.value.toLong
  }


  case class Hit(
                  _id: String
                  )

  case class Hits(
                   total: Double,
                   hits: List[Hit]
                   )

  case class JobIDsResponse(
                             hits: Hits
                             )

}
  case class JobMetrics(_scroll_id:String,took:Integer,timed_out:Boolean,_shards:Shards,hits:Hits2 )
  case class HingeMetricCount(took:Integer,timed_out:Boolean,_shards:Shards,hits:Hits3,aggregations:Eventcount )
  case class Eventcount(total_event:CountValue)
  case class CountValue(value:Float)
  case class Shards(total:Integer,successful:Integer,skipped:Integer,failed:Integer)
  case class Hits2(total:Integer,max_score:Option[Float],hits:List[MetricsWrapper] )
  case class Hits3(total:Integer,max_score:Option[Float],hits:List[HingeMetricsWrapper] )
  case class HingeMetricsWrapper(_index:String,_type:String,_id:String,_score:Float,_source:HingeMetrics)
  case class HingeMetrics(event_count:Integer)
  case class MetricsWrapper(_index:String,_type:String,_id:String,_score:Float,_source:Metrics)
  case class Metrics(processingExecution_time:Option[String],clientName_keyword:String,viewName_keyword:String,nominal_time:Option[String],dataPaths:Option[Seq[String]])
  case class FinalMetric(id:String,processingExecution_time:String,clientName_keyword:String,viewName_keyword:String,nominal_time:String,dataPaths:Seq[String])


