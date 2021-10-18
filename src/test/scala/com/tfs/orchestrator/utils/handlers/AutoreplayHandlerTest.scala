package com.tfs.orchestrator.utils.handlers

import com.tfs.orchestrator.utils.handlers.elasticsearch.{ESRequestHandler, FinalMetric}
import com.tfs.orchestrator.utils.{Constants, PropertyReader}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

import scalaj.http.HttpResponse


/**
 * Created by Priyanka.N on 13/08/2019.
 */
class AutoreplayHandlerTest extends FlatSpec with MockFactory{

val lateArrivedEventsHandler = new LateArrivedEventsHandler

  it should "filter unique jobs from jobs list" in {
    //id:String,processingExecution_time:String,clientName_keyword:String,viewName_keyword:String,nominal_time:String,dataPaths:Seq[String]
    val metric1 = (new FinalMetric("id1","1566126100000","client1","view1","1566126000000",Seq[String]("path1","path2")))
    val metric2 = (new FinalMetric("id2","1566126990000","client2","view1","1566126990000",Seq[String]("path3","path2")))
    val metric3 = (new FinalMetric("id3","1566129900000","client1","view1","1566126000000",Seq[String]("path1","path2")))
    val metric4 = new FinalMetric("0005026-190802014502989-oozie-oozi-W","1566713977144","client1","Digital_Test30","1565776800000", List[String]("hdfs://stable01///raw/sv2/staging/rtdp/idm/events/non-prod/hilton/year=2019/month=08/day=14/hour=10/min=00"))
    val metric5 = new FinalMetric("0005700-190802014502989-oozie-oozi-W","1566966569639","client1","Digital_Test30","1565776800000",
        List("hdfs://stable01///raw/sv2/staging/rtdp/idm/events/non-prod/hilton/year=2019/month=08/day=14/hour=10/min=00"))

    var jobMetricsList = List[FinalMetric](metric1,metric2,metric3,metric4,metric5)
    var actualFilteredList = lateArrivedEventsHandler.getLatestJobRuns(jobMetricsList)
    var expectedList = List[FinalMetric](metric2,metric3,metric5)
    assert(actualFilteredList.size == expectedList.size,"actual and expected filtered list sizes are different")
    assert(!actualFilteredList.contains(metric1),"job filtering for unique view-client-nominaltime is not faling")
  }


  it should "round off the processing time and group the entries" in {

    var jobMetricsList = scala.collection.mutable.Seq[FinalMetric]()
    //id:String,processingExecution_time:String,clientName_keyword:String,viewName_keyword:String,nominal_time:String,dataPaths:Seq[String]
    jobMetricsList = jobMetricsList :+ new FinalMetric("id1","1566126100000","client1","view1","1566126000000",Seq[String]("path1","path2"))
    jobMetricsList = jobMetricsList :+ new FinalMetric("id2","1566126990000","client2","view1","1566126990000",Seq[String]("path3","path2"))
    jobMetricsList = jobMetricsList :+ new FinalMetric("id3","1566129900000","client1","view1","1566126000000",Seq[String]("path1","path2"))
    val actualMap = lateArrivedEventsHandler.getMapWithRoundedTime(jobMetricsList)
    val wf1 = Map[String,Seq[String]]("wf-001" -> Seq[String]("path1","path2"))
    val wf2 = Map[String,Seq[String]]("wf-002" -> Seq[String]("path3"))
    val wf3 = Map[String,Seq[String]]("wf-003" -> Seq[String]("pathx"))
    //rounded off to latest 15th minute of the corresponding hour
    val expectedMap = Map[Long,Map[String,Seq[String]]](1566126000000l->wf1,1566126900000l-> wf2 , 1566129600000l -> wf3)
    assert(actualMap.keySet.equals(expectedMap.keySet),"rounding off to nearest hour is failing2")
  }

  it should "fetch the difference in percentage" in {
    val actual =  lateArrivedEventsHandler.getDifferenceInPercentage(100,120)
    assert (actual == 20.00,"percentage calaculation failed")
  }

  it should "get job metrics from ES" in {
    val mockESrequest = mock[ESRequestHandler]
    val jobResults = "{\"_scroll_id\":\"DnF1ZXJ5VGhlbkZldGNoBgAAAAAAeP9zFjBkamZWT3BsUlkydjFMSm1QTV9rdlEAAAAAAHj_bxYwZGpmVk9wbFJZMnYxTEptUE1fa3ZRAAAAAAB4_3AWMGRqZlZPcGxSWTJ2MUxKbVBNX2t2UQAAAAAAeP9yFjBkamZWT3BsUlkydjFMSm1QTV9rdlEAAAAAAHj_cRYwZGpmVk9wbFJZMnYxTEptUE1fa3ZRAAAAAAB4_3QWMGRqZlZPcGxSWTJ2MUxKbVBNX2t2UQ==\",\"took\":15,\"timed_out\":false,\"_shards\":" +
      "{\"total\":2,\"successful\":6,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":2,\"max_score\":0,\"hits\":[{\"_index\":\"dp2-jobmetrics_aug-2019\",\"_type\":\"dp2-jobmetrics_aug-2019\",\"_id\":\"0005026-190802014502989-oozie-oozi-W\",\"_score\":0,\"_source\":{\"processingExecution_time\":\"1566713977144\",\"clientName_keyword\":\"hilton\",\"viewName_keyword\":\"Digital_Test30\",\"nominal_time\":1565776800000,\"dataPaths\":[\"hdfs:\\/\\/stable01\\/\\/\\/raw\\/sv2\\/staging\\/rtdp\\/idm\\/events\\/non-prod\\/hilton\\/year=2019\\/month=08\\/day=14\\/hour=10\\/min=00\"]}},{\"_index\":\"dp2-jobmetrics_aug-2019\",\"_type\":\"dp2-jobmetrics_aug-2019\",\"_id\":\"0005697-190802014502989-oozie-oozi-W\",\"_score\":0,\"_source\":{\"processingExecution_time\":\"1566965076484\",\"clientName_keyword\":\"hilton\",\"viewName_keyword\":\"Digital_Test30\",\"nominal_time\":1565780400000,\"dataPaths\":[\"hdfs:\\/\\/stable01\\/\\/\\/raw\\/sv2\\/staging\\/rtdp\\/idm\\/events\\/non-prod\\/hilton\\/year=2019\\/month=08\\/day=14\\/hour=11\\/min=00\"]}}]}}"
    val jobResultsEmpty = "{\"_scroll_id\":\"DnF1ZXJ5VGhlbkZldGNoBgAAAAAAeP9zFjBkamZWT3BsUlkydjFMSm1QTV9rdlEAAAAAAHj_bxYwZGpmVk9wbFJZMnYxTEptUE1fa3ZRAAAAAAB4_3AWMGRqZlZPcGxSWTJ2MUxKbVBNX2t2UQAAAAAAeP9yFjBkamZWT3BsUlkydjFMSm1QTV9rdlEAAAAAAHj_cRYwZGpmVk9wbFJZMnYxTEptUE1fa3ZRAAAAAAB4_3QWMGRqZlZPcGxSWTJ2MUxKbVBNX2t2UQ==\",\"took\":15,\"timed_out\":false,\"_shards\":" +
      "{\"total\":2,\"successful\":6,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":0,\"max_score\":0,\"hits\":[]}}"

    val nextBatchResults =""
    (mockESrequest.deleteScrollId _).expects(*).returns(HttpResponse("body text", 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]()))
    (mockESrequest.fetchJobDetailsFromES(_:String,_:String,_:String)).expects(Constants.JOB_INDEX,"scroll=1m",*).returns(HttpResponse(jobResults, 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]())).anyNumberOfTimes()
    var i=0
    for(i <- 0 to 1)
    { if(i==1)
      (mockESrequest.fetchJobDetailsFromESWithoutIndex(_:String,_:String)).expects(*,*).returns(HttpResponse(jobResultsEmpty, 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]()))
      else
      (mockESrequest.fetchJobDetailsFromESWithoutIndex(_:String,_:String)).expects(*,*).returns(HttpResponse(jobResults, 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]()))
    }
    System.setProperty(Constants.COMMAND_PROPS_DIR_KEY,"src/main/resources/properties")
    PropertyReader.initialize(false, "src/main/resources/properties")
    val lateArrivedEventsHandlerMock = new LateArrivedEventsHandler {
      var triggerRetryInvoked = false
      override def triggerRetry(abc : Seq[FinalMetric]): Unit =
      {
        triggerRetryInvoked = true
      }
    }
    lateArrivedEventsHandlerMock.esRequestHandler = mockESrequest
    lateArrivedEventsHandlerMock.replayJobsForLateEvents
    assert(lateArrivedEventsHandlerMock.triggerRetryInvoked == true,"Retry of jobs methid was not called")
  }

  it should "trigger a retry for applicable jobs" in {
    PropertyReader.initialize(false, "src/main/resources/properties")

    //val processingTime = DateUtils.epochToUTCString("YYYY-MM-dd",1566965700000l)
    val eventCountrtdp = "{\"took\":38,\"timed_out\":false,\"_shards\":{\"total\":105,\"successful\":105,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":290,\"max_score\":11.987485,\"hits\":[{\"_index\":\"rtdp_idm_event_dq_aug-2019\",\"_type\":\"rtdp_idm_event_dq_aug-2019\",\"_id\":\"job_1566018324253_37070:attempt_1566018324253_37070_m_000007_0:hilton:1565780040000:sv2\",\"_score\":11.987485,\"_source\":{\"event_count\":1}}]},\"aggregations\":{\"total_event\":{\"value\":384}}}"
    val eventCountrtdpAfter = "{\"took\":38,\"timed_out\":false,\"_shards\":{\"total\":105,\"successful\":105,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":290,\"max_score\":11.987485,\"hits\":[{\"_index\":\"rtdp_idm_event_dq_aug-2019\",\"_type\":\"rtdp_idm_event_dq_aug-2019\",\"_id\":\"job_1566018324253_37070:attempt_1566018324253_37070_m_000007_0:hilton:1565780040000:sv2\",\"_score\":11.987485,\"_source\":{\"event_count\":1}}]},\"aggregations\":{\"total_event\":{\"value\":500}}}"
    val eventCountpxassist = "{\"took\":38,\"timed_out\":false,\"_shards\":{\"total\":105,\"successful\":105,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":290,\"max_score\":11.987485,\"hits\":[{\"_index\":\"rtdp_idm_event_dq_aug-2019\",\"_type\":\"rtdp_idm_event_dq_aug-2019\",\"_id\":\"job_1566018324253_37070:attempt_1566018324253_37070_m_000007_0:hilton:1565780040000:sv2\",\"_score\":11.987485,\"_source\":{\"event_count\":1}}]},\"aggregations\":{\"total_event\":{\"value\":55}}}"
    val mockESrequest = mock[ESRequestHandler]
    val before = "{  \"_source\": [ \"event_count\" ],  \"query\":  {    \"bool\":{      \"must\":[        {          \"range\" : {            \"ingestion_time\" : {              \"lte\" : \"1566965700000\"            }          }        },        {          \"term\": {            \"outputPath_keyword\": {              \"value\": \"hdfs:/raw/sv2/staging/rtdp/idm/events/non-prod/hilton/year=2019/month=08/day=14/hour=10/min=00\"            }          }        }      ]    }  },  \"aggs\" : {    \"total_event\" : { \"sum\" : { \"field\" : \"event_count\" } }  }}"
    //val after = "{  \"_source\": [ \"event_count\" ],  \"query\":  {    \"bool\":{      \"must\":[        {          \"range\" : {            \"ingestion_time\" : {              \"lte\" : \"1568317566012\"            }          }        },        {          \"term\": {            \"outputPath_keyword\": {              \"value\": \"hdfs:/raw/sv2/staging/rtdp/idm/events/non-prod/hilton/year=2019/month=08/day=14/hour=10/min=00\"            }          }        }      ]    }  },  \"aggs\" : {    \"total_event\" : { \"sum\" : { \"field\" : \"event_count\" } }  }}"
    (mockESrequest.fetchJobDetailsFromES(_:String,_:String)).expects("rtdp_idm_event_dq_*/_search",before).returns(HttpResponse(eventCountrtdp, 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]()))
    (mockESrequest.fetchJobDetailsFromES(_:String,_:String)).expects("rtdp_idm_event_dq_*/_search",*).returns(HttpResponse(eventCountrtdpAfter, 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]()))
    (mockESrequest.fetchJobDetailsFromES(_:String,_:String)).expects("pxassist_event_dq_*/_search",*).returns(HttpResponse(eventCountpxassist, 200, scala.Predef.Map[scala.Predef.String, scala.IndexedSeq[scala.Predef.String]]())).repeat(2)


    val testMap = Map(1566965700000l->Map("0005700-190802014502989-oozie-oozi-W" -> Seq("hdfs://stable01///raw/sv2/staging/rtdp/idm/events/non-prod/hilton/year=2019/month=08/day=14/hour=10/min=00")),
                      1566964800000l-> Map("0005697-190802014502989-oozie-oozi-W"->Seq("hdfs://stable01///raw/sv2/staging/pxassist/idm/events/non-prod/hilton/year=2019/month=08/day=14/hour=11/min=00")))

    val lateArrivedEventsHandlerMock = new LateArrivedEventsHandler {
      var invokeRetryCalled = false
      override def invokeRetry(jobList:scala.collection.mutable.Set[String]): Unit =
      {
        invokeRetryCalled = true
      }
    }
    lateArrivedEventsHandlerMock.esRequestHandler = mockESrequest
    lateArrivedEventsHandlerMock.checkAndTriggerRetry(testMap)
    assert(lateArrivedEventsHandlerMock.invokeRetryCalled == true,"Retry of jobs methid was not called")
  }
}
