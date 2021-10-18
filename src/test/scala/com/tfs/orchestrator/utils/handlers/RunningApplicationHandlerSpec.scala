package com.tfs.orchestrator.utils.handlers

import com.tfs.orchestrator.cache.{CacheEntry, ExecutionCache}
import com.tfs.orchestrator.catalog.polling.ViewSchedule
import com.tfs.orchestrator.utils.{Constants, HadoopUtils, PropertyReader}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.http.HttpStatus
import org.apache.logging.log4j.scala.Logging
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import scalaj.http.HttpResponse

import scala.collection.mutable.ListBuffer

class RunningApplicationHandlerSpec extends FlatSpec with MockFactory with Logging {
  behavior of "RunningApplicationHandler"

  PropertyReader.initialize(false, "src/test/resources/properties")
  ExecutionCache.invalidate()

  it should "return an empty list of running applications with empty execution cache" in {
    val runningApplicationHandler = new RunningApplicationHandlerMock()
    assert(runningApplicationHandler.fetchRunningApplications().size == 0)
  }

  it should "return an empty list of running applications with non empty execution cache" in {
    initializeCache
    val runningApplicationHandler = new RunningApplicationHandler() {
      protected override def fetchAndFilterRunningApplications(appLists: ListBuffer[String], entry: CacheEntry) = {
      }
    }
    assert(runningApplicationHandler.fetchRunningApplications().size == 0)
  }


  it should "return a non empty list of running applications with non empty execution cache" in {
    initializeCache
    val runningApplicationHandler = new RunningApplicationHandler() {
      protected override def fetchAndFilterRunningApplications(appLists: ListBuffer[String], entry: CacheEntry) = {
        if (entry.clientName.equals("dish")) {
          appLists += "id1"
          appLists += "id2"
        }
      }
    }
    assert(runningApplicationHandler.fetchRunningApplications().size == 2)
  }


  it should "return an empty list of running applications with non ok http response" in {
    val runningApplicationHandler = new RunningApplicationHandlerMock()
    val appLists = ListBuffer[String]()
    val entry = new CacheEntry("View_Dimensional", "dish", "60")
    val httpResponse = new HttpResponse[String]("", HttpStatus.SC_BAD_REQUEST, null)
    runningApplicationHandler.addToRunningApplicationList(appLists, entry, httpResponse)
    assert(appLists.size == 0)
  }

  it should "return a non empty list of running applications with ok http response" in {
    val runningApplicationHandler = new RunningApplicationHandlerMock()
    val appLists = ListBuffer[String]()
    val entry = new CacheEntry("View_Dimensional", "dish", "60")
    val body = "{\n  \"took\": 5,\n  \"timed_out\": false,\n  \"_shards\": {\n    \"total\": 5,\n    \"successful\": 5,\n    \"skipped\": 0,\n    \"failed\": 0\n  },\n  \"hits\": {\n    \"total\": 10,\n    \"max_score\": 8.547091,\n    \"hits\": [\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000668-190104041009979-oozie-oozi-W\",\n        \"_score\": 8.547091,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000464-190104041009979-oozie-oozi-W\",\n        \"_score\": 8.524912,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000150-190104041009694-oozie-oozi-W\",\n        \"_score\": 8.524912,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000612-190104041009979-oozie-oozi-W\",\n        \"_score\": 8.493747,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000650-190104041009979-oozie-oozi-W\",\n        \"_score\": 8.493747,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000749-190104041009979-oozie-oozi-W\",\n        \"_score\": 8.493747,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000188-190104041009694-oozie-oozi-W\",\n        \"_score\": 8.493747,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000207-190104041009694-oozie-oozi-W\",\n        \"_score\": 8.493747,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000536-190104041009979-oozie-oozi-W\",\n        \"_score\": 8.391901,\n        \"_source\": {}\n      },\n      {\n        \"_index\": \"dp2-jobmetrics*\",\n        \"_type\": \"dp2-jobmetrics*\",\n        \"_id\": \"0000276-190104041009694-oozie-oozi-W\",\n        \"_score\": 8.391901,\n        \"_source\": {}\n      }\n    ]\n  }\n}"
    val httpResponse = new HttpResponse[String](body, HttpStatus.SC_OK, null)
    runningApplicationHandler.addToRunningApplicationList(appLists, entry, httpResponse)
    assert(appLists.size == 10)
  }

  it should "return default timeout of 2" in {
    val entry = new CacheEntry("View_Dimensional","dish", "-1")
    val runningApplicationHandler = new RunningApplicationHandlerMock
    assert(runningApplicationHandler.deriveSlaTime(entry)==2)
  }


  it should "return entry timeout multiplies by the multiplier " in {
    val entry = new CacheEntry("View_Dimensional","dish", "25")
    val runningApplicationHandler = new RunningApplicationHandlerMock
    assert(runningApplicationHandler.deriveSlaTime(entry)==50)
  }

  private def initializeCache = {
    val jsonStr = "[{\n        \"viewName\": \"View_Dimensional\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"searsonline\" , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"1\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"dish\" , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"hilton\" , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"DP2\",\n        \"jobStartTime\": \"1535706000000\",\n        \"jobEndTime\": \"1543572000000\",\n        \"complexity\": \"3\"\n    }]"
    cache(jsonStr)
  }

  private def cache(jsonStr: String) = {
    ExecutionCache.invalidate()
    implicit val formats = DefaultFormats
    val viewSchedules = parse(jsonStr).extract[List[ViewSchedule]]
    ExecutionCache.cacheExecutionProperties(viewSchedules)
  }

  class RunningApplicationHandlerMock extends RunningApplicationHandler {
    override def addToRunningApplicationList(appLists: ListBuffer[String], entry: CacheEntry, response: HttpResponse[String]) = {
      super.addToRunningApplicationList(appLists, entry, response)
    }

    override def deriveSlaTime(entry: CacheEntry) = {
      super.deriveSlaTime(entry)
    }

    override def fetchRunningFilter(): String = {
      ""
    }

  }

}
