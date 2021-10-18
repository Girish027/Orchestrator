package com.tfs.orchestrator.tasks

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Date

import com.tfs.orchestrator.utils.SlaRecordPublish.JobMetadata
import com.tfs.orchestrator.utils.kafka.KafkaWriter.{PlatformStats, PlatformDetailsStats}
import com.tfs.orchestrator.utils.{Constants, DateUtils, SlaRecordPublish, PropertyReader}
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import org.scalatest.FlatSpec
class SlaPublishTaskSpec extends FlatSpec {
  
  implicit val formats = DefaultFormats
  behavior of "SlaPublishTask"

  val propsDir = "src/main/resources/properties"
  PropertyReader.initialize(false, propsDir)

  //it should "construct ES data type url as expected." in {
    //assert(SlaRecordPublish.getESDataTypeUrl().equals("http://localhost:9021/dp2-jobmetrics*/_mapping/dp2-jobmetrics*"))
  //}

  it should "construct ES index url as expected." in {
    assert(SlaRecordPublish.getESIndexUrl().equals("http://localhost:9021/dp2-jobmetrics*"))
  }

  it should "construct ES ingestion url for sla metrics publish as expected." in {
    /*assert(SlaRecordPublish.getESIngestionUrl(None).equals("http://localhost:9021/dp2orchestrator/jobStats2"))
    assert(SlaRecordPublish.getESIngestionUrl(Option("123")).equals("http://localhost:9021/dp2orchestrator/jobStats2/123"))*/
    assert(SlaRecordPublish.getESIngestionUrl(None).equals("http://localhost:9021/dp2-jobmetrics*/dp2-jobmetrics*"))
    assert(SlaRecordPublish.getESIngestionUrl(Option("123")).equals("http://localhost:9021/dp2-jobmetrics*/dp2-jobmetrics*/123"))
  }

  it should "parse the oozie workflow stats properly and convert them into output format." in {
    val oozieWorkflowStats = SlaRecordPublish.constructOozieWFStats(
      getDummyOozieResponse("src/test/resources/samples/oozieResponse.txt"), JobMetadata("v1", "c1", 0L, 0L, 0L),"dummyDQStatus","FAILED")
    assert(oozieWorkflowStats.workflowStatus.equalsIgnoreCase("FAILED"))
    assert(oozieWorkflowStats.retryCount.equals(10))
    assert(oozieWorkflowStats.actions.toList.size == 7)
    //Actions should have been added in order
    assert(oozieWorkflowStats.actions(0).actionName.equals(":start:"))
    assert(oozieWorkflowStats.actions(1).actionName.equals("PreprocessTask"))
    assert(oozieWorkflowStats.actions(2).actionName.equals("input_availability"))
    assert(oozieWorkflowStats.actions(3).actionName.equals("ProcessingTask"))
    assert(oozieWorkflowStats.actions(4).actionName.equals("PublishTask"))
    assert(oozieWorkflowStats.actions(5).actionName.equals("emailFail"))
    assert(oozieWorkflowStats.actions(6).actionName.equals("fail"))

    //Actions should have the start time and end time as appropriately
    assert(oozieWorkflowStats.actions(3).startTime.equals("Thu, 22 Feb 2018 07:41:33 GMT"))
    assert(oozieWorkflowStats.actions(3).endTime.equals("Thu, 22 Feb 2018 08:00:06 GMT"))

    //Actions should have status converted to appropriate status.
    assert(oozieWorkflowStats.actions(3).status.equals("SUCCESS"))
    assert(oozieWorkflowStats.actions(4).status.equals("FAIL"))
  }

  it should "set the workflow status as SUCCESS if all actions are OK" in {
    val oozieWorkflowStats = SlaRecordPublish.constructOozieWFStats(
      getDummyOozieResponse("src/test/resources/samples/oozieSuccessReponse.txt"), JobMetadata("v1", "c1", 0L, 0L, 0L),"dummyDQStatus","SUCCEEDED")
    assert(oozieWorkflowStats.workflowStatus.equalsIgnoreCase("SUCCEEDED"))
  }

  it should "skip the running tasks" in {
    val oozieWorkflowStats = SlaRecordPublish.constructOozieWFStats(
      getDummyOozieResponse("src/test/resources/samples/oozieRunningResponse.txt"), JobMetadata("View_DCF",
        "sainsburysargos", 0L, 1530028800000L, 1530032400000L),"dummyDQStatus","SUCCEEDED")
    assert(oozieWorkflowStats.workflowStatus.equalsIgnoreCase("SUCCEEDED"))
    assert(oozieWorkflowStats.actions.length==3)
    assert(oozieWorkflowStats.actions(0).actionName.equals(":start:"))
    assert(oozieWorkflowStats.actions(1).actionName.equals("PreprocessTask"))
    assert(oozieWorkflowStats.actions(2).actionName.equals("input_availability"))
  }

  it should "parse dqStatus from Es response when record exist in ES" in {
    val validResponse =
      """{"_index":"dp2-dq-summary","_type":"dq-report-summary","_id":"0000019-181019015039516-oozie-oozi-W","_version":1,"found":true,
        |"_source":{"JobId":"0000019-181019015039516-oozie-oozi-W","DQStatus":"MYSTATUS"}}""".stripMargin
    val oozieWorkflowStats = SlaRecordPublish.fetchDQStatusFromResponseJSON(validResponse)
    assert(oozieWorkflowStats.equals("MYSTATUS"))
  }

  it should "parse dqStatus from Es response when record doesn't exist in ES" in {
    val validResponse = """{"_index":"dp2-dq-summary","_type":"dq-report-summary","_id":"0000019-181019015039516-oozie-oozi-Ww","found":false}"""
    val oozieWorkflowStats = SlaRecordPublish.fetchDQStatusFromResponseJSON(validResponse)
    assert(oozieWorkflowStats.equals("NA"))
  }

  it should "match the data pushed into dp2 metrics in Kafka" in {

    var oozieResponseStr = getDummyOozieResponse("src/test/resources/samples/oozieWFResponse.txt")
    var metricsResponse = getDummyOozieResponse("src/test/resources/samples/insertedMetrics.txt")
    var oozieResponse = parse(oozieResponseStr).asInstanceOf[JObject]
    val slaRecordPublish = SlaRecordPublish
    var actions = slaRecordPublish.constructOozieActionStats((oozieResponse \ "actions").asInstanceOf[JArray])
    var jsonString = compactRender(Extraction.decompose(PlatformDetailsStats("0001776-190612044448318-oozie-oozi-W",
      "dp2-jobmetrics", 1560874689489L , PlatformStats("Digital_Test4", "hilton", 1541934000000L, 1541934000000L, 1541937600000L,
        1560874602000L, 1560874689000L, (1560874689000L - 1560874602000L), actions, 0, "SUCCEEDED", "NA",""))))

    assert(jsonString.equals(metricsResponse))

  }

  it should "convert scheduleStartTime in GMT to correct timezone time for SFTP" in {
    val scheduleStartTime: Date = DateUtils.utcStringToDate(Constants.SPARTAN_DATE_FORMAT, "201902021700")
    val sftpStartTime = DateUtils.fetchClientZoneTime(scheduleStartTime.getTime, "Asia/Kolkata", Constants.SFTP_TIME_FORMAT)
    System.out.println("time : "+ sftpStartTime)
    assert(sftpStartTime.equals("201902022230"))
  }

  def getDummyOozieResponse(fileName: String) = {
    val specReader = new BufferedReader(new InputStreamReader(
      new FileInputStream(fileName)))

    val specBuilder = new StringBuilder
    while (specReader.ready()) {
      specBuilder.append(specReader.readLine)
    }

    specBuilder.toString()
  }

}
