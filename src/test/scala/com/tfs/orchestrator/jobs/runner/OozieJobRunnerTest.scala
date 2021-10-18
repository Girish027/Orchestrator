package com.tfs.orchestrator.jobs.runner

import java.util
import java.util.{Date, Properties}

import com.tfs.orchestrator.exceptions.{RestClientException, InvalidIngestionType, InitializationException}
import com.tfs.orchestrator.jobs.runner.oozie.OozieJobRunner
import com.tfs.orchestrator.properties.{JobScheduleProperties, SystemProperties}
import com.tfs.orchestrator.utils.PropertyReader
import org.apache.oozie.client.WorkflowJob.Status
import org.apache.oozie.client.{OozieClient, WorkflowAction, WorkflowJob}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec


class OozieJobRunnerTest extends FlatSpec with MockFactory {

  behavior of "OozieJobRunner"
  class MockOozieJobRunner(val clientForTest: OozieClient=null) extends OozieJobRunner(new SystemProperties(){}) {
    override def initialize(systemProperties: SystemProperties) = {}

    this.client = clientForTest
  }

  try {
    PropertyReader.initialize(false, "src/main/resources/properties")
  } catch {
    case ex: InitializationException => {
      //PropertyReader Suite might have altered the "properties.dir" property.
      Thread.sleep(50)
      PropertyReader.initialize(false, "src/main/resources/properties")
    }
  }

  it should "have the default oozie properties initialized." in {
    val mockOozieJobRunner = new MockOozieJobRunner()
    assert(mockOozieJobRunner.defaultQueueName.equals("default"))
    assert(mockOozieJobRunner.useSystemPath.equals("true"))
    assert(mockOozieJobRunner.oozieClientUrl.equals("http://psr-dap-druid02.app.shared.int.sv2.247-inc.net:11000/oozie"))
    assert(mockOozieJobRunner.oozieLibpath.equals("/user/oozie/dp2/lib"))
  }

  it should "invoke the OozieClient run function to run the cron job" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    //Set the expectation
    (client.run _).expects(*).returns("hello")
    assert(mockOozieJobRunner.scheduleCron(JobScheduleProperties("tfs", "billing", "oozie")).get.equals("hello"))
  }

  it should "invoke OozieClient's change function on timestamp update" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    //Set the expectation
    (client.change _).expects(*, *).once()
    assert(mockOozieJobRunner.updateJobEndTime("1234-5678", new Date).equals(true))
  }

  it should "invoke OozieClient's getJobInfo function on timestamp update" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    //Set the expectation
    (client.getJobInfo (_:String)).expects(*).returns(new WorkflowJob {
      override def getLastModifiedTime: Date = ???

      override def getAppPath: String = ???

      override def getStartTime: Date = ???

      override def getAcl: String = ???

      override def getEndTime: Date = ???

      override def getRun: Int = ???

      override def getUser: String = ???

      override def getCreatedTime: Date = ???

      override def getConf: String = ???

      override def getId: String = ???

      override def getStatus: Status = WorkflowJob.Status.RUNNING

      override def getParentId: String = ???

      override def getConsoleUrl: String = ???

      override def getGroup: String = ???

      override def getAppName: String = ???

      override def getActions: util.List[WorkflowAction] = ???

      override def getExternalId: String = ???
    })
    assert(mockOozieJobRunner.isJobAlive("1234-5678").equals(true))
  }

  it should "invoke the OozieClient run function to run the regular job" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    //Set the expectation
    (client.run _).expects(*).returns("hello")
    assert(mockOozieJobRunner.scheduleOnce(JobScheduleProperties("tfs", "billing", "oozie")).get.equals("hello"))
  }

  it should "invoke the OozieCLient kill to kill the job" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    //Set the expectation
    (client kill (_:String)).expects(*).once()
    assert(mockOozieJobRunner.killJob("1234-5678").equals(true))
  }

  it should "pouplate all the cron job related properties before firing the request" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    var captureProps:Properties = new Properties()
    val jobProperties = JobScheduleProperties("tfs", "billing", "oozie")
    jobProperties.jobStartTime = new Date(1514764800000L)
    jobProperties.jobEndTime = new Date(1514768400000L)

    //Set the expectation
    (client.run (_:Properties)).expects(*).onCall { props: Properties => captureProps=props; "hello" }
    assert(mockOozieJobRunner.scheduleCron(jobProperties).get.equals("hello"))
    assert(captureProps.getProperty("viewName") != null)
    assert(captureProps.getProperty("clientName") != null)
    assert(captureProps.getProperty("cronExpression") != null)
    assert(captureProps.getProperty("jobStartTime") != null && captureProps.getProperty("jobStartTime").equals("2018-01-01T00:00Z"))
    assert(captureProps.getProperty("jobEndTime") != null && captureProps.getProperty("jobEndTime").equals("2018-01-01T01:00Z"))
    assert(captureProps.getProperty("workflowForCoord") != null)
    assert(captureProps.getProperty("jobTracker") != null)
    assert(captureProps.getProperty("nameNode") != null)
    assert(captureProps.getProperty("queueName") != null)
    assert(captureProps.getProperty("jobComplexity") != null)
    assert(captureProps.getProperty("sleepTime") != null)
    assert(captureProps.getProperty("cronExpression") != null)

    assert(captureProps.getProperty("user.name") != null)
    assert(captureProps.getProperty("oozie.coord.application.path") != null)
    assert(captureProps.getProperty("workflowForCoord") != null)
    assert(captureProps.getProperty("replayTask") != null)
    assert(captureProps.getProperty("hdfsConfigDir") != null)
  }

  it should "pouplate all the workflow related properties before firing the request" in {
    val client = mock[OozieClient]
    val mockOozieJobRunner = new MockOozieJobRunner(client)
    var captureProps:Properties = new Properties()
    //Set the expectation
    (client.run (_:Properties)).expects(*).onCall { props: Properties => captureProps=props; "hello" }
    assert(mockOozieJobRunner.scheduleOnce(JobScheduleProperties("tfs", "billing", "oozie")).get.equals("hello"))
    assert(captureProps.getProperty("viewName") != null)
    assert(captureProps.getProperty("clientName") != null)
    assert(captureProps.getProperty("cronExpression") != null)
    assert(captureProps.getProperty("jobStartTime") != null)
    assert(captureProps.getProperty("jobEndTime") != null)
    assert(captureProps.getProperty("jobTracker") != null)
    assert(captureProps.getProperty("nameNode") != null)
    assert(captureProps.getProperty("queueName") != null)
    assert(captureProps.getProperty("jobComplexity") != null)
    assert(captureProps.getProperty("sleepTime") != null)
    assert(captureProps.getProperty("cronExpression") != null)

    assert(captureProps.getProperty("user.name") != null)
    assert(captureProps.getProperty("oozie.wf.application.path") != null)

  }

  it should "return job id if one job is running" in {
    val oozieJobRunner = new OozieJobRunner(new SystemProperties {}){
      override def getRestResponse(url: String): String ={
        getSingleCordinatorJobResponse
      }
      override def initialize(systemProperties: SystemProperties) = {}
      override def getRunningCordinatorJobUrl(view_client : String): String ={
        "http://oozie.hadoop.247-inc.net:11000/oozie/v2/jobs?jobtype=coordinator&filter=name%3DView_DCF_hilton;status%3DRUNNING"
      }
    }
    val jobId : Option[String] = oozieJobRunner.getRunningJobId("View_DCF_hilton")
    assert(jobId == Some("0000683-180830064731168-oozie-oozi-C"))
  }

  it should "return job id of the job with last end time if more than one job is running" in {
    val oozieJobRunner = new OozieJobRunner(new SystemProperties {}){
      override def getRestResponse(url: String): String ={
        getMultipleCordinatorJobResponse
      }
      override def initialize(systemProperties: SystemProperties) = {}
      override def getRunningCordinatorJobUrl(view_client : String): String ={
        "http://oozie.hadoop.247-inc.net:11000/oozie/v2/jobs?jobtype=coordinator&filter=name%3DView_DCF_hilton;status%3DRUNNING"
      }
    }
    val jobId : Option[String] = oozieJobRunner.getRunningJobId("View_DCF_hilton")
    assert(jobId == Some("1111683-180830064731168-oozie-oozi-C"))
  }

  it should "throw RestClientException if fails in getting cordinator job response" in {
    val oozieJobRunner = new OozieJobRunner(new SystemProperties {}) {
      override def getRestResponse(url: String): String = {
        throw new RestClientException("")
      }

      override def initialize(systemProperties: SystemProperties) = {}
      override def getRunningCordinatorJobUrl(view_client : String): String ={
        "http://oozie.hadoop.247-inc.net:11000/oozie/v2/jobs?jobtype=coordinator&filter=name%3DView_DCF_hilton;status%3DRUNNING"
      }
    }
    try{
      val jobId : Option[String] = oozieJobRunner.getRunningJobId("View_DCF_hilton")
    }catch {
      case ex: Exception => {
        assert(ex.isInstanceOf[RestClientException])
      }
    }
  }

  def getSingleCordinatorJobResponse: String = {
    "{\"total\":1,\"offset\":1,\"len\":50,\"coordinatorjobs\":[{\"mat_throttling\":0,\"executionPolicy\":\"FIFO\",\"conf\":null,\"acl\":null,\"frequency\":\"0 * * * *\"," +
      "\"total\":0,\"pauseTime\":null,\"consoleUrl\":null,\"lastAction\":\"Thu, 20 Sep 2018 18:00:00 GMT\",\"coordJobName\":\"View_IVR2ChatRequestReport_britishgas\"," +
      "\"startTime\":\"Sat, 01 Sep 2018 10:00:00 GMT\",\"coordExternalId\":null,\"timeUnit\":\"CRON\",\"group\":null,\"coordJobId\":\"0000683-180830064731168-oozie-oozi-C\"," +
      "\"coordJobPath\":\"hdfs:\\/\\/production1\\/user\\/dataplat\\/dp2\\/orchestrator\\/coord\\/coord.xml\",\"bundleId\":null,\"timeZone\":\"UTC\",\"concurrency\":1," +
      "\"timeOut\":120,\"nextMaterializedTime\":\"Thu, 20 Sep 2018 18:00:00 GMT\",\"asString\":\"Coordinator application id[0000683-180830064731168-oozie-oozi-C] status[RUNNING]\"," +
      "\"endTime\":\"Wed, 30 Jun 2021 00:00:00 GMT\",\"user\":\"dataplat\",\"actions\":[],\"status\":\"RUNNING\"}]}"
  }

  def getMultipleCordinatorJobResponse: String = {
    "{\"total\":2,\"offset\":1,\"len\":50,\"coordinatorjobs\":[{\"mat_throttling\":0,\"executionPolicy\":\"FIFO\",\"conf\":null,\"acl\":null,\"frequency\":\"0 * * * *\",\"total\":0," +
      "\"pauseTime\":null,\"consoleUrl\":null,\"lastAction\":\"Thu, 20 Sep 2018 18:00:00 GMT\",\"coordJobName\":\"View_IVR2ChatRequestReport_britishgas\"," +
      "\"startTime\":\"Sat, 01 Sep 2018 10:00:00 GMT\",\"coordExternalId\":null,\"timeUnit\":\"CRON\",\"group\":null,\"coordJobId\":\"0000683-180830064731168-oozie-oozi-C\"," +
      "\"coordJobPath\":\"hdfs:\\/\\/production1\\/user\\/dataplat\\/dp2\\/orchestrator\\/coord\\/coord.xml\",\"bundleId\":null,\"timeZone\":\"UTC\",\"concurrency\":1," +
      "\"timeOut\":120,\"nextMaterializedTime\":\"Thu, 20 Sep 2018 18:00:00 GMT\",\"asString\":\"Coordinator application id[0000683-180830064731168-oozie-oozi-C] status[RUNNING]\"," +
      "\"endTime\":\"Wed, 30 Sep 2018 00:00:00 GMT\",\"user\":\"dataplat\",\"actions\":[],\"status\":\"RUNNING\"},{\"mat_throttling\":0,\"executionPolicy\":\"FIFO\",\"conf\":null," +
      "\"acl\":null,\"frequency\":\"0 * * * *\",\"total\":0,\"pauseTime\":null,\"consoleUrl\":null,\"lastAction\":\"Thu, 20 Sep 2018 18:00:00 GMT\"," +
      "\"coordJobName\":\"View_IVR2ChatRequestReport_britishgas\",\"startTime\":\"Sat, 01 Sep 2018 10:00:00 GMT\",\"coordExternalId\":null,\"timeUnit\":\"CRON\",\"group\":null," +
      "\"coordJobId\":\"1111683-180830064731168-oozie-oozi-C\",\"coordJobPath\":\"hdfs:\\/\\/production1\\/user\\/dataplat\\/dp2\\/orchestrator\\/coord\\/coord.xml\"," +
      "\"bundleId\":null,\"timeZone\":\"UTC\",\"concurrency\":1,\"timeOut\":120,\"nextMaterializedTime\":\"Thu, 20 Sep 2018 18:00:00 GMT\"," +
      "\"asString\":\"Coordinator application id[1111683-180830064731168-oozie-oozi-C] status[RUNNING]\",\"endTime\":\"Wed, 30 Jun 2021 00:00:00 GMT\",\"user\":\"dataplat\"," +
      "\"actions\":[],\"status\":\"RUNNING\"}]}"
  }
}