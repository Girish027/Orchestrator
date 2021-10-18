package com.tfs.orchestrator.managers

import java.util
import java.util.{Date, Properties}

import com.tfs.orchestrator.exceptions.{HibernateException, RestClientException, ScheduleException}
import com.tfs.orchestrator.hibernate.entities.ViewClientJobId
import com.tfs.orchestrator.hibernate.service.ViewClientJobIdService
import com.tfs.orchestrator.hibernate.utils.HibernateUtils
import com.tfs.orchestrator.jobs.runner.JobRunner
import com.tfs.orchestrator.properties.{JobScheduleProperties, SystemProperties}
import com.tfs.orchestrator.utils.RESOURCE_DEGREE
import org.hibernate.Session
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

import scala.util.Try

class OrchestratorManagerTest extends FlatSpec with MockFactory {
  behavior of "OrchestratorManager"

  it should("schedule a cron oozie job for given JobScheduleProperties") in {
    val mockJobRunner = new MockJobRunner
    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(new MockHibernateUtils))
    manager.createCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs"))
    assert(mockJobRunner.jobPropsScheduleCron.clientIdentifier.equals("hilton"))
    assert(mockJobRunner.jobPropsScheduleCron.viewName.equals("DummyReport"))
    assert(mockJobRunner.jobPropsScheduleCron.userName.equals("tfs"))
    assert(mockJobRunner.jobPropsScheduleCron.jobComplexity.equals(RESOURCE_DEGREE.LOW))
  }

  it should("should update the oozie cron job details to the Database") in {
    val mockHibernateUtils = new MockHibernateUtils
    val manager = new MockOrchestratorManager(new MockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    manager.createCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs"))
    assert(mockHibernateUtils.createCount.equals(1))
  }

  "OrchestratorManager.createCronJob" should "return false, if scheduleCron fails" in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]
    (mockJobRunner.getRunningJobId _).expects(*).returns(None)
    (mockJobRunner.scheduleCron _).expects(*).throws(new ScheduleException(""))

    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(!manager.createCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  "OrchestratorManager.createCronJob" should "return false, if scheduleCron succeeds and db update fails" in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]
    (mockJobRunner.getRunningJobId _).expects(*).returns(None)
    (mockJobRunner.scheduleCron _).expects(*).returns(Some("1234-5678"))
    (mockHibernateUtils.executeCreate _).expects(*).throws(new HibernateException(""))
    (mockJobRunner.killJob _).expects(*).returns(true)

    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(!manager.createCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  "OrchestratorManager.createCronJob" should "return true, if scheduleCron succeeds, db update fails but could " +
    "not kill job for consistency" in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]
    (mockJobRunner.getRunningJobId _).expects(*).returns(None)
    (mockJobRunner.scheduleCron _).expects(*).returns(Some("1234-5678"))
    (mockHibernateUtils.executeCreate _).expects(*).throws(new HibernateException(""))
    (mockJobRunner.killJob _).expects(*).throws(new ScheduleException(""))

    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(manager.createCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  it should("kill the oozie cron job, if there's a scheduled job already") in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]

    (mockHibernateUtils.executeSelect _).expects(*).returns(createDummyList(1, "hilton", "Billing", "1234-5678"))
    (mockJobRunner.isJobAlive _).expects("1234-5678").returns(true)
    (mockJobRunner.killJob _).expects("1234-5678").returns(true)
    (mockHibernateUtils.executeDelete _).expects(*).once()
    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))

    assert(manager.killCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  it should("not kill any oozie cron job or update the db, if there's a scheduled no job already") in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]

    (mockHibernateUtils.executeSelect _).expects(*).returns(new util.ArrayList[ViewClientJobId]()).twice()
    (mockJobRunner.isJobAlive _).expects("1234-5678").never()
    (mockJobRunner.killJob _).expects("1234-5678").never()
    (mockHibernateUtils.executeDelete _).expects(*).never()
    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(manager.killCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  it should("not kill any oozie cron job but update the db, if there's a scheduled job already") in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]

    (mockHibernateUtils.executeSelect _).expects(*).returns(createDummyList(1, "hilton", "Billing", "1234-5678")).once()
    (mockJobRunner.isJobAlive _).expects("1234-5678").returns(false)
    (mockJobRunner.killJob _).expects("1234-5678").never()
    (mockHibernateUtils.executeDelete _).expects(*).once
    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(manager.killCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  it should("return false, if oozie could not query/kill the job") in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]

    (mockHibernateUtils.executeSelect _).expects(*).returns(createDummyList(1, "hilton", "Billing", "1234-5678"))
    (mockJobRunner.isJobAlive _).expects("1234-5678").throws(new ScheduleException(""))
    (mockJobRunner.killJob _).expects("1234-5678").never()
    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(!manager.killCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }


  it should("return false, if db update after killing fails") in {
    val mockJobRunner = mock[JobRunner]
    val mockHibernateUtils = mock[MockHibernateUtils]

    (mockHibernateUtils.executeSelect _).expects(*).returns(createDummyList(1, "hilton", "Billing", "1234-5678"))
    (mockJobRunner.isJobAlive _).expects("1234-5678").returns(true)
    (mockJobRunner.killJob _).expects("1234-5678").returns(true)
    (mockHibernateUtils.executeDelete _).expects(*).throws(new HibernateException(""))
    val manager = new MockOrchestratorManager(mockJobRunner, new ViewClientJobIdService(mockHibernateUtils))
    assert(!manager.killCronJob(JobScheduleProperties("hilton", "DummyReport", "tfs")))
  }

  it should("schedule a new job if no job is running for view_client") in{
    val orchestratorManager = new OrchestratorManager{
      override def getRunningJobId(viewClient: String): Option[String] = {
        None
      }
      override def scheduleCron(jobProperties: JobScheduleProperties): Option[String] = {
        Some("0000683-180830064731168-oozie-oozi-C")
      }
      override def createViewClientJobId(jobProperties: JobScheduleProperties, cronJobId: String): ViewClientJobId = {
        new ViewClientJobId
      }
      override def saveViewClientJobId(clientJobId: Option[ViewClientJobId]): Unit = {}
    }
    var jobPropsScheduleCron = JobScheduleProperties("hilton", "Billing", "test_user")
    val create : Boolean = orchestratorManager.createCronJob(jobPropsScheduleCron)
    assert(create == true)
  }

 /* not applicable anymore with code fix in place for a bug : jobs getting descheduled.
  it should("not schedule a new job and continue with job id of current running job if some job is running for view_client") in{
    val orchestratorManager = new OrchestratorManager{
      override def getRunningJobId(viewClient: String): Option[String] = {
        Some("0000683-180830064731168-oozie-oozi-C")
      }
      override def createViewClientJobId(jobProperties: JobScheduleProperties, cronJobId: String): ViewClientJobId = {
        new ViewClientJobId
      }
      override def saveViewClientJobId(clientJobId: Option[ViewClientJobId]): Unit = {}
    }
    var jobPropsScheduleCron = JobScheduleProperties("hilton", "Billing", "test_user")
    val create : Boolean = orchestratorManager.createCronJob(jobPropsScheduleCron)
    assert(create == true)
  }*/

  it should("throw RestClientException if fails in getting running job id") in{
    val orchestratorManager = new OrchestratorManager{
      override def getRunningJobId(viewClient: String): Option[String] = {
        throw new RestClientException("")
      }
    }
    var jobPropsScheduleCron = JobScheduleProperties("hilton", "Billing", "test_user")
    try{
      orchestratorManager.createCronJob(jobPropsScheduleCron)
    }catch {
      case ex: Exception => {
        assert(ex.isInstanceOf[RestClientException])
      }
    }
  }

  def createDummyList(id: Int, client: String, viewName: String, jobId: String) = {
    val list = new util.ArrayList[ViewClientJobId]()
    val viewIdJob = new ViewClientJobId
    viewIdJob.clientId = client
    viewIdJob.id = id
    viewIdJob.viewName = viewName
    viewIdJob.jobId = jobId
    viewIdJob.jobStartEpochTime = System.currentTimeMillis()
    viewIdJob.jobEndEpochTime = System.currentTimeMillis()
    list.add(viewIdJob)
    list
  }

  class MockHibernateUtils extends HibernateUtils {
    var createCount: Int = 0
    var killCount: Int = 0
    override def executeCreate(createFunction: Session=>Unit) = {createCount= createCount+1}
    override def executeSelect(selectFunction: Session=>util.List[_]): util.List[_] = {
      createDummyList(1, "hilton", "DummyReport", "1234-5678")
    }
    override def executeDelete(deleteFunction: (Session)=>Unit) = { killCount = killCount+1;}

  }

  class MockOrchestratorManager(override val jobRunner: JobRunner,
                                override val viewClientJobIdService:ViewClientJobIdService) extends OrchestratorManager {

  }

  class MockJobRunner extends JobRunner {
    var jobPropsScheduleCron: JobScheduleProperties = null
    var jobPropsScheduleOnce: JobScheduleProperties = null
    var jobIdKillJob: String = null

    override def scheduleCron(jobProperties: JobScheduleProperties): Option[String] = {
      this.jobPropsScheduleCron = jobProperties; Some("true")
    }

    override def initialize(systemProperties: SystemProperties): Unit = {}

    override def killJob(jobId: String): Boolean = {this.jobIdKillJob=jobId; true}

    override def scheduleOnce(jobProperties: JobScheduleProperties): Option[String] = {
      this.jobPropsScheduleOnce = jobProperties;
      Some("true")
    }
    override def isJobAlive(jobId: String): Boolean = true

    override def updateJobEndTime(jobId: String, endTime: Date) = true

    override def change(jobId: String, changeValue: String)= true

    override def getRunningJobId(view_client : String): Option[String] = None

    override def rerun(wfId:String,conf:Properties): Try[Unit] = Try(Unit)
  }
}
