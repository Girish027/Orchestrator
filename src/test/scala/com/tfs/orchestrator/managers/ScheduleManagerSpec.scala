package com.tfs.orchestrator.managers

import java.util.Date

import com.tfs.orchestrator.catalog.polling.{ClientExecutionProperties, ClientsCronExpression, ViewSchedule}
import com.tfs.orchestrator.hibernate.entities.{View, ViewClientJobId}
import com.tfs.orchestrator.hibernate.service.{ViewClientJobIdService, ViewService}
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.utils.{DateUtils, PropertyReader, RESOURCE_DEGREE}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer

class ScheduleManagerSpec extends FlatSpec with MockFactory {

  private val nextHourDate = DateUtils.convertToNextHourDate(new Date())

  private val mockOrchestratorManager = mock[OrchestratorManager]
  private val mockViewService = mock[ViewService]
  private val mockViewClientJobIdService = mock[ViewClientJobIdService]
  PropertyReader.initialize(false, "src/test/resources/properties")


  it should "descheduled jobs and update an entry from the view table if an existing client is deleted for the view" in {

    val view = getViewInstance("c1,c2", "test")

    val mockScheduleManager = new MockScheduleManager()

    (mockViewService.updateView _).expects(*).once()
    val vcJobID1 = getViewClientJobIDInstance("c1", "test")
    vcJobID1.jobEndEpochTime = nextHourDate.getTime

    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(view.viewName, "c1", view.cronExpression).returns(Option(List(vcJobID1)))
    (mockOrchestratorManager.change _).expects(vcJobID1, nextHourDate).returns(true)

    var clientList = List("c1")
    val deletedClients: List[String] = clientList.filterNot(List("c2").contains(_))
    mockScheduleManager.deScheduleDeletedClients(view, deletedClients)
    assert(view.clientIds == "c2")
  }

  it should "return JobScheduleProperties for each of the clients in a given View" in {
    val view = new View()
    view.viewName = "DQ_On_RawIDM"
    view.userName = "sys"
    view.jobStartTime = "1501545600000"
    view.jobEndTime = "1504224000000"
    view.jobComplexity = "2 * * * *"
    view.cronExpression = ""
    view.clientIds="siriusxm"

    val view1 = new View()
    view1.viewName = "DQ_On_RawIDM"
    view1.userName = "sys"
    view1.jobStartTime = "1501545600000"
    view1.jobEndTime = "1504224000000"
    view1.jobComplexity = "2 * * * *"
    view1.cronExpression = ""
    view1.clientIds="dish"

    val jobProperties1 = JobScheduleProperties.apply("siriusxm", view.viewName, view.userName)
    jobProperties1.jobStartTime = new Date(view.jobStartTime.toLong)
    jobProperties1.jobEndTime = new Date(view.jobEndTime.toLong)
    jobProperties1.jobComplexity = RESOURCE_DEGREE.MEDIUM
    jobProperties1.jobCronExpression = view.cronExpression

    val jobProperties2 = JobScheduleProperties.apply("dish", view1.viewName, view1.userName)
    val nextHourDate = DateUtils.convertToNextHourDate(new Date())
    jobProperties2.jobStartTime = nextHourDate
    jobProperties2.jobEndTime = new Date(view1.jobEndTime.toLong)
    jobProperties2.jobComplexity = RESOURCE_DEGREE.MEDIUM
    jobProperties2.jobCronExpression = view1.cronExpression

    val expected = List(jobProperties1, jobProperties2)
    val actual = new ListBuffer[JobScheduleProperties]()
    val mockScheduleManager = new MockScheduleManager
    actual.appendAll(mockScheduleManager.getJobsProperties(view, List("siriusxm")))
    actual.appendAll(mockScheduleManager.getJobsProperties(view1, List("dish"), nextHourDate))

    var compared = false
    expected.foreach(e => {
      compared = actual.exists(a => e.viewName == e.viewName &&
        e.jobCronExpression == a.jobCronExpression &&
        e.jobStartTime == a.jobStartTime &&
        e.jobEndTime == a.jobEndTime &&
        e.jobComplexity == a.jobComplexity)
    })
    assert(compared)
  }

  it should "schedule crons and update views in db when a new view is created for a given client(s)" in {
    var view = new View
    view.userName = "user"
    view.viewName = "test"
    view.jobComplexity = "3"
    view.jobStartTime = "1506726000000"
    view.jobEndTime = "1506726000000"
    view.cronExpression = "30 * * * *"
    view.clientIds="c1"

    var views=List[View]()
    views::=view
    (mockOrchestratorManager.createCronJob _).expects(*).returns(true)
    (mockViewService.getViewByNameAndCron _).expects("test", *).returns(null)
    (mockViewService.saveView _).expects(*).once()
    var clientLists = List("c1")
    (mockViewClientJobIdService.getClientIdsForExistingJobs _).expects(view.viewName, clientLists).returns(None)
    val mockScheduleManager = new MockScheduleManager
    mockScheduleManager.scheduleViewClientsAndUpdate(view, clientLists)

    /*(mockOrchestratorManager.createCronJob _).expects(*).returns(true).twice()
    (mockViewService.getViewByNameAndCron _).expects("test", *).returns(null).once()
    (mockViewService.saveView _).expects(*).once()
    (mockViewService.getViewByNameAndCron _).expects("test", *).returns(views).once()
    (mockViewService.updateView _).expects(*).once()
    clientLists = List("c1", "c2")
    val existingClientIds: List[String] = List("c1")
    (mockViewClientJobIdService.getClientIdsForExistingJobs _).expects(view.viewName, clientLists).returns(Option(existingClientIds))
    mockScheduleManager.scheduleViewClientsAndUpdate(view, List("c1", "c2"))*/
  }

  it should "deschedule NonExistent Crons" in {

    val view = new View
    view.userName = "user"
    view.viewName = "test"
    view.jobComplexity = "3"
    view.jobStartTime = "1506726000000"
    view.jobEndTime = "1506726000000"
    view.cronExpression = "abc"
    view.clientIds = "c1,c2"


    //val clientList = List("c1")
    val execProp: Map[String,String] = Map[String,String]()
    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("c1",execProp,None,null,null)
    val clientExec = ListBuffer[ClientExecutionProperties](clientExecutionProp1)
    val clientsCron = ClientsCronExpression(clientExec.toList, "* * 0 * *")
    val viewSchedule = ViewSchedule("test", List(clientsCron), "user", "1506726000000", "1506726000000", "3")
    val mockScheduleManager = new MockScheduleManager
    (mockViewService.getViewsByName _).expects(viewSchedule.viewName).returns(List(view))
    val vcJobID1: ViewClientJobId = getViewClientJobIDInstance("c1", "test")
    vcJobID1.jobEndEpochTime = nextHourDate.getTime
    val vcJobID2: ViewClientJobId = getViewClientJobIDInstance("c2", "test")
    vcJobID2.jobEndEpochTime = nextHourDate.getTime

    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(view.viewName, vcJobID1.clientId, view.cronExpression).returns(Option(List(vcJobID1)))
    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(view.viewName, vcJobID2.clientId, view.cronExpression).returns(Option(List(vcJobID2)))

    (mockOrchestratorManager.change _).expects(vcJobID1, nextHourDate).returns(true)
    (mockOrchestratorManager.change _).expects(vcJobID2, nextHourDate).returns(true)

    (mockViewService.updateView _).expects(view)
    (mockViewService.deleteView _).expects(view)

    mockScheduleManager.descheduleNonExistentCrons(viewSchedule)

    assert(view.clientIds == "")

  }

  /*it should "schedule jobs for new clients added to the cron bucket" in {

    val targetView = getViewInstance("c1", "test")
    val localView = getViewInstance("c3", "test")

    var localViews=List[View]()
    localViews::=localView

    val existingClients: List[String] = List("c3")
    val newClients: List[String] = targetView.clientIds.split(",").toList

    val existingJobClientIds = List()
    (mockViewClientJobIdService.getClientIdsForExistingJobs _).expects(targetView.viewName, newClients).returns(Option(existingJobClientIds))

    (mockOrchestratorManager.createCronJob _).expects(*).returns(true)

    (mockViewService.getViewByNameAndCron _).expects(targetView.viewName, targetView.cronExpression).returns(localViews)
    (mockViewService.saveView _).expects(targetView)

    val mockScheduleManager = new MockScheduleManager
    mockScheduleManager.scheduleNewClients(targetView, existingClients, newClients)

    //assert(localView.clientIds.split(",").length == 3)


  }*/

  it should "update job end time" in {
    val localView = getViewInstance("c1,c2,c3", "v1")
    val targetView = getViewInstance("c2,c3,c4", "v1")
    localView.jobEndTime = "1506724000000"
    targetView.jobEndTime = "1506725000000"
    val newClientIds = targetView.clientIds.split(",")
    val existingClientIds = localView.clientIds.split(",")
    val commonClientIds = newClientIds.filter(clientId => {
      existingClientIds.contains(clientId)
    }).toList

    val vcJobId1 = getViewClientJobIDInstance("c1", "v1")
    val vcJobId2 = getViewClientJobIDInstance("c2", "v1")
    val vcJobId3 = getViewClientJobIDInstance("c3", "v1")
    val vcJobIds = List(vcJobId1, vcJobId2, vcJobId3)
    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: List[String], _: String)).expects(targetView.viewName, commonClientIds, targetView.cronExpression).returns(Option(vcJobIds))
    (mockViewService.updateView _).expects(localView)

    val mockScheduleManager = new MockScheduleManager {
      override def getCommonClients(newClientIds: Array[String], existingClientIds: Array[String]) = {
        commonClientIds
      }
    }
    mockScheduleManager.updateJobEndTime(localView, targetView)
    assert(localView.jobEndTime == targetView.jobEndTime)

  }

  it should "delete non existent views" in {

    val viewSchedule1 = ViewSchedule("v1", null, null, null, null, null)
    val viewSchedule2 = ViewSchedule("v2", null, null, null, null, null)
    val viewSchedule3 = ViewSchedule("v3", null, null, null, null, null)
    val viewSchedule4 = ViewSchedule("v4", null, null, null, null, null)

    val viewSchedules = List(viewSchedule1, viewSchedule2, viewSchedule3, viewSchedule4)

    val view1 = getViewInstance("c1,c2", "v5")
    val view2 = getViewInstance("c3,c4", "v6")

    val deletedClients = List("c1", "c2", "c3", "c4")
    val views = List(view1, view2)

    (mockViewService.getAllViews _).expects().returns(views)

    val mockScheduleManager = new MockScheduleManager {
      override def deScheduledViewClientsAndUpdate(localView: View, clientIds: List[String]): Unit = {
        assert(deletedClients.containsSlice(clientIds))

      }
    }
    mockScheduleManager.deleteNonExistentViews(viewSchedules)

  }

  it should "deschedule jobs for a client and cron expression having a single existing but active cron jobs" in {

    val localView = getViewInstance("c1", "v1")
    val currentTimeEpoch = System.currentTimeMillis()
    var vcJobId2 = getViewClientJobIDInstance("c1", "v1")
    vcJobId2.jobStartEpochTime = currentTimeEpoch - 100000
    vcJobId2.jobEndEpochTime = currentTimeEpoch + 100000
    val vcJobIds = List(vcJobId2)
    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(localView.viewName, "c1", localView.cronExpression).returns(Option(vcJobIds))

    (mockOrchestratorManager.change _).expects(vcJobId2, nextHourDate).returns(true)

    val mockScheduleManager = new MockScheduleManager
    assert(mockScheduleManager.deScheduleJob(localView, "c1"))

  }


  it should "deschedule jobs for a client and cron expression having existing jobs with past end dates" in {

    val localView = getViewInstance("c1", "v1")
    val currentTimeEpoch = System.currentTimeMillis()
    var vcJobId2 = getViewClientJobIDInstance("c1", "v1")
    vcJobId2.jobStartEpochTime = currentTimeEpoch - 200000
    vcJobId2.jobEndEpochTime = currentTimeEpoch - 100000
    val vcJobIds = List(vcJobId2)
    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(localView.viewName, "c1", localView.cronExpression).returns(Option(vcJobIds))

    val mockScheduleManager = new MockScheduleManager
    assert(mockScheduleManager.deScheduleJob(localView, "c1"))

  }

  it should "deschedule job for a client and cron expression having a single active multiple existing(maybe inactive) cron jobs" in {

    val localView = getViewInstance("c1", "v1")
    val currentTimeEpoch = System.currentTimeMillis()
    var vcJobId1 = getViewClientJobIDInstance("c1", "v1")
    vcJobId1.jobStartEpochTime = currentTimeEpoch - 100000
    vcJobId1.jobEndEpochTime = currentTimeEpoch - 50000
    var vcJobId2 = getViewClientJobIDInstance("c1", "v1")
    vcJobId2.jobStartEpochTime = currentTimeEpoch - 100000
    vcJobId2.jobEndEpochTime = currentTimeEpoch + 100000
    val vcJobIds = List(vcJobId1, vcJobId2)
    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(localView.viewName, "c1", localView.cronExpression).returns(Option(vcJobIds))

    (mockOrchestratorManager.change _).expects(vcJobId2, nextHourDate).returns(true)

    val mockScheduleManager = new MockScheduleManager
    assert(mockScheduleManager.deScheduleJob(localView, "c1"))

  }

  it should "kill future scheduled jobs for a client and cron expression" in {

    val localView = getViewInstance("c1", "v1")
    val currentTimeEpoch = System.currentTimeMillis()
    var vcJobId1 = getViewClientJobIDInstance("c1", "v1")
    vcJobId1.jobStartEpochTime = currentTimeEpoch + 100000
    vcJobId1.jobEndEpochTime = currentTimeEpoch + 150000
    var vcJobId2 = getViewClientJobIDInstance("c1", "v1")
    vcJobId2.jobStartEpochTime = currentTimeEpoch - 100000
    vcJobId2.jobEndEpochTime = currentTimeEpoch - 50000
    val vcJobIds = List(vcJobId1, vcJobId2)
    (mockViewClientJobIdService.getViewClientJobIds(_: String, _: String, _: String)).expects(localView.viewName, "c1", localView.cronExpression).returns(Option(vcJobIds))

    (mockOrchestratorManager.killCronJobById _).expects(vcJobId1).returns(true)

    val mockScheduleManager = new MockScheduleManager
    assert(mockScheduleManager.deScheduleJob(localView, "c1"))

  }

  private def getViewInstance(clientIds: String, viewName: String): View = {
    val view = new View
    view.userName = "user"
    view.viewName = viewName
    view.jobComplexity = "3"
    view.jobStartTime = "1506726000000"
    view.jobEndTime = "1506726000000"
    view.cronExpression = "* * 0 * *"
    view.clientIds = clientIds

    view
  }

  private def getViewClientJobIDInstance(clientId: String, viewName: String): ViewClientJobId = {
    val vcJobID: ViewClientJobId = new ViewClientJobId();
    vcJobID.jobEndEpochTime = 1506726000000L
    vcJobID.clientId = clientId
    vcJobID.jobId = "1"
    vcJobID.jobStartEpochTime = 1506726000000L
    vcJobID.cronExpression = "* * 0 * *"
    vcJobID.viewName = viewName
    vcJobID
  }

  class MockScheduleManager extends ScheduleManager {
    override def getOrchestratorManager: OrchestratorManager = mockOrchestratorManager

    override def getViewService: ViewService = mockViewService

    override def getViewClientJobIDService = mockViewClientJobIdService

    override def getNextHourDate = nextHourDate

  }

}
