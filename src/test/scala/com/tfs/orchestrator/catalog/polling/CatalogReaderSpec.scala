package com.tfs.orchestrator.catalog.polling

import java.util.Date

import com.tfs.orchestrator.hibernate.entities.{ViewClientJobId, _}
import com.tfs.orchestrator.hibernate.service.{ViewClientJobIdService, ViewService}
import com.tfs.orchestrator.managers.{OrchestratorManager, ScheduleManager}
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.utils.{Constants, DateUtils, PropertyReader, RESOURCE_DEGREE}
import net.liftweb.json.{DefaultFormats, parse}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer
import scala.io.Source


class CatalogReaderSpec extends FlatSpec with MockFactory {

  PropertyReader.initialize(false, "src/test/resources/properties")
  val catalogReader = new CatalogReader("", 1235)
  val mockScheduleManager = mock[ScheduleManager]
  val mockViewService = mock[ViewService]


  it should "create a view entity out of ViewScheduler inputs" in {
    val execProp: Map[String,String] = Map[String,String]("queue"->"abc")
    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("c1",execProp,None,"150670","150672")
    val clientExecutionProp2 : ClientExecutionProperties = ClientExecutionProperties("c2",execProp,None,null,null)
    val clientExec = ListBuffer[ClientExecutionProperties]()
    clientExec += clientExecutionProp1
    clientExec += clientExecutionProp2
    val viewSchedule = ViewSchedule("ViewSW", List(ClientsCronExpression(clientExec.toList, "* * 1 * *")), "oozie", "1506726000000", "1506726000000", "LOW")

    var expectedViews = List.empty[View]

    viewSchedule.clientCronExpressionList foreach(clientsCron => clientsCron.clientExecProps foreach (client => {
      val expected = new View
      expected.viewName = viewSchedule.viewName
      expected.clientIds = client.name
      if(client.jobStartTime!=null){
        expected.jobStartTime=client.jobStartTime
      }
      else {
        expected.jobStartTime = viewSchedule.jobStartTime
      }
      if(client.jobEndTime!=null){
        expected.jobEndTime=client.jobEndTime
      }
      else{
        expected.jobEndTime = viewSchedule.jobEndTime
      }
      expected.jobComplexity = viewSchedule.complexity
      expected.cronExpression = viewSchedule.clientCronExpressionList(0).cronExpression
      expected.userName = viewSchedule.userName
      expectedViews::=expected
    }))


    val actualViews = catalogReader.createViewEntity(viewSchedule, viewSchedule.clientCronExpressionList(0))

    for(i <- 0 to 1){
      expectedViews(i).id=actualViews(i).id
      assert(expectedViews(i).id==actualViews(i).id)
      assert(expectedViews(i).jobStartTime == actualViews(i).jobStartTime)
      assert(expectedViews(i).jobEndTime == actualViews(i).jobEndTime)

    }
  }



  it should "read the output from catalog service and convert to list of schedules" in {
    var execProp: Map[String,String] = Map[String,String]("queue"->"cv_test")
    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("siriusxm",execProp,None,null,null)
    val clientExecutionProp2 : ClientExecutionProperties = ClientExecutionProperties("dish",Map[String,String](),None,null,null)
    val clientExec1 = ListBuffer[ClientExecutionProperties]()
    clientExec1 += clientExecutionProp1
    clientExec1 += clientExecutionProp2
    val clientExec2 = ListBuffer[ClientExecutionProperties]()
    clientExec2 += clientExecutionProp2
    val catalogSnapshot = "[{\"viewName\":\"DQ_On_RawIDM\",\"clientCronExpressionList\":[{\"clientExecProps\":[{\"executionProperties\":{\"queue\":\"cv_test\"},\"name\":\"siriusxm\",\"jobStartTime\":null,\"jobEndTime\":null},{\"executionProperties\":{},\"name\":\"dish\",\"jobStartTime\":null,\"jobEndTime\":null}],\"cronExpression\":\"0 * * * *\"}],\"userName\":\"sys\",\"jobStartTime\":\"1504742400000\",\"jobEndTime\":\"1504828800000\",\"complexity\":\"2\"},{\"viewName\":\"ViewSW\",\"clientCronExpressionList\":[{\"clientExecProps\":[{\"executionProperties\":{},\"name\":\"dish\",\"jobStartTime\":null,\"jobEndTime\":null}],\"cronExpression\":\"0 * * * *\"}],\"userName\":\"sys\",\"jobStartTime\":\"1501545600000\",\"jobEndTime\":\"1504224000000\",\"complexity\":\"2\"}]"
    //val catalogSnapshot = Source.fromFile("src/main/resources/samples/catalogsnapshot.txt").getLines.mkString
    val view1 = new ViewSchedule("DQ_On_RawIDM", List(ClientsCronExpression(clientExec1.toList, "0 * * * *")), "sys", "1504742400000", "1504828800000", "2")
    val view2 = new ViewSchedule("ViewSW", List(ClientsCronExpression(clientExec2.toList, "0 * * * *")), "sys", "1501545600000", "1504224000000", "2")


    val expected = List(view1, view2)
    val actual = catalogReader.getViewSchedulesFromJson(catalogSnapshot)

    assert(expected.equals(actual))
  }


  it should "schedule new job for a client moving from one cron expression bucket to a another and deschedule the old job" in {

    var targetViews=List[View]()
    val targetView = new View
    targetView.userName = "user"
    targetView.viewName = "test"
    targetView.jobComplexity = "3"
    targetView.jobStartTime = "1506726000000"
    targetView.jobEndTime = "1506726000000"
    targetView.cronExpression = "* * 0 * *"
    targetView.clientIds = "c1,c2"
    targetViews::=targetView

    var localViews=List[View]()
    var localView = new View
    localView.userName = "user"
    localView.viewName = "test"
    localView.jobComplexity = "3"
    localView.jobStartTime = "1506726000000"
    localView.jobEndTime = "1506726000000"
    localView.cronExpression = "abc"
    localView.clientIds = "c1"
    localViews::=localView

    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("c1",Map[String,String](),None,null,null)
    val clientExecutionProp2 : ClientExecutionProperties = ClientExecutionProperties("c2",Map[String,String](),None,null,null)
    val clientExec = ListBuffer[ClientExecutionProperties]()
    clientExec += clientExecutionProp1
    clientExec += clientExecutionProp2
    val clientsCron = ClientsCronExpression(clientExec.toList, "* * 0 * *")
    val viewSchedule = ViewSchedule("test", List(clientsCron), "user", "1506726000000", "1506726000000", "3")
    val existingClientIds = List ("c1")
    val viewSchedules = List(viewSchedule)
    val mockCatalogReader = new MockCatalogReader(mockScheduleManager) {
      override def createViewEntity(viewSchedule: ViewSchedule, clientsCron: ClientsCronExpression): List[View] = targetViews

      override def getExistingClientIds(localView: View): List[String] = existingClientIds

    }
    var allClients = List[String]()
    clientsCron.clientExecProps foreach(client => allClients = client.name :: allClients)

    val deletedClients: List[String] = existingClientIds.filterNot(allClients.contains(_))
    val newlyAddedClients: List[String] = allClients.filterNot(existingClientIds.contains(_))

    (mockViewService.getViewByNameAndCron _).expects(targetView.viewName, clientsCron.cronExpression).returns(localViews)
    (mockScheduleManager.scheduleNewClients _).expects(targetView,newlyAddedClients)
    (mockScheduleManager.deScheduleDeletedClients _).expects(localView, deletedClients)
    (mockScheduleManager.descheduleNonExistentCrons _).expects(viewSchedule)
    (mockScheduleManager.deleteNonExistentViews _).expects(viewSchedules)

    mockCatalogReader.updateOrchestrator(viewSchedules)
  }


  it should "filter Clients based on DataCenter" in {

    var envProp1: Map[String,String] = Map[String,String]("dataCenter"->"sv1")
    var envProp2: Map[String,String] = Map[String,String]("dataCenter"->"sv2")
    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("c1",Map[String,String](),Option(envProp1),null,null)
    val clientExecutionProp2 : ClientExecutionProperties = ClientExecutionProperties("c2",Map[String,String](),Option(envProp2),null,null)
    val clientExec = ListBuffer[ClientExecutionProperties]()
    clientExec += clientExecutionProp1
    clientExec += clientExecutionProp2
    val clientsCron = ClientsCronExpression(clientExec.toList, "* * 0 * *")
    val mockCatalogReader = new MockCatalogReader(mockScheduleManager) {
    }
    var filterClients = new FilterClientsByDataCenter
    val clients:List[String] = filterClients.applyFilter(clientsCron.clientExecProps,List[String]())
    assert(clients.size.equals(1))
    assert(clients(0).equals("c1"))
  }

  it should "filter Clients based comma separated values on DataCenter" in {

    var envProp1: Map[String,String] = Map[String,String]("dataCenter"->"sv1")
    var envProp2: Map[String,String] = Map[String,String]("dataCenter"->"sv2,sv1")
    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("c1",Map[String,String](),Option(envProp1),null,null)
    val clientExecutionProp2 : ClientExecutionProperties = ClientExecutionProperties("c2",Map[String,String](),Option(envProp2),null,null)
    val clientExec = ListBuffer[ClientExecutionProperties]()
    clientExec += clientExecutionProp1
    clientExec += clientExecutionProp2
    val clientsCron = ClientsCronExpression(clientExec.toList, "* * 0 * *")
    val mockCatalogReader = new MockCatalogReader(mockScheduleManager) {
    }
    var filterClients = new FilterClientsByDataCenter
    val clients:List[String] = filterClients.applyFilter(clientsCron.clientExecProps,List[String]())
    assert(clients.size.equals(2))
  }

  it should "valid if environment properties getting parsed properly " in {
    val jsonStr = "{\n        \"viewName\": \"View_ABCD\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"-1\",\n                            \"queue\": \"default\"\n                        },\n                        \"name\": \"marriott_marriott\",\"jobStartTime\":null,\"jobEndTime\":null,\n                        \"environmentProperties\":{\n                          \"dataCenter\":\"sv1\"\n                        }\n                    }\n                ],\n                \"cronExpression\": \"0 10 * * *\"\n            }\n        ],\n        \"userName\": \"Insights\",\n        \"jobStartTime\": \"1540684800000\",\n        \"jobEndTime\": \"1919376000000\",\n        \"complexity\": \"2\"\n    }"
    implicit val formats = DefaultFormats
    val viewSchedule = parse(jsonStr).extract[ViewSchedule]
    assert(viewSchedule.clientCronExpressionList(0).clientExecProps(0).environmentProperties.get("dataCenter").equals("sv1"))
  }

  it should "schedule new job for a new client cron expression" in {
    val targetView = new View
    targetView.userName = "user"
    targetView.viewName = "test"
    targetView.jobComplexity = "3"
    targetView.jobStartTime = "1506726000000"
    targetView.jobEndTime = "1506726000000"
    targetView.cronExpression = "* * 0 * *"
    targetView.clientIds = "c1"

    var targetViews=List[View]()
    targetViews::=targetView

    val clientExecutionProp1 : ClientExecutionProperties = ClientExecutionProperties("c1",Map[String,String](),None,null,null)
    val clientExec = ListBuffer[ClientExecutionProperties]()
    clientExec += clientExecutionProp1

    val clientsCron = ClientsCronExpression(clientExec.toList, "* * 0 * *")
    val viewSchedule = ViewSchedule("test", List(clientsCron), "user", "1506726000000", "1506726000000", "3")
    val existingClientIds = List[String]()
    val viewSchedules = List(viewSchedule)
    val mockCatalogReader = new MockCatalogReader(mockScheduleManager) {
      override def createViewEntity(viewSchedule: ViewSchedule, clientsCron: ClientsCronExpression): List[View] = targetViews

      override def getExistingClientIds(localView: View): List[String] = existingClientIds

    }

    var allClients = List[String]()
    clientsCron.clientExecProps foreach(client => allClients = client.name :: allClients)

    val newlyAddedClients: List[String] = allClients.filterNot(existingClientIds.contains(_))
    (mockViewService.getViewByNameAndCron _).expects(targetView.viewName, clientsCron.cronExpression).returns(null)
    (mockScheduleManager.scheduleNewClients _).expects(targetView, newlyAddedClients)

    mockCatalogReader.updateOrchestrator(viewSchedules)
  }


  class MockCatalogReader(mockScheduleManager: ScheduleManager)
    extends CatalogReader("localhost:8080", 5000) {
    override def getScheduleManager: ScheduleManager = mockScheduleManager
    override def getViewService: ViewService = mockViewService
  }

}