package com.tfs.orchestrator.services.http

import java.util
import java.util.Date

import com.tfs.orchestrator.hibernate.entities.View
import com.tfs.orchestrator.hibernate.service.ViewService
import com.tfs.orchestrator.managers.OrchestratorManager
import com.tfs.orchestrator.utils.{Constants, PropertyReader, RESOURCE_DEGREE}
import io.undertow.server.HttpServerExchange
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class ReplayServiceSpec extends FlatSpec with MockFactory {

  behavior of "ReplayService"

  it should "validate the params received in the service and return a single value" in {
    val service = new ReplayService
    //Mock exchange
    val exchange = new HttpServerExchange(null)

    //Mock the query params of the http exchange
    val values = new util.ArrayDeque[String]()
    values.add("hello")
    exchange.getQueryParameters().put("key", values)

    val responseValue = service.validateAndReturnValue("key", exchange, true)
    assert(responseValue.get.equals("hello"))
  }


  it should "validate the params received in the service and return None if no params available for " +
    "optional keys." in {
    val service = new MockReplayService
    //Mock exchange
    val exchange = new HttpServerExchange(null)

    val responseValue = service.validateAndReturnValue("key", exchange, true)
    assert(service.httpCode.equals(200))
    assert(!responseValue.isDefined)
  }


  it should "validate the params received in the service and set error code to 400 and if no params available for " +
    "non-optional keys." in {
    val service = new MockReplayService
    //Mock exchange
    val exchange = new HttpServerExchange(null)

    val responseValue = service.validateAndReturnValue("key", exchange, false)
    assert(service.httpCode.equals(400))
    assert(!responseValue.isDefined)
  }


  it should "validate the params received in the service and set error code to 400 and if duplicate params " +
    "available for non-optional keys." in {
    val service = new MockReplayService
    //Mock exchange
    val exchange = new HttpServerExchange(null)
    val values = new util.ArrayDeque[String]()
    values.add("hello")
    values.add("world")
    exchange.getQueryParameters().put("key", values)

    val responseValue = service.validateAndReturnValue("key", exchange, false)
    assert(service.httpCode.equals(400))
    assert(!responseValue.isDefined)
  }


  it should "validate the replay dates by checking they are in the past and start is older than end" in {
    val service = new MockReplayService
    val exchange = new HttpServerExchange(null)

    val startDate = new Date(System.currentTimeMillis() - 100000)
    val endDate = new Date(System.currentTimeMillis() - 10000)
    val dates = service.validateStartAndEndDate(startDate.getTime.toString, endDate.getTime.toString, exchange)
    assert(dates.isDefined)
    assert(dates.get._1.equals(startDate))
    assert(dates.get._2.equals(endDate))
  }


  it should "set error code to 400 if start date is newer than end date" in {
    val service = new MockReplayService
    val exchange = new HttpServerExchange(null)

    val endDate = new Date(System.currentTimeMillis() - 100000)
    val startDate = new Date(System.currentTimeMillis() - 10000)
    val dates = service.validateStartAndEndDate(startDate.getTime.toString, endDate.getTime.toString, exchange)
    assert(!dates.isDefined)
    assert(service.httpCode.equals(400))
  }


  it should "set error code to 400 if start date is newer than current time" in {
    val service = new MockReplayService
    val exchange = new HttpServerExchange(null)

    val endDate = new Date(System.currentTimeMillis() + 100000)
    val startDate = new Date(System.currentTimeMillis() + 10000)
    val dates = service.validateStartAndEndDate(startDate.getTime.toString, endDate.getTime.toString, exchange)
    assert(!dates.isDefined)
    assert(service.httpCode.equals(400))
  }


  it should "set error code to 400 if end date is newer than current time" in {
    val service = new MockReplayService
    val exchange = new HttpServerExchange(null)

    val endDate = new Date(System.currentTimeMillis() + 100000)
    val startDate = new Date(System.currentTimeMillis() - 10000)
    val dates = service.validateStartAndEndDate(startDate.getTime.toString, endDate.getTime.toString, exchange)
    assert(!dates.isDefined)
    assert(service.httpCode.equals(400))
  }

  it should "derive JobSCheduleProperties from View definition and client name" in {
    val service = new ReplayService
    val startDate = new Date
    val endDate = new Date(System.currentTimeMillis() + 10000)
    val viewState = mockViewForSuccess

    val jobScheduleProps = service.getJobScheduleProperties(viewState, "hilton", startDate, endDate)
    assert(jobScheduleProps.clientIdentifier.equals("hilton"))
    assert(jobScheduleProps.jobCronExpression.equals(viewState.cronExpression))
    assert(jobScheduleProps.jobComplexity.equals(RESOURCE_DEGREE.MEDIUM))
    assert(jobScheduleProps.jobStartTime.equals(startDate))
    assert(jobScheduleProps.jobEndTime.equals(endDate))
  }

  it should "schedule the job and return the workflow id if everything is success" in {
    //Inject mocks for ReplayService dependents
    PropertyReader.initialize(false, "src/test/resources/properties")
    val service = new MockReplayService
    val orchestratorManager = mock[OrchestratorManager]
    val viewService = mock[ViewService]
    service.manager = orchestratorManager
    service.viewService = viewService

    //Set the expectations
    (viewService.getViewByNameAndClientId _).expects(*, *).returns(mockViewForSuccess)
    (orchestratorManager.createReplay _).expects(*).returns(Some("1234-5678"))

    service.handleRequest(mockExchangeForSuccess())
    assert(service.httpCode.equals(200))
    assert(service.msg.contains(""""job": "1234-5678""""))
  }

  it should "give 500 error if Orchestrator could not schedule a job on oozie" in {
    //Inject mocks for ReplayService dependents
    PropertyReader.initialize(false, "src/test/resources/properties")
    val service = new MockReplayService
    val orchestratorManager = mock[OrchestratorManager]
    val viewService = mock[ViewService]
    service.manager = orchestratorManager
    service.viewService = viewService

    //Set the expectations
    (viewService.getViewByNameAndClientId _).expects(*, *).returns(mockViewForSuccess)
    (orchestratorManager.createReplay _).expects(*).returns(None)

    service.handleRequest(mockExchangeForSuccess())
    assert(service.httpCode.equals(500))
    assert(service.msg.contains(""""status""""))
  }

  /**
   *
   * @return
   */
  def mockExchangeForSuccess(): HttpServerExchange = {
    //Mock the query params of the http exchange
    val startDate = new Date(System.currentTimeMillis() - 100000)
    val endDate = new Date(System.currentTimeMillis() - 10000)

    val clients = new util.ArrayDeque[String]()
    clients.add("hilton")
    val views = new util.ArrayDeque[String]()
    views.add("billing")
    val datastarttimes = new util.ArrayDeque[String]()
    datastarttimes.add(startDate.getTime.toString)
    val dataendtimes = new util.ArrayDeque[String]()
    dataendtimes.add(endDate.getTime.toString)

    val exchange = new HttpServerExchange(null)
    exchange.getQueryParameters().put("client", clients)
    exchange.getQueryParameters().put("view", clients)
    exchange.getQueryParameters().put("datastarttime", datastarttimes)
    exchange.getQueryParameters().put("dataendtime", dataendtimes)
    return exchange
  }

  def mockViewForSuccess: View = {
    val billingView = new View
    billingView.viewName = "billing"
    billingView.cronExpression = "* 0 0 0 0"
    billingView.userName = "tfs"
    billingView
  }


  class MockReplayService extends ReplayService {
    var httpCode = 200
    var msg = "Good"
    var manager = new OrchestratorManager
    var viewService: ViewService = null

    override def sendResponse(httpServerExchange: HttpServerExchange, errorCode: Int, msg: String): Unit = {
      this.httpCode = errorCode
      this.msg = msg
    }

    override def getOrchestratorManager() = {
      manager
    }

    override def getViewService() = {
      viewService
    }
  }

}
