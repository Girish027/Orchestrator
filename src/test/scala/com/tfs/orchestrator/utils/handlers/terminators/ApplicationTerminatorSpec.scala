package com.tfs.orchestrator.utils.handlers.terminators

import com.tfs.orchestrator.utils.PropertyReader
import org.apache.http.HttpStatus
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer
import scalaj.http.HttpResponse

class ApplicationTerminatorSpec extends FlatSpec {
  behavior of "ApplicationTerminator"

  it should "terminate applications for the workflows with no successor" in {
    PropertyReader.initialize(false, "src/test/resources/properties")
    val impl = new ApplicationTerminatorImpl {

      override def killApplications(workflowIds: ListBuffer[String], applicationIds: Map[String, String]) = {
        println("Applications killed")
      }

      override def fetchYarnApplicationIdMappedToWorkflowIds(workflowIds: ListBuffer[String]): Map[String, String] = {
        println("Returning a map of wokflow ids to application ids")
        Map()
      }
    }
    val workflowIds = ListBuffer("0001804-181211013141461-oozie-oozi-W", "0001805-181211013141461-oozie-oozi-W")
    impl.terminate(workflowIds)
  }


  it should "terminate applications for the workflows with successor" in {
    val impl = new ApplicationTerminatorImpl {

      override def killApplications(workflowIds: ListBuffer[String], applicationIds: Map[String, String]) = {
        println("Applications killed")
      }

      override def fetchYarnApplicationIdMappedToWorkflowIds(workflowIds: ListBuffer[String]): Map[String, String] = {
        println("Returning a map of wokflow ids to application ids")
        Map()
      }
    }

    val successor = new ApplicationTerminatorImpl {
      override def terminate(workflowIds: ListBuffer[String]): Unit = {
        println("Terminating applications for successor")
      }
    }

    val workflowIds = ListBuffer("0001804-181211013141461-oozie-oozi-W", "0001805-181211013141461-oozie-oozi-W")
    impl.successor = Option.apply(successor)
    impl.terminate(workflowIds)
  }


  it should "kill list of eligible applications and remove it from the input list" in {
    val impl = new ApplicationTerminatorImpl {
      override def killApplication(applicationId: String): Boolean = {
        val runningApplications = List("application_1544439888269_16908", "application_1544439888269_16909")
        if (runningApplications.contains(applicationId)) {
          return true
        }
        false
      }
    }

    val workflowIds = ListBuffer("0001804-181211013141461-oozie-oozi-W", "0001805-181211013141461-oozie-oozi-W", "0001806-181211013141461-oozie-oozi-W")
    var wfAppIds: Map[String, String] = Map()
    wfAppIds += ("0001804-181211013141461-oozie-oozi-W" -> "application_1544439888269_16908")
    wfAppIds += ("0001805-181211013141461-oozie-oozi-W" -> "application_1544439888269_16909")
    wfAppIds += ("0001806-181211013141461-oozie-oozi-W" -> "application_1544439888269_16910")

    impl.killApplications(workflowIds, wfAppIds)
    assert(workflowIds.contains("0001806-181211013141461-oozie-oozi-W"))
  }

  it should "submit yarn to kill for a running application and return state as successfull" in {
    val impl = new ApplicationTerminatorImpl {
      override protected def submitKillToYarn(applicationId: String): HttpResponse[String] = {
        new HttpResponse[String]("", HttpStatus.SC_OK, Map())
      }
    }
    assert(impl.killApplication("application_1544439888269_16920"))
  }

  it should "submit yarn to kill for a done application and return state as KILLED" in {
    val impl = new ApplicationTerminatorImpl {
      override protected def submitKillToYarn(applicationId: String): HttpResponse[String] = {
        new HttpResponse[String]("", HttpStatus.SC_BAD_REQUEST, Map())
      }
    }
    assert(!impl.killApplication("application_1544439888269_16920"))
  }

  it should "retrieve all running yarn applications mapped to workflow" in {
    val impl = new ApplicationTerminatorImpl {
      override def fetchYarnApplicationId(workflowId: String, url: String): Option[String] = {
        var wfAppIds: Map[String, String] = Map()
        wfAppIds += ("0001804-181211013141461-oozie-oozi-W" -> "application_1544439888269_16908")
        wfAppIds += ("0001805-181211013141461-oozie-oozi-W" -> "application_1544439888269_16909")
        val yarnAppID = wfAppIds.get(workflowId)
        if (yarnAppID != null) {
          return yarnAppID
        }
        None
      }
    }
    val workflowIds = ListBuffer("0001804-181211013141461-oozie-oozi-W", "0001805-181211013141461-oozie-oozi-W", "0001806-181211013141461-oozie-oozi-W")

    val appToWfIdMap = impl.fetchYarnApplicationIdMappedToWorkflowIds(workflowIds)
    assert(appToWfIdMap.get("0001804-181211013141461-oozie-oozi-W").nonEmpty)
    assert(appToWfIdMap.get("0001805-181211013141461-oozie-oozi-W").nonEmpty)
    assert(!appToWfIdMap.get("0001806-181211013141461-oozie-oozi-W").nonEmpty)
  }


}


class ApplicationTerminatorImpl extends ApplicationTerminator {
  override def filterRunningApplications(a: Nothing): Option[String] = {
    None
  }

  override def killApplications(workflowIds: ListBuffer[String], wfAppIds: Map[String, String]): Unit = {
    super.killApplications(workflowIds, wfAppIds)
  }

  override def killApplication(applicationId: String): Boolean = {
    super.killApplication(applicationId)
  }

  override def fetchYarnApplicationIdMappedToWorkflowIds(workflowIds: ListBuffer[String]): Map[String, String] = {
    super.fetchYarnApplicationIdMappedToWorkflowIds(workflowIds)
  }

}
