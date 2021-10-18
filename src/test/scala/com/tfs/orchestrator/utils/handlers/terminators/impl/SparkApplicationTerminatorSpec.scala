package com.tfs.orchestrator.utils.handlers.terminators.impl

import java.util.Date

import com.tfs.orchestrator.utils.{DateUtils, PropertyReader}
import net.liftweb.json.{DefaultFormats, parse}
import org.scalatest.FlatSpec


class SparkApplicationTerminatorSpec extends FlatSpec {
  behavior of "SparkApplicationTerminator"
  PropertyReader.initialize(false, "src/test/resources/properties")

  it should "return running yarn application id" in {
    //TODO with real string
    val app = new App("application_1544779815530_22518", "yarn", "RUNNING")
    var apps = List[App](app)
    val yarnResponse = new YarnResponse(Option[Apps](new Apps(apps)))
    val impl = new SparkApplicationTerminator
    val appId = impl.filterRunningApplications(yarnResponse)
    assert(appId.nonEmpty)
    assert(appId.get.equals("application_1544779815530_22518"))
  }

  it should "should return empty" in {
    val app = new App("application_1544779815530_22518", "yarn", "KILLED")
    var apps = List[App](app)
    val yarnResponse = new YarnResponse(Option[Apps](new Apps(apps)))
    val impl = new SparkApplicationTerminator
    val appId = impl.filterRunningApplications(yarnResponse)
    assert(appId.isEmpty)
  }
}
