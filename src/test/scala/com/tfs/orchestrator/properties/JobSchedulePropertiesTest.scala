package com.tfs.orchestrator.properties

import com.tfs.orchestrator.exceptions.InvalidParameterException
import com.tfs.orchestrator.utils.{RESOURCE_DEGREE, Constants}
import org.scalatest.FlatSpec

class JobSchedulePropertiesTest extends FlatSpec {
  behavior of "JobScheduleProperties"

  it should "provide default values" in {
    val jobSchedProps = JobScheduleProperties("hilton", "aiva", "oozie")
    assert(jobSchedProps.jobStartTime != null)
    assert(jobSchedProps.jobEndTime != null)
    val timeDiff = jobSchedProps.jobEndTime.getTime - jobSchedProps.jobStartTime.getTime
    assert(timeDiff == (Constants.DEFAULT_JOB_END_DURATION))
    assert(jobSchedProps.jobComplexity.equals(RESOURCE_DEGREE.LOW))
    assert(jobSchedProps.clientIdentifier.equals("hilton"))
    assert(jobSchedProps.viewName.equals("aiva"))
    assert(jobSchedProps.userName.equals("oozie"))
  }

  it should "throw an exception if clientId is null" in {
    assertThrows[InvalidParameterException](JobScheduleProperties(null, "dummy", "default"))
  }

  it should "throw an exception if viewName is null" in {
    assertThrows[InvalidParameterException](JobScheduleProperties("dummy", null, "default"))
  }

  it should "throw an exception if user name is null" in {
    assertThrows[InvalidParameterException](JobScheduleProperties("dummy", "dummy", null))
  }
}
