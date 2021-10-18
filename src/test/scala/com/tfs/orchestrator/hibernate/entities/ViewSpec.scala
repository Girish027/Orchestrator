package com.tfs.orchestrator.hibernate.entities

import org.scalatest.FlatSpec

class ViewSpec extends FlatSpec {

  it should "compare two objects" in {
    val view1 = new View
    view1.viewName = "viewName"
    view1.cronExpression = "* * 1 * *"
    view1.jobComplexity = "LOW"
    view1.userName = "userName"
    view1.jobStartTime = "1506726000000"
    view1.jobEndTime = "1506726000000"

    val view2 = new View()
    view2.viewName = "viewName"
    view2.cronExpression = "* * 1 * *"
    view2.jobComplexity = "LOW"
    view2.userName = "userName"
    view2.jobStartTime = "1506726000000"
    view2.jobEndTime = "1506726000000"

    assert(view1.compareWith(view2))
  }

  it should "return false when comparing two different objects" in {
    val view1 = new View
    view1.viewName = "viewName"
    view1.cronExpression = "* * 1 * *"
    view1.jobComplexity = "LOW"
    view1.userName = "userName"
    view1.jobStartTime = "1506726000000"
    view1.jobEndTime = "1506726000000"

    val view2 = new View()
    view2.viewName = "viewName_diff"
    view2.cronExpression = "* * 1 * *"
    view2.jobComplexity = "LOW"
    view2.userName = "userName"
    view2.jobStartTime = "1506726000000"
    view2.jobEndTime = "1506726000000"

    assert(view1.compareWith(view2))
  }


  it should "update one entity with the other entity" in {
    val view1 = new View
    view1.viewName = "viewName"
    view1.cronExpression = "* * 1 * *"
    view1.jobComplexity = "LOW"
    view1.userName = "userName"
    view1.jobStartTime = "1506726000000"
    view1.jobEndTime = "1506726000000"

    val view2 = new View()
    view2.viewName = "viewName"
    view2.cronExpression = "* * 1 * *"
    view2.jobComplexity = "LOW"
    view2.userName = "userName_updated"
    view2.jobStartTime = "1506726000000"
    view2.jobEndTime = "1506726000000"

    view1.updateView(view2)
    assert(view1.userName == "userName_updated")
  }

  it should "compare end date and return true as end date differs" in {
    val view1 = new View
    view1.jobEndTime = "1506726000000"
    val view2 = new View
    view2.jobEndTime = "1506526000000"

    assert(!view1.compareJobEndWith(view2))
  }

  it should "compare end date and return true as end date are same" in {
    val view1 = new View
    view1.jobEndTime = "1506726000000"
    val view2 = new View
    view2.jobEndTime = "1506726000000"

    assert(view1.compareJobEndWith(view2))
  }

}
