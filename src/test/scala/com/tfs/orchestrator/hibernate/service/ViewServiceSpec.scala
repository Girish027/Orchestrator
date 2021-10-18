package com.tfs.orchestrator.hibernate.service

import java.util

import com.tfs.orchestrator.hibernate.entities.View
import com.tfs.orchestrator.hibernate.utils.HibernateUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class ViewServiceSpec extends FlatSpec with MockFactory{
    behavior of "ViewService"

  it should "return a view when queried by viewName" in {
    val utils = mock[HibernateUtils]
    val mockEntity = new View
    mockEntity.viewName = "View_DCF"
    val mockEntityList = new util.ArrayList[View]
    mockEntityList.add(mockEntity)
    (utils.executeSelect _).expects(*).returns(mockEntityList)
    val viewService = new ViewService(utils)
    val view = viewService.getViewByNameAndCron("View_DCF", "0 * * * *")
    assert("View_DCF" == view(0).viewName)
  }


  it should "return None when queried if the table is empty" in {
    val utils = mock[HibernateUtils]
    val mockEntity = new View
    mockEntity.viewName = "View_DCF"
    val mockEntityList = new util.ArrayList[View]
    (utils.executeSelect _).expects(*).returns(mockEntityList)
    val viewService = new ViewService(utils)
    val view = viewService.getViewByNameAndCron("View_DCF", "0 * * * *")
    assert(null == view)
  }

  it should "invoke hibernateUtils to create an entry" in {
    val utils = mock[HibernateUtils]
    (utils.executeCreate _).expects(*).once()

    val service = new ViewService(utils)
    service.saveView(new View())
  }

  it should "invoke hibernatUtils to update an entry" in {

    val utils = mock[HibernateUtils]
    val mockEntity = new View()
    mockEntity.viewName = "View_DCF"
    mockEntity.jobStartTime = "1506726000000"
    mockEntity.jobEndTime = "1506726000000"

    (utils.executeUpdate _).expects(*).once()

    val service = new ViewService(utils)
    service.updateView(mockEntity)
  }

  it should "return list of views when getAllViews are called" in {
    val utils = mock[HibernateUtils]
    val viewService = new ViewService(utils)
    var view1 = new View
    view1.userName = "user"
    view1.viewName = "view1"
    view1.jobComplexity = "3"
    view1.jobStartTime = "1506726000000"
    view1.jobEndTime = "1506726000000"
    view1.cronExpression = "* * 0 * *"
    view1.clientIds = "c1,c2"

    var view2 = new View
    view2.userName = "user"
    view2.viewName = "view2"
    view2.jobComplexity = "3"
    view2.jobStartTime = "1506726000000"
    view2.jobEndTime = "1506726000000"
    view2.cronExpression = "abc"
    view2.clientIds = "c1"

    val javaList = new util.ArrayList[View]()
    javaList.add(view1)
    javaList.add(view2)
    (utils.executeSelect _).expects(*).returns(javaList)
    assert(viewService.getAllViews() == List(view1, view2))
  }

}
