package com.tfs.orchestrator.hibernate.service

import java.util

import com.tfs.orchestrator.hibernate.entities.ViewClientJobId
import com.tfs.orchestrator.hibernate.utils.HibernateUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class ViewClientJobIdServiceTest extends FlatSpec with MockFactory {
  behavior of "ViewClientJobIdService"

  val utils = mock[HibernateUtils]
  it should "invoke the HibernateUtils to query the entity" in {
    val utils = mock[HibernateUtils]
    val mockReturnValue = new java.util.ArrayList[ViewClientJobId]()
    val mockEntity = new ViewClientJobId()
    mockEntity.jobId = "1234-5678"
    mockReturnValue.add(mockEntity)

    //Set the expectation
    (utils.executeSelect _).expects(*).returns(mockReturnValue)

    //Make the calls
    val service = new ViewClientJobIdService(utils)
    val jobId = service.getJobId("viesw", "hilton", "0 * * * *")
    assert("1234-5678".equals(jobId.get))
  }



  it should "should return None for getJobId if no valid entity available" in {
    val utils = mock[HibernateUtils]
    val mockReturnValue = new java.util.ArrayList[ViewClientJobId]()

    //Set the expectation
    (utils.executeSelect _).expects(*).returns(mockReturnValue)
    (utils.executeSelect _).expects(*).returns(mockReturnValue)

    //Make the calls
    val service = new ViewClientJobIdService(utils)
    val jobId = service.getJobId("viesw", "hilton", "0 * * * *")
    assert(None.equals(jobId))
  }



  it should "invoke HibernateUtils to save the entity" in {
    val utils = mock[HibernateUtils]

    //Set the expectation
    (utils.executeCreate _).expects(*).once()

    //Make the calls
    val service = new ViewClientJobIdService(utils)
    service.saveViewClientJobId(new ViewClientJobId())
  }

  it should "return list of existing client ids" in {

    val service = new ViewClientJobIdService(utils)
    val viewName = "test"
    val clientIds = List("c1","c2")
    val vcJobId1 = getViewClientJobIDInstance("c1",viewName)
    val vcJobId2 = getViewClientJobIDInstance("c2",viewName)
    val vcJobList:java.util.List[_] =  java.util.Arrays.asList(vcJobId1,vcJobId2)
    (utils.executeSelect _).expects(*).returns(vcJobList)
    val output = service.getClientIdsForExistingJobs(viewName,clientIds)
    assert(output.isDefined)
    assert(output.get.contains("c1") && output.get.contains("c2"))
  }


  it should "return none if no existing client ids exists" in {

    val service = new ViewClientJobIdService(utils)
    val viewName = "test"
    val clientIds = List("c1","c2")
    val vcJobList:java.util.List[_] =  java.util.Arrays.asList()
    (utils.executeSelect _).expects(*).returns(vcJobList)
    val output = service.getClientIdsForExistingJobs(viewName,clientIds)
    assert(!output.isDefined)
  }


  it should "return list of existing view client job ids for a single client" in {

    val service = new ViewClientJobIdService(utils)
    val viewName = "test"
    val clientId ="c1"
    val cronExpression="0 * * * *"
    val vcJobId1 = getViewClientJobIDInstance("c1",viewName)
    val vcJobId2 = getViewClientJobIDInstance("c2",viewName)
    val vcJobList:java.util.List[_] =  java.util.Arrays.asList(vcJobId1,vcJobId2)
    (utils.executeSelect _).expects(*).returns(vcJobList)
    val output = service.getViewClientJobIds(viewName,clientId,cronExpression)
    assert(output.isDefined)
    assert(output.get.head.clientId.equals("c1"))
  }


  it should "return none if no existing view client job ids exists for a single client" in {

    val service = new ViewClientJobIdService(utils)
    val viewName = "test"
    val clientId ="c1"
    val cronExpression="0 * * * *"
    val vcJobList:java.util.List[_] =  java.util.Arrays.asList()
    (utils.executeSelect _).expects(*).returns(vcJobList)
    val output = service.getViewClientJobIds(viewName,clientId,cronExpression)
    assert(!output.isDefined)
  }


  it should "return list of existing view client job ids for a list of clients" in {

    val service = new ViewClientJobIdService(utils)
    val viewName = "test"
    val clientIds = List("c1","c2")
    val cronExpression="0 * * * *"
    val vcJobId1 = getViewClientJobIDInstance("c1",viewName)
    val vcJobId2 = getViewClientJobIDInstance("c2",viewName)
    val vcJobList:java.util.List[_] =  java.util.Arrays.asList(vcJobId1,vcJobId2)
    (utils.executeSelect _).expects(*).returns(vcJobList)
    val output = service.getViewClientJobIds(viewName,clientIds,cronExpression)
    assert(output.isDefined)
    assert(output.get.head.clientId.equals("c1"))
  }


  it should "return none if no existing view client job ids exists for a list of clients" in {

    val service = new ViewClientJobIdService(utils)
    val viewName = "test"
    val clientIds = List("c1","c2")
    val cronExpression="0 * * * *"
    val vcJobList:java.util.List[_] =  java.util.Arrays.asList()
    (utils.executeSelect _).expects(*).returns(vcJobList)
    val output = service.getViewClientJobIds(viewName,clientIds,cronExpression)
    assert(!output.isDefined)
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

}
