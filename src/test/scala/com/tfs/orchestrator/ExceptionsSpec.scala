package com.tfs.orchestrator

import org.scalatest.FlatSpec

class ExceptionsSpec extends FlatSpec {


 /* PropertyReader.initialize(false, "src/test/resources/properties")
  "HibernateException" should "set the message and cause appropriately" in {
    val cause = new Exception
    val hibernateEx = new HibernateException(cause, "random message")
    assert(hibernateEx.getCause.equals(cause))
    assert(hibernateEx.getMessage.equals("random message"))
  }

  "ScheduleException" should "set the message and cause appropriately" in {
    val cause = new Exception
    val scheduleEx = new ScheduleException(cause, "random message")
    assert(scheduleEx.getCause.equals(cause))
    assert(scheduleEx.getMessage.equals("random message"))
  }

  "InitializationException" should "set the message and cause appropriately" in {
    val cause = new Exception
    val initEx = new InitializationException(cause, "random message")
    assert(initEx.getCause.equals(cause))
    assert(initEx.getMessage.equals("random message"))
  }

  "InitializationException" should "set the cause as null" in {
    val initEx = new InitializationException("random message")
    assert(initEx.getCause == null)
    assert(initEx.getMessage.equals("random message"))
  }

  "Test" should "test" in {
    val exporterDateDetails = ExporterDateDetails(1541934000000L,1541937600000L,"2018-11-11T11:00+0000")
   val exporterConfigs = ExporterConfigs("LongRun","ALL","")
   val exporterDetails = ExporterDetails("hilton","Digital_Test22","wfId","Insights",false,exporterDateDetails,exporterConfigs)
   implicit val formats = DefaultFormats
    val jsonString = write(exporterDetails)
    print(jsonString)
   val exporterJobHandler = new ExporterJobHandler
    exporterJobHandler.handleExports()
    //JobInitializer.initialize()
    Thread.sleep(1000000l)

  }*/
}
