package com.tfs.orchestrator.utils.handlers

import com.tfs.orchestrator.managers.OrchestratorManager
import com.tfs.orchestrator.properties.JobScheduleProperties
import com.tfs.orchestrator.tasks.ExporterPublish.{ExporterConfigs, ExporterDateDetails, ExporterDetails}
import com.tfs.orchestrator.utils.WorkflowType
import com.tfs.orchestrator.utils.kafka.KafkaReader
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

class ExporterJobHandlerTests extends FunSuite with BeforeAndAfter with MockitoSugar {

  private val kafkaReader = mock[KafkaReader]
  private val orchestratorManager = mock[OrchestratorManager]
  private val jobScheduleProps = mock[JobScheduleProperties]
  private val validKafkaMsg = "{\"clientName\":\"hilton\",\"viewName\":\"Digital_Test9\",\"wfId\":\"wfId\",\"userName\":\"Insights\",\"replayTask\":false,\"exporterDateDetails\":{\"dataStartTime\":1541934000000,\"dataEndTime\":1541937600000,\"ucInstanceDate\":\"2018-11-11T11:00+0000\"},\"exporterConfigs\":{\"queue\":\"LongRun\",\"ingestionType\":\"ALL\",\"exporterPluginJarLoc\":\"\"}}"
  private val invalidKafkaMsg = "{\"hilton\",\"viewName\":\"Digital_Test9\",\"wfId\":\"wfId\",\"userName\":\"Insights\",\"replayTask\":false,\"exporterDateDetails\":{\"dataStartTime\":1541934000000,\"dataEndTime\":1541937600000,\"ucInstanceDate\":\"2018-11-11T11:00+0000\"},\"exporterConfigs\":{\"queue\":\"LongRun\",\"ingestionType\":\"ALL\",\"exporterPluginJarLoc\":\"\"}}"
  private val validButFieldMissingKafkaMsg = "{\"viewName\":\"Digital_Test9\",\"wfId\":\"wfId\",\"userName\":\"Insights\",\"replayTask\":false,\"exporterDateDetails\":{\"dataStartTime\":1541934000000,\"dataEndTime\":1541937600000,\"ucInstanceDate\":\"2018-11-11T11:00+0000\"},\"exporterConfigs\":{\"queue\":\"LongRun\",\"ingestionType\":\"ALL\",\"exporterPluginJarLoc\":\"\"}}"

  test("Handle exports success case") {
    var testVariable: Int = 0
    val exporterJobHandler = new ExporterJobHandlerMock {

      override def submitExporterJob(kafkaMsg: String) = {
        testVariable = testVariable + 1
      }
    }

    when(kafkaReader.hasNext).thenReturn(true).thenReturn(true).thenReturn(false)
    when(kafkaReader.next()).thenReturn("Message")
    when(kafkaReader.commit).thenReturn(true)
    exporterJobHandler.handleExports()
    assert(testVariable == 2)

  }

  test("Handle exports success case but interrupted") {
    var testVariable: Int = 0
    val exporterJobHandler = new ExporterJobHandlerMock {

      override def submitExporterJob(kafkaMsg: String) = {
        testVariable = testVariable + 1
        this.interrupt
      }
    }

    when(kafkaReader.hasNext).thenReturn(true)
    when(kafkaReader.next()).thenReturn("Message")
    when(kafkaReader.commit).thenReturn(true)
    exporterJobHandler.handleExports()
    assert(testVariable == 1)

  }

  test("already interrupted") {
    var testVariable: Int = 0
    val exporterJobHandler = new ExporterJobHandlerMock {
    }
    exporterJobHandler.interrupt

  }


  test("submission job successful case") {
    val exporterJobHandler = new ExporterJobHandlerMock {
      override def submitWorkflow(exporterDetails: ExporterDetails): Boolean = {
        assert(exporterDetails != null)
        true
      }
    }
    exporterJobHandler.submitExporterJob(validKafkaMsg)
  }

  test("submission job failed due to invalid json") {
    val exporterJobHandler = new ExporterJobHandlerMock {
      override def submitWorkflow(exporterDetails: ExporterDetails): Boolean = {
        assert(false)
        true
      }
    }
    exporterJobHandler.submitExporterJob(invalidKafkaMsg)
  }

  test("submission job failed due to missing fields") {
    val exporterJobHandler = new ExporterJobHandlerMock {
      override def submitWorkflow(exporterDetails: ExporterDetails): Boolean = {
        assert(false)
        true
      }
    }
    exporterJobHandler.submitExporterJob(invalidKafkaMsg)
  }

  test("Successful submission of workflow") {
    val exporterJobHandler = new ExporterJobHandlerMock{
      override def getJobScheduleProperties(viewName: String, clientName: String, userName: String, ucInstanceDate: String, replayTask: Boolean, wfType: WorkflowType.Value, externalId: String): JobScheduleProperties = {
        jobScheduleProps
      }
    }
    when(orchestratorManager.scheduleOnce(jobScheduleProps)).thenReturn(Option("1"))
    assert(exporterJobHandler.submitWorkflow(new ExporterDetails("C", "W", "wf", "XYZ", false, new ExporterDateDetails(100l,100l,"date"), new ExporterConfigs("","",""))))
  }

  test("Submission of workflow fails") {
    val exporterJobHandler = new ExporterJobHandlerMock{
      override def getJobScheduleProperties(viewName: String, clientName: String, userName: String, ucInstanceDate: String, replayTask: Boolean, wfType: WorkflowType.Value, externalId: String): JobScheduleProperties = {
        jobScheduleProps
      }
    }
    when(orchestratorManager.scheduleOnce(jobScheduleProps)).thenThrow(new RuntimeException)
    assert(!exporterJobHandler.submitWorkflow(new ExporterDetails("C", "W", "wf", "XYZ", false, new ExporterDateDetails(100l,100l,"date"), new ExporterConfigs("","",""))))
  }


  class ExporterJobHandlerMock extends ExporterJobHandler {

    override def getKafkaReaderInstance = {
      kafkaReader
    }

    override def getOrchestratorManagerInstance = {
      orchestratorManager
    }

    override def submitExporterJob(kafkaMsg: String): Unit = {
      super.submitExporterJob(kafkaMsg)
    }

    override def submitWorkflow(exporterDetails: ExporterDetails): Boolean = {
      super.submitWorkflow(exporterDetails)
    }

    override def getJobScheduleProperties(viewName: String, clientName: String, userName: String, ucInstanceDate: String, replayTask: Boolean, wfType: WorkflowType.Value, externalId: String): JobScheduleProperties = {
      super.getJobScheduleProperties(viewName, clientName, userName, ucInstanceDate, replayTask, wfType, externalId)
    }

  }


}
