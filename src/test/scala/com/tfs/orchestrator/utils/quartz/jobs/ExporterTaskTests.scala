package com.tfs.orchestrator.utils.quartz.jobs

import com.tfs.orchestrator.utils.handlers.ExporterJobHandler
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

class ExporterTaskTests extends FunSuite with BeforeAndAfter with MockitoSugar {

  private val exporterJobHandler = mock[ExporterJobHandler]

  test("Exporter job handler is defined") {
    val exporterTask = new ExporterTask {
      override protected def getExporterJobHandlerInstance = {
        Option(exporterJobHandler)
      }
    }
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(2000l)
        exporterTask.interrupt()
      }
    }).start()
    doNothing().when(exporterJobHandler).handleExports()
    exporterTask.execute(null)
  }

  test("Exporter job handler is not defined due to exception") {
    val exporterTask = new ExporterTask {
      override protected def getExporterJobHandlerInstance = {
        throw new RuntimeException
      }
    }
    exporterTask.execute(null)
    exporterTask.interrupt()
  }

  test("Exporter job handler is defined and throws exception") {
    val exporterTask = new ExporterTask {
      override protected def getExporterJobHandlerInstance = {
        Option(exporterJobHandler)
      }
    }
    when(exporterJobHandler.handleExports()).thenThrow(new RuntimeException)
    exporterTask.execute(null)
  }


}
