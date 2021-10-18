package com.tfs.orchestrator.utils.handlers

import com.tfs.orchestrator.utils.handlers.terminators.ApplicationTerminator
import com.tfs.orchestrator.utils.handlers.terminators.impl._
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.ListBuffer

object TerminationHandler extends Logging {
  /**
    * terminators and runningApplicationHandler needs to be initialized only once.
    */
  private lazy val terminators = Option[ApplicationTerminator[_ <: Response]](initializeTerminators)
  private lazy val runningApplicationHandler = new RunningApplicationHandler()

  /**
    * Triggers a call to fetch the running applications and terminate the applicable applications.
    */
  def terminateApplications(): Unit = {
    try {
      val workflowIds = runningApplicationHandler.fetchRunningApplications()
      terminators.get.terminate(workflowIds.to[ListBuffer])
    }
    catch{
      case ex: Exception => logger.error("Unable to terminate applications",ex)
    }
  }

  private def initializeTerminators: ApplicationTerminator[_ <: Response] = {
    logger.info("Initializing terminators")
    val sparkAppTerminator: ApplicationTerminator[YarnResponse] = new SparkApplicationTerminator
    val wfAppTerminator: ApplicationTerminator[OozieResponse] = new WorkflowApplicationTerminator
    sparkAppTerminator.successor = Option[ApplicationTerminator[OozieResponse]](wfAppTerminator)

    sparkAppTerminator
  }
}
