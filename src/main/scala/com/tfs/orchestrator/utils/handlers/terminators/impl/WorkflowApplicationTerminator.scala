package com.tfs.orchestrator.utils.handlers.terminators.impl

import com.tfs.orchestrator.utils.handlers.terminators.ApplicationTerminator

final class WorkflowApplicationTerminator extends ApplicationTerminator[OozieResponse] {

  private val wfActionTerminalStates = List[String]("OK", "FAILED", "KILLED")
  private val wfActions = List[String]("PreprocessTask")

  override val URL = OOZIE_URL + "$id?show=info"

  override def filterRunningApplications(oozieResponse: OozieResponse): Option[String] = {
    val lastAction = oozieResponse.actions.takeRight(1)(0)
    if (!wfActionTerminalStates.contains(lastAction.status) && wfActions.contains(lastAction.name)) {
      val url = lastAction.consoleUrl
      val lastIndex = url.lastIndexOf("/")
      val secondLastIndex = url.lastIndexOf("/", lastIndex - 1)
      return Option[String](url.substring(secondLastIndex + 1, lastIndex))
    }
    None
  }
}

case class Actions(
                    name: String,
                    status: String,
                    consoleUrl: String
                  )

case class OozieResponse(
                          appName: String,
                          parentId: String,
                          id: String,
                          actions: List[Actions],
                          status: String,
                          group: String
                        ) extends Response
