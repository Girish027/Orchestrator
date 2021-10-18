package com.tfs.orchestrator.utils.handlers.terminators.impl

import com.tfs.orchestrator.utils.handlers.terminators.ApplicationTerminator

final class SparkApplicationTerminator extends ApplicationTerminator[YarnResponse] {

  private val yarnTerminalStates = List[String]("FINISHED", "FAILED", "KILLED")

  override val URL: String = YARN_URL + "?applicationTags=$id"

  override def filterRunningApplications(yarnResponse: YarnResponse): Option[String] = {
    val apps = yarnResponse.apps
    if (apps.nonEmpty) {
      apps.get.app.foreach(app => {
        if (!yarnTerminalStates.contains(app.state)) {
          return Option[String](app.id)
        }
      })
    }
    None
  }
}


case class App(
                id: String,
                applicationType: String,
                state: String
              )

case class Apps(
                 app: List[App]
               )

case class YarnResponse(
                         apps: Option[Apps]
                       ) extends Response

