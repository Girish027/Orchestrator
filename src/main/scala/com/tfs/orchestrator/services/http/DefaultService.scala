package com.tfs.orchestrator.services.http

import com.tfs.orchestrator.utils.Constants
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.util.Headers

/**
 * Default service with API descriptions.
 */
class DefaultService extends HttpHandler {

  val response =
    """{
      |   "/orchestrator/replay": {
      |      "description": "Invocation of replay service",
      |      "params": [
      |         {
      |            "client": "mandatory",
      |            "view": "mandatory",
      |            "datastarttime": "mandatory. epoch time in millis.",
      |            "dataendtime": "mandatory. epoch time in millis",
      |            "scheduletime": "optional(default# now)",
      |            "queue": "optional(default# default)"
      |         }
      |      ]
      |   },
      |   "/orchestrator/health": {
      |      "description": "Provides health of the Orchestrator service."
      |   }
      |}""".stripMargin


  override def handleRequest(exchange: HttpServerExchange): Unit = {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, Constants.HTTP_APPLICATION_JSON);
    exchange.getResponseSender.send(response)
  }

}
