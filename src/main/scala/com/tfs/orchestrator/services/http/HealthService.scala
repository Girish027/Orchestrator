package com.tfs.orchestrator.services.http

import com.tfs.orchestrator.utils.Constants
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.util.Headers

/**
 * Provides nominal response about Orchestrator.
 */
class HealthService extends HttpHandler {

  val response = """{"status": "ok", "Service": "Orchestrator"}"""

  /**
   * TODO: Needs more work.
   * @param exchange
   */
  override def handleRequest(exchange: HttpServerExchange): Unit = {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, Constants.HTTP_APPLICATION_JSON);
    exchange.getResponseSender.send(response)
  }
}
