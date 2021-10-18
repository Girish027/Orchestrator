package com.tfs.orchestrator.managers

import java.net.InetAddress

import com.tfs.orchestrator.services.http.{DefaultService, HealthService, ReplayService}
import com.tfs.orchestrator.utils.PropertyReader
import io.undertow.{Handlers, Undertow}
import org.apache.logging.log4j.scala.Logging
import com.tfs.orchestrator.services.http.ReplayIngestorService

/**
 * Initializes the undertow server with appropriate services.
 */
class UndertowManager extends Logging {

  val applicationProps = PropertyReader.getApplicationProperties()

  /**
   * Initializes all the undertow http based services.
   */
  def start(): Unit = {
    val port = applicationProps.getProperty("orchestrator.service.port", "8989").toInt
    val host = applicationProps.getProperty("orchestrator.service.host", InetAddress.getLocalHost.getCanonicalHostName)
    val server = Undertow.builder()
      .addHttpListener(port, host)
      .setHandler(Handlers.path(new DefaultService).
        addPrefixPath("/orchestrator/health", new HealthService).
        addExactPath("/orchestrator/replay", new ReplayService).addExactPath("/orchestrator/replay/ingestion", new ReplayIngestorService)
      ).build();

    logger.info("Starting the Undertow server!")
    server.start
  }

}
