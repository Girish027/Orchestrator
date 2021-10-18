package com.tfs.orchestrator.catalog.polling

import com.tfs.orchestrator.utils.PropertyReader
import org.apache.logging.log4j.scala.Logging

/**
  * Create an instance of CatalogReader and starts the polling service with catalog service
  */
object CatalogInitializer extends Logging{
    private var catalogReader: CatalogReader = null

    def initialize(): Unit = {
      try {
        val url = PropertyReader.getApplicationProperties().getProperty("catalog.snapshot.url")
        val pollingInterval = Integer.parseInt(PropertyReader.getApplicationProperties().getProperty("catalog.polling.interval"))
        logger.debug(s"Catalog Snapshot url is set to ${url} and the polling interval is set to ${pollingInterval}")
        catalogReader = new CatalogReader(url, pollingInterval)
        catalogReader.start()
      }catch{
        case e: Exception => {
          logger.error("Error in starting Catalog Reader",e)
          sys.exit(1)
        }

      }

    }

    def getCatalogReader(): CatalogReader = catalogReader
}
