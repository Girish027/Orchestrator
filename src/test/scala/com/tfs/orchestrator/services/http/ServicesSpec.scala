package com.tfs.orchestrator.services.http

import org.scalatest.FlatSpec

class ServicesSpec extends FlatSpec {
  "DefaultService" should "have the response for all api" in {
    assert(new HealthService().response.contains("""{"status": "ok", "Service": "Orchestrator"}"""))
  }

  "HealthService" should "have the response for all api" in {

    assert(new DefaultService().response.equals("""{
   "/orchestrator/replay": {
      "description": "Invocation of replay service",
      "params": [
         {
            "client": "mandatory",
            "view": "mandatory",
            "datastarttime": "mandatory. epoch time in millis.",
            "dataendtime": "mandatory. epoch time in millis",
            "scheduletime": "optional(default# now)",
            "queue": "optional(default# default)"
         }
      ]
   },
   "/orchestrator/health": {
      "description": "Provides health of the Orchestrator service."
   }
}""".stripMargin))
  }
}
