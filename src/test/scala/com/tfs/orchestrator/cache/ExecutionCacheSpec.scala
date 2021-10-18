package com.tfs.orchestrator.cache

import com.tfs.orchestrator.catalog.polling.ViewSchedule
import net.liftweb.json.{DefaultFormats, parse}
import org.scalatest.FlatSpec

class ExecutionCacheSpec extends FlatSpec {
  behavior of "ExecutionCache"


  it should "return valid list of cache entries for SLA" in {
    val jsonStr = "[{\n        \"viewName\": \"View_DCF\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"searsonline\" , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"dish\" , \"jobStartTime\":null,\"jobEndTime\":null   \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"hilton\" \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"DP2\",\n        \"jobStartTime\": \"1535706000000\",\n        \"jobEndTime\": \"1543572000000\",\n        \"complexity\": \"3\"\n    }]"
    cache(jsonStr)
    val entries = ExecutionCache.getSlaEntries()
    assert(entries.size.equals(3))
    entries.foreach(entry => assert(entry.value.equals("0")))
  }


  it should "return valid list of cache entries for SLA without duplicates for the same input" in {
    val jsonStr = "[{\n        \"viewName\": \"View_DCF\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"searsonline\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"dish\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"hilton\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"DP2\",\n        \"jobStartTime\": \"1535706000000\",\n        \"jobEndTime\": \"1543572000000\",\n        \"complexity\": \"3\"\n    }]"
    cache(jsonStr)
    cache(jsonStr)
    assert(ExecutionCache.getSlaEntries().size.equals(3))
  }


  it should "return the latest cache entry" in {
    var jsonStr = "[{\n        \"viewName\": \"View_DCF\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"searsonline\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"dish\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"0\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"hilton\" , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"DP2\",\n        \"jobStartTime\": \"1535706000000\",\n        \"jobEndTime\": \"1543572000000\",\n        \"complexity\": \"3\"\n    }]"
    cache(jsonStr)
    jsonStr = "[{\n        \"viewName\": \"View_DCF\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"2\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"searsonline\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"2\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"dish\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"2\",\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"hilton\" , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"DP2\",\n        \"jobStartTime\": \"1535706000000\",\n        \"jobEndTime\": \"1543572000000\",\n        \"complexity\": \"3\"\n    }]"
    cache(jsonStr)
    val entries = ExecutionCache.getSlaEntries()
    assert(entries.size.equals(3))
    entries.foreach(entry => assert(entry.value.equals("2")))
  }

  it should "return empty list of cache entries for SLA" in {
    val jsonStr = "[{\n        \"viewName\": \"View_DCF\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"searsonline\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"dish\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"queue\": \"LongRun\"\n                        },\n                        \"name\": \"hilton\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"DP2\",\n        \"jobStartTime\": \"1535706000000\",\n        \"jobEndTime\": \"1543572000000\",\n        \"complexity\": \"3\"\n}]"
    cache(jsonStr)
    assert(ExecutionCache.getSlaEntries().isEmpty)
  }

  it should "return empty list of cache entries for SLA when input is empty" in {
    val jsonStr = ""
    cache(jsonStr)
    assert(ExecutionCache.getSlaEntries().isEmpty)
  }


  it should "return valid list of cache entries for SLA across multiple views" in {
    val jsonStr = "[   \n\t{\n        \"viewName\": \"View_ABCD\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"-1\",\n                            \"queue\": \"default\"\n                        },\n                        \"name\": \"marriott_marriott\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 10 * * *\"\n            }\n        ],\n        \"userName\": \"Insights\",\n        \"jobStartTime\": \"1540684800000\",\n        \"jobEndTime\": \"1919376000000\",\n        \"complexity\": \"2\"\n    },\n    {\n        \"viewName\": \"View_AIVA_Billing_200\",\n        \"clientCronExpressionList\": [\n            {\n                \"clientExecProps\": [\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"-1\",\n                            \"queue\": \"default\"\n                        },\n                        \"name\": \"marriott\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    },\n                    {\n                        \"executionProperties\": {\n                            \"slaBreachTime\": \"-1\",\n                            \"queue\": \"default\"\n                        },\n                        \"name\": \"dish\"  , \"jobStartTime\":null,\"jobEndTime\":null \n                    }\n                ],\n                \"cronExpression\": \"0 * * * *\"\n            }\n        ],\n        \"userName\": \"Insights\",\n        \"jobStartTime\": \"1540684800000\",\n        \"jobEndTime\": \"1919376000000\",\n        \"complexity\": \"2\"\n\t}\n]"
    cache(jsonStr)
    assert(ExecutionCache.getSlaEntries().size.equals(3))
  }

  private def cache(jsonStr: String) = {
    ExecutionCache.invalidate()
    implicit val formats = DefaultFormats
    val viewSchedules = parse(jsonStr).extract[List[ViewSchedule]]
    ExecutionCache.cacheExecutionProperties(viewSchedules)
  }
}
