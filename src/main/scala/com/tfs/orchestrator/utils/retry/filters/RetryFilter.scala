package com.tfs.orchestrator.utils.retry.filters

trait RetryFilter {

  def filter(wfIds:List[String]): Unit

}
