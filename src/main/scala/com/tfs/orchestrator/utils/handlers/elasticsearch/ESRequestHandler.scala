package com.tfs.orchestrator.utils.handlers.elasticsearch

import com.tfs.orchestrator.utils.{Constants, PropertyReader}
import scalaj.http.{Http, HttpOptions, HttpResponse}


class ESRequestHandler {

  private lazy val ES_URL = PropertyReader.getApplicationPropertiesValue("sla.es.ingest.url", Constants.DEFAULT_ES_URL)

  def fetchJobDetailsFromES(index: String, param: String, requestBody: String): HttpResponse[String] = {
    val url = s"$ES_URL/$index?$param"
    triggerHttpRequest(requestBody, url)
  }

  def fetchJobDetailsFromES(index: String, requestBody: String): HttpResponse[String] = {
    val url = s"$ES_URL/$index"
    triggerHttpRequest(requestBody, url)
  }

  def fetchJobDetailsFromESWithoutIndex(param: String, requestBody: String): HttpResponse[String] = {
    val url = s"$ES_URL/$param"
    triggerHttpRequest(requestBody, url)
  }

  def deleteScrollId(url:String):HttpResponse[String] =
  {
    triggerHttpRequestDelete(s"$ES_URL/$url")
  }

  private def triggerHttpRequest(filter: String, url: String):HttpResponse[String] = {
    Http(url).postData(filter)
      .header("Content-Type", Constants.CONTENT_TYPE)
      .header("Charset", Constants.ENCODING)
      .option(HttpOptions.readTimeout(Constants.HTTP_TIMEOUT)).asString
  }

  private def triggerHttpRequestDelete(url: String):HttpResponse[String] = {
    Http(url).
      method("DELETE")
      .header("Content-Type", Constants.CONTENT_TYPE)
      .header("Charset", Constants.ENCODING)
      .option(HttpOptions.readTimeout(Constants.HTTP_TIMEOUT)).asString
  }

}
