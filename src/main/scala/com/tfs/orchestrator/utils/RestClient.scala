package com.tfs.orchestrator.utils

import com.tfs.orchestrator.exceptions.RestClientException
import org.apache.logging.log4j.scala.Logging

import scalaj.http.{HttpResponse, Http}

/**
  * A Generic RestClient that can be used to query the GET and POST Rest APIs. POST can accept payload in the format of Json
  */
object RestClient extends Logging {
  /**
    * REST call for GET method
    *
    * @param requestUrl - url
    * @return - Returns the response of the GET api in the String format
    */
  def sendGetRequest(requestUrl: String): String = {
    var response: HttpResponse[String] = null
    try {
      val connTimeoutMs = PropertyReader.getApplicationProperties().getProperty("http.connect.timeout", "2000").toInt
      val readTimeoutMs = PropertyReader.getApplicationProperties().getProperty("http.read.timeout", "10000").toInt
      response = Http(requestUrl)
        .timeout(connTimeoutMs, readTimeoutMs)
        .asString
    } catch {
      case ex: Exception => {
        throw new RestClientException("", ex)
      }
    }
    if(Constants.HTTP_200 == response.code) {
      return response.body
    }else{
      throw new RestClientException(s"Get call failed with response code ${response.code.toString}")
    }
  }


  /**
   *
   * @param url
   * @param payload
   * @return
   */
  def sendJsonPutRequest(url: String, payload: String): String ={
    try{
      val response = Http(url).put(payload).headers(getDefaultHttpHeaders).asString
      if(response.isSuccess) {
        return response.body
      }else{
        throw new RestClientException(s"Put call failed with response code ${response.code.toString}")
      }
    }catch{
      case ex: Exception => {
        throw  new RestClientException(s"Exception while executing put request: url: $url, body: $payload ", ex)
      }
    }
  }

  /**
   *
   * @param url
   * @param payload
   * @return
   */
  def sendJsonPostRequest(url: String, payload: String): String ={
    try{
      val response = Http(url).postData(payload).headers(getDefaultHttpHeaders).asString
      if(response.isSuccess) {
        return response.body
      }else{
        throw new RestClientException(s"Post call failed with response code ${response.code.toString}")
      }
    }catch{
      case ex: Exception => {
        throw  new RestClientException(s"Exception while executing post request: url: $url, body: $payload ", ex)
      }
    }
  }

  /**
   *
   * @return
   */
  def getDefaultHttpHeaders(): Map[String, String] = {
    return Map("Content-Type" -> "application/json", "Charset" -> "UTF-8")
  }
}
