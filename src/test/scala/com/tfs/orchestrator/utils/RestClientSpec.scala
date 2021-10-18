package com.tfs.orchestrator.utils

import com.tfs.orchestrator.exceptions.RestClientException
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}


class RestClientSpec extends FlatSpec with Matchers with MockFactory{

  it should "throw RestClientException if the url is incorrect" in {
    val restClient = RestClient
    a [RestClientException] should be thrownBy {
      restClient.sendGetRequest("junk://loclahost.com")
    }
  }

}
